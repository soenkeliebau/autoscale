# ADR: StackableScaler — HPA Integration with Operator-Controlled Scaling Hooks

**Date:** 2026-03-06
**Status:** Implemented
**Deciders:** Stackable platform team
**Repositories:** `operator-rs`, `nifi-operator`

---

## Context

Kubernetes Horizontal Pod Autoscalers can target `StatefulSet` resources directly via the `/scale` subresource. This works mechanically but bypasses the Stackable operator entirely. The operator has no opportunity to run product-specific tasks before or after a scaling event.

For NiFi this is a concrete problem: nodes must be offloaded (their data flow tasks drained and redistributed) before a node is removed. Scaling the `StatefulSet` directly would terminate pods without offloading, potentially causing data loss or cluster instability. The same need exists to varying degrees across other Stackable products.

The goal was to design a mechanism that:
- Keeps HPAs as the scaling trigger (avoid replacing HPA with a custom controller)
- Gives operators a hook mechanism before and after every scale event
- Works generically across all Stackable operators, not just NiFi
- Does not require operators to poll or busy-wait in a reconcile loop

---

## Decision

Introduce a `StackableScaler` CRD defined in `operator-rs` and installed platform-wide via the commons operator. The HPA targets `StackableScaler` via its `/scale` subresource instead of the `StatefulSet`. The product operator drives a state machine on the `StackableScaler` and only propagates replica changes to the `StatefulSet` once pre-scale hooks complete.

---

## Alternatives Considered

### Alternative 1: HPA targets StatefulSet directly; operator detects the change

The operator could watch `StatefulSet` replica changes and react after the fact. It would detect a discrepancy between running pods and expected state and run cleanup.

**Rejected because:** There is no way to intercept the scale before it takes effect. Pods are terminated before the operator can act. This is a "post-mortem" hook, not a pre-scale hook. NiFi specifically requires offloading before termination.

### Alternative 2: Disable HPA and implement a custom metrics-based controller

Build a controller that reads custom metrics (e.g., from Prometheus) and drives scaling decisions directly, without the HPA.

**Rejected because:** This duplicates HPA functionality (metric scraping, cooldown logic, scaling policies) and requires each operator to implement its own scaling decision logic. HPAs are well-understood and well-tested. The goal was to intercept scaling, not replace it.

### Alternative 3: Use KEDA with a custom scaler

KEDA supports custom scaler plugins and has a more flexible pause/resume mechanism than the standard HPA.

**Deferred rather than rejected:** KEDA would allow tighter lifecycle control (pausing the scaler when in `Failed` state, for example). However, KEDA is not a standard platform dependency, and introducing it solely for this feature adds operational overhead. The standard HPA approach was chosen first; KEDA remains an option if the HPA's lack of back-pressure proves limiting in practice. The `Failed` state and its interaction with the HPA's `AbleToScale: False` condition is an acknowledged known limitation of the chosen design.

---

## Design Decisions

### Decision 1: CRD defined in `operator-rs`, not per-operator

**Options considered:**
- Define `StackableScaler` in each product operator that needs it
- Define it once in `operator-rs`, following the `S3Connection` pattern

`S3Connection` is a cross-operator resource defined in `operator-rs` and installed via the commons operator. `StackableScaler` has the same shape: it is a platform-level concept that every operator will eventually need, and it references product clusters by name rather than embedding product-specific logic.

**Decision:** Follow the `S3Connection` pattern. `StackableScaler` is defined in `operator-rs/crates/stackable-operator/src/crd/scaler/`, registered in `operator-rs/crates/stackable-operator/src/crd/mod.rs`, and installed by the commons operator. NiFi is the proof-of-concept; other operators adopt the pattern by implementing the `ScalingHooks` trait.

### Decision 2: Activation via `replicas: 0` convention

**Options considered:**
- Require an explicit `scalingMode: external` field in the cluster spec
- Reuse the existing `replicas: 0` convention, which already signals "externally managed replicas"
- Validate at `StackableScaler` creation time that `replicas: 0` is set, and reject otherwise

The existing codebase already uses `replicas: 0` to suppress operator-driven StatefulSet replica management (a prior workaround for users wanting to use HPAs directly against the StatefulSet). Adding a new field would require changes to every product CRD.

**Decision:** Reuse `replicas: 0`. A `StackableScaler` is only effective for a role group where `spec.replicas == 0`. If `replicas: 0` is set but no `StackableScaler` exists, the existing behaviour is preserved (backwards compatibility). A validating webhook on `StackableScaler` creation enforces the constraint and rejects resources targeting role groups where `replicas != 0`. This makes the activation convention explicit and detectable.

### Decision 3: `clusterRef` without `apiVersion`

**Options considered:**
- `clusterRef: { apiVersion, kind, name }` — fully qualified reference
- `clusterRef: { kind, name }` — kind-only reference

Including `apiVersion` in `clusterRef` creates coupling to specific CRD versions and complicates version upgrades. CRD conversion webhooks already handle API version transitions; the `StackableScaler` need not duplicate that mapping.

**Decision:** `clusterRef` contains only `kind` and `name`. The `kind` field drives label-based watch filtering (see Decision 4). `apiVersion` was removed from the design.

### Decision 4: Label-based watch filtering via mutating webhook

Each product operator should watch only the `StackableScaler` resources that target its cluster kind. Two approaches were considered:

**Option A:** Require users to set `stackable.tech/cluster-kind: NifiCluster` manually on every `StackableScaler`.

**Option B:** A mutating admission webhook in the commons operator reads `spec.clusterRef.kind` on `StackableScaler` CREATE and UPDATE and sets the label automatically.

**Decision:** Option B. User-facing label requirements are error-prone and undiscoverable. The commons operator already runs admission webhooks; adding one for `StackableScaler` is a natural fit. Product operators use `watcher::Config::default().labels("stackable.tech/cluster-kind=NifiCluster")` for server-side filtering, but do not require users to maintain the label manually.

### Decision 5: Trait-based hook interface

**Options considered:**
- **Callback closures:** Pass `Box<dyn Fn(...)>` hooks to `reconcile_scaler`.
- **Trait object:** Accept `&dyn ScalingHooks`.
- **Generic trait bound:** `reconcile_scaler<H: ScalingHooks>(hooks: &H, ...)`.
- **Inline hook logic in each operator:** No shared abstraction.

The hook implementations may be async and stateful (e.g., holding a NiFi API client). Closure-based approaches are awkward with async. Trait objects have object-safety constraints that conflict with async methods without boxing.

**Decision:** Generic `ScalingHooks` trait with RPITIT (Return Position `impl Trait` In Traits, stable since Rust 1.75). Default implementations return `HookOutcome::Done` immediately so operators only override the hooks they need. `pre_scale`, `post_scale`, and `on_failure` are the three hook points. `on_failure` is best-effort (errors logged, not propagated) because cleanup on failure should not itself fail the reconcile.

### Decision 6: `ScalingDirection` derived by `operator-rs`, not the operator

During design it was asked whether the hook implementor should derive scaling direction themselves by comparing replica counts, or whether `operator-rs` should compute it and pass it in via `ScalingContext`.

**Decision:** `operator-rs` derives `ScalingDirection::Up` or `Down` from `current_replicas` vs `desired_replicas` and includes it in `ScalingContext`. This removes a class of off-by-one errors in operator code and keeps the `ScalingContext` semantically complete. The rule is: `desired >= current` → `Up`; `desired < current` → `Down`. Equal counts (i.e., re-reconciling without a replica change) are treated as `Up` (no-op in practice since the state machine stays `Idle`).

### Decision 7: Single `status.replicas` field (no `currentReplicas` / `replicas` split)

**Options considered:**
- Two fields: `status.currentReplicas` (actual running) + `status.replicas` (HPA-visible target)
- One field: `status.replicas` serving both purposes

The HPA requires `statusReplicasPath` to point to the current replica count for its scaling calculations. Having two fields with potentially different values creates ambiguity about which one the HPA is reading and which the operator is writing.

**Decision:** Single `status.replicas`. It is the HPA-visible field (`statusReplicasPath: .status.replicas`) and the single source of truth. It reflects the StatefulSet target and is updated when the state machine transitions into the `Scaling` stage (not only at `Idle`). `status.desiredReplicas` is a separate tracking field that records the in-flight target from `Idle` departure until `PostScaling → Idle` completion; it is not used by the HPA.

### Decision 8: Mid-flight `spec.replicas` changes rejected by webhook

**Problem:** The HPA continuously adjusts `spec.replicas`. If the HPA writes a new value while the state machine is in `PreScaling` or `PostScaling`, the operator would need to handle the mid-flight change, potentially aborting and restarting the hook sequence.

**Decision:** A validating webhook rejects writes to `spec.replicas` when `status.currentState.stage` is not `Idle` and not `Failed`. The HPA receives a rejection and surfaces `AbleToScale: False` in its own conditions. It backs off and retries. This is the limit of what standard Kubernetes HPAs support; no special signalling is needed on the operator side. Rejected writes are not a silent failure — they are visible in HPA events.

### Decision 9: `Failed` as a terminal trap state with annotation-based recovery

**Options considered:**
- Automatic retry with backoff on hook failure
- Terminal `Failed` state with manual recovery
- Configurable retry count in the `StackableScaler` spec

Automatic retries risk repeatedly invoking a broken hook (e.g., a misconfigured NiFi API endpoint) and masking the failure. A terminal state forces operator attention to failed scaling events and prevents the HPA from unknowingly triggering further scaling while the cluster is in a bad state.

**Decision:** `Failed` is a terminal trap state. No automatic retries. Recovery is via:
```bash
kubectl annotate stackablescaler <name> autoscaling.stackable.tech/retry=true
```
The operator strips the annotation and resets `status.currentState.stage` to `Idle`. This is Kubernetes-idiomatic (annotation-driven operations are common in operators) and non-destructive (the annotation can be inspected before being acted on).

### Decision 10: `reconcile_scaler` placed after StatefulSet apply

**Initial implementation:** `reconcile_scaler` was called before the StatefulSet was built and applied, with `statefulset_stable: false` as a placeholder. The `Scaling → PostScaling` transition requires knowing whether the StatefulSet has converged (all desired pods ready).

**Revised design (Task 10):** `reconcile_scaler` is moved to after `cluster_resources.add(client, rg_statefulset)`. The returned applied `StatefulSet` has a current `status` from the server-side apply response, allowing `statefulset_stable` to be computed from `status.ready_replicas == spec.replicas`.

A subtlety was identified: the `&& spec.replicas > 0` guard (intended to prevent a freshly created STS with no pods from being considered stable) would also block a legitimate scale-to-zero from ever leaving the `Scaling` stage. This was fixed by additionally checking `scaler.status.desired_replicas == Some(0)` — only when the scaler explicitly targets zero does `sts_desired == 0` count as stable.

### Decision 11: `ScalingResult.action` must be returned from `reconcile_nifi`

**Initial implementation:** `scaling_result.action` was computed but never returned from the controller. `reconcile_nifi` always returned `Action::await_change()`.

**Consequence:** The hook polling mechanism was non-functional. `reconcile_scaler` returns `Action::requeue(10s)` when a hook is in progress, but this was silently discarded. The controller only re-reconciled on watch events, not on the timer.

**Fix:** The rolegroup loop was refactored to collect `scaling_result.action` from each rolegroup. The first non-None action is stored in `scaler_action`. `reconcile_nifi` returns `scaler_action.unwrap_or_else(Action::await_change)`. When multiple rolegroups have active scalers, the first requeue encountered is used; subsequent requeues are subsumed (acceptable because all rolegroups are re-evaluated on each reconcile).

---

## Known Limitations

### HPA back-pressure

The standard Kubernetes HPA has no mechanism to be told to permanently stop. When the scaler is in `Failed` state, the validating webhook rejects `spec.replicas` writes, causing the HPA to surface `AbleToScale: False` and back off — but it retries indefinitely. There is no way to signal "give up" to a standard HPA. KEDA's `ScaledObject` supports pausing, which would address this; the current design accepts this limitation as a reasonable trade-off against adding KEDA as a dependency.

### Job name collisions across successive scale events

`JobTracker` derives job names deterministically from the scaler name and stage (e.g., `my-scaler-pre-scale`). If the cleanup delete after a successful job fails (best-effort), and the same scaler is asked to scale again, the second `start_or_check` call finds the completed job from the first event, sees `succeeded > 0`, and returns `Done` without running a new offload. Incorporating a generation counter or `desiredReplicas` into the job name would address this; it is left as a follow-up.

### Webhook implementation out of scope

The mutating and validating webhooks described in this ADR (label injection, `spec.replicas` seeding from current StatefulSet on creation, mid-flight write rejection, `replicas: 0` enforcement) must be implemented in the commons operator. This work was not part of the initial implementation and is documented in `docs/plans/2026-03-06-stackable-scaler-impl.md` Task 11.

---

## Implementation Structure

```
operator-rs/crates/stackable-operator/src/crd/scaler/
├── mod.rs          — StackableScaler CRD types, resolve_replicas helper
├── hooks.rs        — ScalingHooks trait, ScalingContext, HookOutcome, ScalingCondition
├── reconciler.rs   — reconcile_scaler state machine driver
└── job_tracker.rs  — JobTracker helper for Job-based async hooks

nifi-operator/rust/operator-binary/src/
├── operations/scaling.rs   — NifiScalingHooks implementation
├── main.rs                 — StackableScaler watch registration
└── controller.rs           — scaler lookup, reconcile_scaler call, resolve_replicas
```

---

## References

- Design document: `docs/plans/2026-03-06-stackable-scaler-design.md`
- Implementation plan: `docs/plans/2026-03-06-stackable-scaler-impl.md`
- `S3Connection` pattern reference: `operator-rs/crates/stackable-operator/src/crd/s3/`
