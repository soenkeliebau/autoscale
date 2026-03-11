# StackableScaler: HPA integration with operator-controlled scaling hooks

## Summary

Introduces a `StackableScaler` CRD that acts as an intermediary between the Kubernetes
HorizontalPodAutoscaler and Stackable-managed StatefulSets. Instead of the HPA scaling
the StatefulSet directly (which would bypass the operator), the HPA targets the
`StackableScaler` via its `/scale` subresource. The product operator drives a state
machine that runs pre/post-scale hooks before propagating replica changes to the
StatefulSet.

NiFi is the proof-of-concept: its `pre_scale` hook offloads, disconnects, and deletes
nodes from the NiFi cluster via the REST API before the StatefulSet is scaled down,
preventing data loss and cluster instability.

Changes span three repositories:
- **operator-rs** — CRD definition, state machine, hook trait, job tracker
- **commons-operator** — CRD rollout, admission webhook
- **nifi-operator** — `NifiScalingHooks` implementation, controller integration

---

## Motivation

Kubernetes HPAs can target StatefulSets directly via the `/scale` subresource. This
works mechanically but bypasses the Stackable operator entirely — the operator has no
opportunity to run product-specific lifecycle tasks before or after a scaling event.

For NiFi this is a concrete problem: NiFi cluster nodes must be **offloaded** (their
flowfile data redistributed to remaining nodes) before removal. Scaling the StatefulSet
directly terminates pods without offloading, causing data loss or cluster instability.

The same need exists across other Stackable products (Kafka partition reassignment,
HDFS block replication, etc.), so the mechanism was designed generically.

---

## Design decisions

### 1. CRD in `operator-rs`, not per-operator

The `StackableScaler` follows the `S3Connection` pattern: a platform-level concept
defined once in `operator-rs` and installed by the commons-operator. Each product
operator adopts it by implementing the `ScalingHooks` trait — no CRD duplication.

**Rejected:** Defining `StackableScaler` in each product operator. This would require
duplicated CRD schemas, duplicated webhook logic, and cross-version coordination when
the CRD evolves.

### 2. HPA targets `StackableScaler`, not StatefulSet

The HPA writes `spec.replicas` on the `StackableScaler` via its `/scale` subresource.
The operator reads `spec.replicas`, runs hooks, and only propagates the new count to
the StatefulSet once pre-scale hooks complete.

**Rejected:** HPA targets StatefulSet directly, operator reacts after the fact. There
is no way to **intercept** a scale event before pods are terminated — only post-mortem
cleanup is possible, which is insufficient for NiFi offloading.

**Rejected:** Replace HPA with a custom metrics-based controller. This duplicates HPA
functionality (metric scraping, cooldown logic, scaling policies) and forces each
operator to implement its own scaling decision logic.

**Deferred:** KEDA with a custom scaler. KEDA supports pause/resume and tighter
lifecycle control, but introduces an external dependency. The standard HPA approach
was chosen first; KEDA remains an option if HPA limitations prove problematic.

### 3. Activation via `replicas: 0` convention

The existing codebase already uses `replicas: 0` in role group configs to signal
"externally managed replicas" (a prior workaround for users wanting to use HPAs
directly). Adding a new field (e.g., `scalingMode: external`) would require changes
to every product CRD.

A `StackableScaler` is only effective when the referenced role group has `replicas: 0`.
If `replicas: 0` is set without a `StackableScaler`, existing behavior is preserved.

### 4. Label-based watch filtering via mutating webhook

Each product operator watches only `StackableScaler` resources for its cluster kind
(e.g., `NifiCluster`). Rather than requiring users to manually set a
`stackable.tech/cluster-kind` label, the commons-operator admission webhook
**automatically injects** it from `spec.clusterRef.kind` on every CREATE and UPDATE.

Operators use server-side filtering:
```rust
watcher::Config::default().labels("stackable.tech/cluster-kind=NifiCluster")
```

**Rejected:** Require users to set the label manually. Error-prone and undiscoverable.

### 5. Generic `ScalingHooks` trait with RPITIT

Hook implementations may be async and stateful (e.g., holding a NiFi API client).
Closure-based approaches are awkward with async. Trait objects have object-safety
constraints that conflict with async methods without boxing.

The `ScalingHooks` trait uses RPITIT (stable since Rust 1.75) with default
implementations that return `HookOutcome::Done` immediately, so operators only
override the hooks they need:

```rust
pub trait ScalingHooks {
    type Error: std::error::Error + Send + Sync + 'static;
    fn pre_scale(&self, ctx: &ScalingContext<'_>) -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send;
    fn post_scale(&self, ctx: &ScalingContext<'_>) -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send;
    fn on_failure(&self, ctx: &ScalingContext<'_>, failed_stage: &FailedStage) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

### 6. `ScalingDirection` derived by `operator-rs`

`operator-rs` computes `ScalingDirection::Up` or `Down` from current vs. desired
replicas and passes it via `ScalingContext`. This removes a class of off-by-one errors
in operator code. Equal counts are treated as `Up` (no-op in practice).

### 7. Single `status.replicas` field

The HPA requires `statusReplicasPath` to point to the current replica count. Having
separate `currentReplicas` and `replicas` fields creates ambiguity. A single
`status.replicas` serves both the HPA and the operator; it is updated during the
`Scaling` stage when the StatefulSet target changes.

### 8. Mid-flight `spec.replicas` changes rejected by webhook

If the HPA writes a new `spec.replicas` while the state machine is mid-transition,
the operator would need to handle aborting and restarting the hook sequence. Instead,
a validating webhook rejects writes to `spec.replicas` when the stage is not `Idle` or
`Failed`. The HPA receives a rejection, surfaces `AbleToScale: False`, backs off, and
retries.

The webhook fetches the **live** `StackableScaler` from the API server (with a 5s
timeout) rather than reading `oldObject.status`, because Kubernetes strips `.status`
from `oldObject` in admission requests for CRDs with a status subresource.

**Fail-closed:** If the API fetch fails or times out, the webhook denies the change.
This is consistent with `failurePolicy: Fail`.

### 9. `Failed` as a terminal trap state

**Rejected:** Automatic retry with backoff. Risks repeatedly invoking a broken hook
and masking the failure.

**Rejected:** Configurable retry count. Adds complexity without clear benefit —
operators need to investigate failures regardless.

`Failed` is terminal. Recovery is via annotation:
```bash
kubectl annotate stackablescaler <name> autoscaling.stackable.tech/retry=true
```
The operator strips the annotation and resets to `Idle`.

### 10. `reconcile_scaler` placed after StatefulSet apply

The `Scaling → PostScaling` transition requires knowing whether the StatefulSet has
converged. By calling `reconcile_scaler` **after** `cluster_resources.add(client, sts)`,
the applied StatefulSet has current status from server-side apply, allowing
`statefulset_stable` to be computed from `ready_replicas == spec.replicas`.

### 11. `clusterRef` without `apiVersion`

Including `apiVersion` in the cluster reference creates coupling to specific CRD
versions. CRD conversion webhooks already handle API version transitions; the
`StackableScaler` doesn't need to duplicate that mapping. `clusterRef` contains only
`kind` and `name`.

---

## CRD: `StackableScaler`

```yaml
apiVersion: autoscaling.stackable.tech/v1alpha1
kind: StackableScaler
metadata:
  name: nifi-nodes-default
  labels:
    stackable.tech/cluster-kind: NifiCluster  # injected by webhook
spec:
  replicas: 5          # written by HPA via /scale subresource
  clusterRef:
    kind: NifiCluster
    name: my-nifi
  role: nodes
  roleGroup: default
status:
  replicas: 3          # current StatefulSet target (read by HPA)
  selector: "..."      # pod label selector (for HPA pod counting)
  desiredReplicas: 5   # in-flight target (set when leaving Idle)
  currentState:
    stage: scaling
    lastTransitionTime: "2026-03-06T10:05:00Z"
```

The `/scale` subresource is configured with:
- `specReplicasPath: .spec.replicas`
- `statusReplicasPath: .status.replicas`
- `labelSelectorPath: .status.selector`

---

## Control flow

### State machine

```
Idle
 │  spec.replicas != status.replicas (new scaling target detected)
 ▼
PreScaling ─── HookOutcome::InProgress ──▶ requeue (10s)
 │  HookOutcome::Done
 ▼
Scaling    ─── StatefulSet not converged ──▶ requeue (5s)
 │  ready_replicas == desired_replicas
 ▼
PostScaling ── HookOutcome::InProgress ──▶ requeue (10s)
 │  HookOutcome::Done
 ▼
Idle  (desiredReplicas cleared, status.replicas updated)

Any stage ──── hook returns Err ──▶ Failed (terminal trap)
```

### Reconcile integration (nifi-operator)

```
reconcile_nifi()
  for each role group:
    1. If role_group.replicas == 0, look up StackableScaler by clusterRef match
    2. Resolve effective replicas via resolve_replicas(rg_replicas, scaler)
    3. Build and apply StatefulSet with resolved replica count
    4. If scaler exists:
       a. Compute statefulset_stable from applied STS status
       b. Construct NifiScalingHooks with credentials, service names, version
       c. Call reconcile_scaler(scaler, hooks, client, stable, selector)
       d. Log ScalingCondition (Healthy / Progressing / Failed)
       e. Collect requeue Action
    5. Return first non-None scaler Action, or await_change()
```

### NiFi pre-scale hook (scale-down)

The scale-down sequence differs by NiFi version because NiFi 2.x changed the required
order of operations:

**NiFi 1.x:** CONNECTED → OFFLOADING → OFFLOADED → DISCONNECTING → DISCONNECTED → DELETE

**NiFi 2.x:** CONNECTED → DISCONNECTING → DISCONNECTED → OFFLOADING → OFFLOADED → DELETE

Each phase runs across all target nodes (ordinals >= desired_replicas) before advancing.
If any node is still transitioning, the hook returns `InProgress` and the reconciler
requeues after 10 seconds.

The hook authenticates with the NiFi REST API using SingleUser credentials read from
a Kubernetes Secret. API calls go through pod-0 (always safe — it won't be removed
during scale-down). TLS verification is disabled because NiFi pods use self-signed
certificates from the Stackable secret operator.

Scale-up returns `Done` immediately (no pre-scale work needed).

---

## Detailed changes

### `operator-rs` (+1218 lines)

| File | Description |
|------|-------------|
| `crd/scaler/mod.rs` | `StackableScaler` CRD types (`StackableScalerSpec`, `StackableScalerStatus`, `ScalerStage`, `ScalerState`, `FailedStage`, `UnknownClusterRef`), `resolve_replicas` helper, `Display` for `ScalerStage`, manual `JsonSchema` impl for the internally-tagged enum, serialization tests |
| `crd/scaler/hooks.rs` | `ScalingHooks` trait with RPITIT, `ScalingContext`, `ScalingDirection`, `HookOutcome`, `ScalingCondition`, `ScalingResult` |
| `crd/scaler/reconciler.rs` | `reconcile_scaler` entry point, `next_stage` pure function (synchronous, unit-tested), status patching, `handle_hook_failure` helper, 8 state-transition unit tests |
| `crd/scaler/job_tracker.rs` | `JobTracker::start_or_check` for Job-based hooks (server-side apply, completion check, cleanup), `job_name` DNS-safe name generator |
| `crd/mod.rs` | `pub mod scaler;` registration |

### `commons-operator` (+469 lines)

| File | Description |
|------|-------------|
| `webhooks/scaler_admission.rs` | MutatingWebhook handler: (1) on UPDATE, fetches live object from API to check stage, denies `spec.replicas` changes during active scaling; (2) on all ops, injects `stackable.tech/cluster-kind` label from `spec.clusterRef.kind` |
| `webhooks/mod.rs` | Registers `scaler_admission` webhook, adds `disable_scaler_admission_webhook` parameter |
| `main.rs` | `--disable-scaler-admission-webhook` CLI flag, `StackableScaler::crd()` in YAML schema output |
| `templates/roles.yaml` | RBAC: `get`, `list`, `watch` on `stackablescalers` for the admission webhook's live-object fetch |
| `extra/crds.yaml` | Generated CRD manifest for `StackableScaler` with `/scale` and `/status` subresources |

### `nifi-operator` (+1808 lines)

| File | Description |
|------|-------------|
| `operations/scaling.rs` | `NifiScalingHooks` implementing `ScalingHooks` trait — version-conditional scale-down (offload→disconnect→delete for 1.x, disconnect→offload→delete for 2.x), target node identification by pod FQDN matching, unit tests for direction, FQDN, URL, version detection |
| `operations/nifi_api.rs` | `NifiApiClient` — reqwest-based client for NiFi REST API: SingleUser token auth, cluster node listing, node status updates (offload/disconnect), node deletion. Typed `NifiNodeStatus` enum, structured error types |
| `operations/credentials.rs` | `resolve_single_user_credentials` — reads admin password from Kubernetes Secret using the `STACKABLE_ADMIN_USERNAME` key |
| `operations/mod.rs` | Module registration for `credentials`, `nifi_api`, `scaling` |
| `controller.rs` | StackableScaler lookup per role group (only when `replicas == 0`), `resolve_replicas` integration, `reconcile_scaler` call after StatefulSet apply, `statefulset_stable` computation with scale-to-zero guard, `ScalingCondition` logging, `scaler_action` collection across role groups |
| `main.rs` | `StackableScaler` watch registration with `stackable.tech/cluster-kind=NifiCluster` label filter, `scaler_store` for the watch mapper |
| `templates/roles.yaml` | RBAC: `get`, `list`, `watch`, `patch` on `stackablescalers` and `get`, `patch` on `stackablescalers/status` |
| `extra/crds.yaml` | Same generated CRD manifest |

---

## Known limitations

1. **No HPA back-pressure.** The standard Kubernetes HPA has no mechanism to be told
   to permanently stop. When the scaler is in `Failed` state, rejected `spec.replicas`
   writes cause `AbleToScale: False` — but the HPA retries indefinitely. KEDA would
   address this but is not a current dependency.

2. **Job name collisions across successive scale events.** `JobTracker` derives names
   deterministically from the scaler name and stage. If cleanup fails and the same
   scaler scales again, the tracker may find the previous completed Job and return
   `Done` without running a new one. Adding a generation counter to the name would
   fix this.

3. **Webhook validation requires API fetch.** Because Kubernetes strips `.status` from
   `oldObject` for CRDs with a status subresource, the admission webhook must fetch
   the live object. This adds latency (bounded by 5s timeout) and requires RBAC
   permissions. The webhook fails closed on fetch failure.

4. **`ScalingCondition` not yet propagated to `NifiCluster` status.** The `ScalingResult`
   includes a `ScalingCondition` that should be written to the cluster CR's
   `status.conditions`, but the nifi-operator currently only logs it.

---

## Test plan

- [ ] Unit tests pass: `cargo test --all-features` in all three repos
- [ ] State machine transitions: 8 unit tests in `reconciler.rs` covering all stage transitions
- [ ] `resolve_replicas`: 6 unit tests covering scaler present/absent, status present/absent, zero/nonzero replicas
- [ ] `ScalingDirection`: 3 unit tests (up, down, equal)
- [ ] `job_name`: 5 unit tests (stability, uniqueness, truncation, trailing hyphen, format)
- [ ] `NifiScalingHooks`: 5 unit tests (direction shortcut, pod FQDN, API URL, version detection)
- [ ] Deploy to cluster, create StackableScaler targeting NifiCluster
- [ ] Verify label injection on CREATE
- [ ] Verify `spec.replicas` change rejected during PreScaling/Scaling/PostScaling
- [ ] Verify `spec.replicas` change allowed during Idle and Failed
- [ ] Trigger scale-down, verify NiFi nodes are offloaded before StatefulSet scales
- [ ] Trigger scale-up, verify immediate propagation (no pre-scale work)
- [ ] Verify `Failed` state is terminal and recoverable via retry annotation
