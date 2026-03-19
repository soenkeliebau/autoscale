# StackableScaler: HPA integration with operator-controlled scaling hooks

## Summary

Introduces a `StackableScaler` CRD that acts as an intermediary between the Kubernetes
HorizontalPodAutoscaler and Stackable-managed StatefulSets. Instead of the HPA scaling
the StatefulSet directly (which would bypass the operator), the HPA targets the
`StackableScaler` via its `/scale` subresource. The product operator drives a state
machine that runs pre/post-scale hooks before propagating replica changes to the
StatefulSet.

Scaling configuration is expressed via a `ReplicasConfig` enum on role groups:
`Fixed(n)` for static counts, `Hpa(config)` for HPA-driven scaling, `ExternallyScaled`
for user-managed scalers, and `Auto(min, max)` for operator-generated HPAs (not yet
implemented). The `HpaConfig` struct exposes only user-relevant fields (`maxReplicas`,
`minReplicas`, `metrics`, `behavior`); the operator fills in `scaleTargetRef` internally
when building the actual `HorizontalPodAutoscaler`. The operator creates and manages
StackableScaler and HPA resources as implementation details — users configure scaling
entirely through the product cluster CRD.

NiFi and Trino are the initial implementations: NiFi's `pre_scale` hook offloads,
disconnects, and deletes nodes from the NiFi cluster via the REST API before scale-down.
Trino's `pre_scale` hook gracefully shuts down workers via the Trino REST API, waiting
for active queries to complete.

Changes span four repositories:
- **operator-rs** — CRD definition, state machine, hook trait, `ReplicasConfig` enum,
  `build_scaler()`/`build_hpa_from_user_spec()` helpers, `ClusterResource` impls
- **commons-operator** — CRD rollout, admission webhook (validation-only)
- **nifi-operator** — `NifiScalingHooks` implementation, `ReplicasConfig`-based reconcile
- **trino-operator** — `TrinoScalingHooks` implementation, `ReplicasConfig`-based reconcile
  with worker-only guard

---

## Motivation

Kubernetes HPAs can target StatefulSets directly via the `/scale` subresource. This
works mechanically but bypasses the Stackable operator entirely — the operator has no
opportunity to run product-specific lifecycle tasks before or after a scaling event.

For NiFi this is a concrete problem: NiFi cluster nodes must be **offloaded** (their
flowfile data redistributed to remaining nodes) before removal. Scaling the StatefulSet
directly terminates pods without offloading, causing data loss or cluster instability.

For Trino, workers must be **gracefully shut down** — active queries need to complete
before the worker is terminated, otherwise query results are lost.

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

### 3. `ReplicasConfig` enum replaces `replicas: 0` convention

Scaling configuration lives entirely in the role group `replicas` field via a
`ReplicasConfig` enum with four variants: `Fixed(n)`, `Hpa(spec)`, `Auto(min, max)`,
and `ExternallyScaled`. The operator creates and manages StackableScaler and HPA
resources as implementation details.

**Rejected:** The `replicas: 0` convention previously used to signal "externally
managed replicas." This was rejected for several reasons:

- **Poor user experience.** Setting `replicas: 0` to mean "externally managed" is
  counterintuitive. Users see `replicas: 0` and reasonably assume zero pods.
- **StackableScaler as implementation detail.** Under `replicas: 0`, users had to
  understand and manually create StackableScaler resources. With `ReplicasConfig`,
  the StackableScaler is created automatically — users never interact with it
  directly unless they choose `ExternallyScaled`, where exposing the scaler is the
  explicit intent.
- **Single place of configuration.** All scaling configuration lives in one field in
  the product cluster CRD. No second resource to create, no label conventions to
  follow, no implicit coupling.
- **Validation clarity.** `Fixed(0)` is explicitly rejected. The old model could not
  distinguish between "I want zero replicas" and "I want external scaling."

**Rejected:** Separate CRD for scaling configuration. Fragments the configuration
surface and requires two resources to be kept in sync.

### 4. Owner references and `.owns()` replace label-based discovery

Each product operator watches StackableScaler and HPA resources via `.owns()`, which
uses owner references for event routing. The operator sets owner references and
standard Stackable labels on the StackableScaler and HPA via `build_scaler()` and
`build_hpa_from_user_spec()` before passing them to `ClusterResources.add()`.

**Rejected:** Label-based discovery via `.watches()` with a manually-written mapper
and a `stackable.tech/cluster-kind` label injected by the admission webhook. This
required mutation logic in the webhook, a server-side label filter per operator, and
complex mapper closures. Owner references are the standard Kubernetes pattern for
parent-child resource relationships.

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

**Implementation note:** The `stackable-webhook` crate does not yet provide a
`ValidatingWebhook` type. The handler uses the existing `MutatingWebhook` framework
but never returns patches, making it functionally identical to a validating webhook.

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

### 11. Slimmed StackableScaler spec

The StackableScaler spec contains only `replicas: i32`. Identity is conveyed through
owner references and standard Stackable labels (`app.kubernetes.io/name`, `instance`,
`managed-by`, `component`, `role-group`), all set by `build_scaler()`. The operator
uses `.owns()` for event routing, so no additional discovery fields are needed.

**Rejected:** Including `clusterRef`, `role`, and `roleGroup` in the spec (the
original design). These fields duplicated information already present in labels and
owner references, and required a label-injection webhook to maintain. Removing them
simplified the CRD, eliminated the webhook mutation, and aligned with standard
Kubernetes ownership patterns.

### 12. Trino worker-only guard

Only Trino worker role groups support `Hpa`, `Auto`, and `ExternallyScaled` variants.
Coordinators are rejected at reconcile time if a non-`Fixed` variant is configured.
This prevents accidental scaling of the coordinator, which is a singleton in most
Trino deployments and has no graceful scale-down mechanism.

---

## CRD: `StackableScaler`

```yaml
apiVersion: autoscaling.stackable.tech/v1alpha1
kind: StackableScaler
metadata:
  name: my-nifi-nodes-default-scaler
  labels:
    app.kubernetes.io/name: nifi
    app.kubernetes.io/instance: my-nifi
    app.kubernetes.io/managed-by: nifi.stackable.tech_nificluster
    app.kubernetes.io/component: nodes
    app.kubernetes.io/role-group: default
  ownerReferences:
    - apiVersion: nifi.stackable.tech/v1alpha1
      kind: NifiCluster
      name: my-nifi
      uid: ...
spec:
  replicas: 5          # written by HPA via /scale subresource
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

## User-facing configuration

```yaml
apiVersion: nifi.stackable.tech/v1alpha1
kind: NifiCluster
metadata:
  name: my-nifi
spec:
  nodes:
    roleGroups:
      # Static replica count (default)
      static:
        replicas: 3

      # HPA-driven scaling — operator creates StackableScaler + HPA
      autoscaled:
        replicas:
          hpa:
            maxReplicas: 10
            metrics:
              - type: Resource
                resource:
                  name: cpu
                  target:
                    type: Utilization
                    averageUtilization: 80

      # External scaler — operator creates StackableScaler, user manages HPA/KEDA
      external:
        replicas: "externallyScaled"
```

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

### Reconcile integration (per role group)

```
reconcile()
  for each role group:
    1. Read ReplicasConfig from role group spec (default: Fixed(1))
    2. Match on variant:
       ├─ Fixed(n) → StatefulSet replicas = n, no scaler
       ├─ Hpa(config) / ExternallyScaled →
       │    a. Read existing StackableScaler (if any) to preserve spec.replicas
       │    b. build_scaler() with existing replicas (default 1 for new scalers)
       │    c. cluster_resources.add() (server-side apply)
       │    d. initialize_scaler_status() if new (prevents scale-to-zero)
       │    e. For Hpa: build_hpa_from_user_spec() + cluster_resources.add()
       │    f. StatefulSet replicas from scaler status
       └─ Auto → error (not yet implemented)
    3. Build and apply StatefulSet with effective replica count
    4. If scaler exists:
       a. Compute statefulset_stable from applied STS status
       b. Construct ProductScalingHooks
       c. Call reconcile_scaler(scaler, hooks, client, stable, selector, role_group)
       d. Log ScalingCondition (Healthy / Progressing / Failed)
       e. Collect requeue Action
    5. Orphan cleanup removes stale scalers/HPAs on variant switch
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

### Trino pre-scale hook (scale-down)

For each worker being removed, the hook sends `PUT /v1/info/state` with body
`"SHUTTING_DOWN"` to the Trino worker REST API. The worker stops accepting new queries
and completes running ones. The hook polls each worker's state until all report
`INACTIVE`, then returns `Done`.

Scale-up returns `Done` immediately.

---

## Detailed changes

### `operator-rs`

| File | Description |
|------|-------------|
| `crd/scaler/mod.rs` | `StackableScaler` CRD types (`StackableScalerSpec` with only `replicas`, `StackableScalerStatus`, `ScalerStage`, `ScalerState`, `FailedStage`), `Display` for `ScalerStage`, manual `JsonSchema` impl, serialization tests |
| `crd/scaler/replicas_config.rs` | `ReplicasConfig` enum (`Fixed`/`Hpa`/`Auto`/`ExternallyScaled`), custom `Deserialize` impl (bare integers, strings, tagged objects), `validate()` method with HPA bounds checks, `HpaConfig` (user-facing fields only: `max_replicas`, `min_replicas`, `metrics`, `behavior`), `AutoConfig`, `ValidationError` |
| `crd/scaler/hooks.rs` | `ScalingHooks` trait with RPITIT, `ScalingContext`, `ScalingDirection`, `HookOutcome`, `ScalingCondition`, `ScalingResult` |
| `crd/scaler/reconciler.rs` | `reconcile_scaler` entry point (takes `role_group_name` as parameter), `next_stage` pure function, status patching, `handle_hook_failure` helper, state-transition unit tests |
| `crd/scaler/builder.rs` | `build_scaler()` — constructs StackableScaler with labels and owner reference |
| `crd/scaler/hpa_builder.rs` | `build_hpa_from_user_spec(&HpaConfig, ...)` — constructs full `HorizontalPodAutoscalerSpec` from user config and fills `scaleTargetRef`; `scale_target_ref()`, `initialize_scaler_status()` |
| `crd/scaler/cluster_resource_impl.rs` | `DeepMerge` impl for StackableScaler |
| `crd/scaler/job_tracker.rs` | `JobTracker::start_or_check` for Job-based hooks |
| `cluster_resources.rs` | `impl ClusterResource` for StackableScaler and HPA, added to `delete_orphaned_resources()` |
| `role_utils.rs` | `RoleGroup.replicas` changed from `Option<u16>` to `Option<ReplicasConfig>` |

### `commons-operator`

| File | Description |
|------|-------------|
| `webhooks/scaler_admission.rs` | Validation-only webhook: on UPDATE, fetches live object from API to check stage, denies `spec.replicas` changes during active scaling. Uses `MutatingWebhook` framework (no `ValidatingWebhook` in stackable-webhook yet) but never returns patches. |
| `webhooks/mod.rs` | Registers `scaler_admission` webhook |
| `main.rs` | `--disable-scaler-admission-webhook` CLI flag |

### `nifi-operator`

| File | Description |
|------|-------------|
| `operations/scaling.rs` | `NifiScalingHooks` implementing `ScalingHooks` trait — version-conditional scale-down |
| `operations/nifi_api.rs` | `NifiApiClient` — reqwest-based client for NiFi REST API |
| `operations/credentials.rs` | `resolve_single_user_credentials` — reads admin password from Secret |
| `controller.rs` | `ReplicasConfig`-based reconcile loop: reads existing StackableScaler to preserve replicas, `build_scaler()` + `cluster_resources.add()` for Hpa/ExternallyScaled, `initialize_scaler_status()` for new scalers, `build_hpa_from_user_spec()` for Hpa variant, `reconcile_scaler()` call with scaler state machine. Error variants: `BuildScaler`, `BuildHpa`, `ApplyScaler`, `ApplyHpa`, `InitializeScalerStatus`, `GetExistingScaler`, `AutoScalingNotYetImplemented` |
| `main.rs` | `.owns()` registration for StackableScaler and HorizontalPodAutoscaler |
| `reporting_task/mod.rs` | Updated role group selection logic for `ReplicasConfig` |

### `trino-operator`

| File | Description |
|------|-------------|
| `operations/scaling.rs` | `TrinoScalingHooks` implementing `ScalingHooks` — graceful worker shutdown |
| `controller.rs` | `ReplicasConfig`-based reconcile loop (same pattern as nifi): reads existing StackableScaler to preserve replicas, worker-only guard rejecting non-Fixed variants for coordinators. Error variants: `BuildScaler`, `BuildHpa`, `ApplyScaler`, `ApplyHpa`, `InitializeScalerStatus`, `GetExistingScaler`, `AutoScalingNotYetImplemented`, `ScalingNotSupportedForRole` |
| `main.rs` | `.owns()` registration for StackableScaler and HorizontalPodAutoscaler |
| `crd/mod.rs` | `num_workers()` and `coordinator_pods()` updated for `ReplicasConfig` (Fixed → count, dynamic variants → 0) |

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

4. **`ScalingCondition` not yet propagated to cluster status.** The `ScalingResult`
   includes a `ScalingCondition` that should be written to the cluster CR's
   `status.conditions`, but operators currently only log it.

5. **`Auto` variant not yet implemented.** The `ReplicasConfig::Auto` variant is
   defined but returns an error at reconcile time. Per-product HPA templates
   (choosing metrics, stabilization windows) are a follow-up task.

6. **trino-operator compilation not verified.** The trino-operator changes could not
   be compilation-checked on the development machine due to missing `openssl-sys`
   development headers. The code follows the same pattern as the verified nifi-operator
   changes.

---

## Test plan

- [ ] Unit tests pass: `cargo test --all-features` in operator-rs, commons-operator, nifi-operator
- [ ] State machine transitions: unit tests in `reconciler.rs` covering all stage transitions
- [ ] `ReplicasConfig` deserialization: tests for integer, tagged object, string, and null inputs
- [ ] `ReplicasConfig` validation: tests for `Fixed(0)`, `Auto` min/max constraints, HPA max/min bounds
- [ ] `build_scaler()`: tests for labels, owner reference, and generated name
- [ ] `build_hpa_from_user_spec()`: tests for `scaleTargetRef` set from `HpaConfig`
- [ ] `ScalingDirection`: unit tests (up, down, equal)
- [ ] `NifiScalingHooks`: unit tests (direction shortcut, pod FQDN, API URL, version detection)
- [ ] Deploy to cluster, create NifiCluster with `replicas: { hpa: { maxReplicas: 5, ... } }`
- [ ] Verify externally-set StackableScaler replicas persist across reconciles
- [ ] Verify StackableScaler and HPA are auto-created with correct owner references
- [ ] Verify `spec.replicas` change rejected during PreScaling/Scaling/PostScaling
- [ ] Verify `spec.replicas` change allowed during Idle and Failed
- [ ] Trigger scale-down, verify NiFi nodes are offloaded before StatefulSet scales
- [ ] Trigger scale-up, verify immediate propagation (no pre-scale work)
- [ ] Verify `Failed` state is terminal and recoverable via retry annotation
- [ ] Switch from `Hpa` to `Fixed` — verify scaler and HPA are cleaned up
- [ ] Verify Trino coordinator rejects non-Fixed replicas config
