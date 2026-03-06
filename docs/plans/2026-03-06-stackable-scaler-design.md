# StackableScaler Design

**Date:** 2026-03-06
**Status:** Approved
**Scope:** `operator-rs` (shared infrastructure) + `nifi-operator` (proof of concept)

---

## Problem Statement

Kubernetes Horizontal Pod Autoscalers (HPAs) can target StatefulSets directly, but doing so bypasses the Stackable operator entirely. The operator has no opportunity to run product-specific tasks before or after scaling — for example, NiFi requires nodes to be offloaded before scale-down. The goal is to introduce a `StackableScaler` resource that the HPA targets instead, giving operators a controlled hook mechanism around scale events.

---

## Solution Overview

Introduce a new `StackableScaler` CRD defined in `operator-rs` and installed via the commons operator. Each `StackableScaler` targets a specific role group in a Stackable cluster. The HPA targets the `StackableScaler` via its `/scale` subresource. The product operator reads desired replicas from the scaler, runs pre/post-scale hooks (potentially long-running and async), and only propagates the new replica count to the StatefulSet once pre-scale hooks complete.

---

## `StackableScaler` CRD

### Location

`operator-rs/crates/stackable-operator/src/crd/scaler/`
Follows the S3Connection pattern: defined in `operator-rs`, installed platform-wide via the commons operator.

### Spec

```yaml
apiVersion: autoscaling.stackable.tech/v1alpha1
kind: StackableScaler
metadata:
  name: nifi-nodes-default
spec:
  replicas: 5          # written by HPA via /scale subresource
  clusterRef:
    kind: NifiCluster  # no apiVersion — CRD versioning handles conversions
    name: my-nifi
  role: nodes
  roleGroup: default
```

The `/scale` subresource is exposed with:

```
specReplicasPath:   .spec.replicas
statusReplicasPath: .status.replicas
labelSelectorPath:  .status.selector
```

### Status

```yaml
status:
  replicas: 3          # current StatefulSet target — read by HPA
  selector: "..."      # pod label selector — used by HPA for pod counting
  desiredReplicas: 5   # target being worked towards (copied from spec.replicas on Idle→PreScaling)
  currentState:
    stage: Scaling     # Idle | PreScaling | Scaling | PostScaling | Failed
    lastTransitionTime: "2026-03-06T10:05:00Z"
    # only present when stage == Failed:
    # failedAt: PreScaling
    # reason: "NiFi API unreachable: connection timeout"
```

`status.replicas` is the single source of truth for current replica count. It is updated when the StatefulSet target changes (during the `Scaling` stage) — not only at `Idle`. `status.desiredReplicas` tracks the target being worked towards and is set once when the state machine leaves `Idle`.

---

## Operator Watch Filtering

Each product operator watches only `StackableScaler` resources relevant to it. Server-side filtering is achieved via a label:

```
stackable.tech/cluster-kind: NifiCluster
```

This label is **set automatically** by a mutating admission webhook in the commons operator, which reads `spec.clusterRef.kind` on `StackableScaler` creation and `UPDATE`. Operators do not require users to set this label manually.

Each operator's watch uses a label selector:

```rust
watcher::Config::default().labels("stackable.tech/cluster-kind=NifiCluster")
```

The watch mapper filters by `clusterRef.name` and maps the changed scaler back to its owning cluster object to trigger reconciliation.

---

## Activation Convention

A `StackableScaler` is only **effective** for a role group where `replicas` is set to `0` in the cluster spec. This builds on the existing convention where `replicas: 0` signals "externally managed — do not touch StatefulSet replicas".

- `replicas: 0`, no `StackableScaler` → existing crutch behaviour (raw HPA targeting StatefulSet directly). Backwards compatible.
- `replicas: 0`, `StackableScaler` present → full hook machinery active, `StackableScaler` is the HPA target.

A **validating webhook** on `StackableScaler` creation rejects the resource if the target role group does not have `replicas: 0`. A **mutating webhook** seeds `spec.replicas` from the current StatefulSet replica count on `StackableScaler` creation, so the HPA starts from the actual running state.

---

## State Machine

State lives in `StackableScaler.status.currentState.stage`. operator-rs owns all transitions.

```
Idle
 │  spec.replicas != status.desiredReplicas
 ▼
PreScaling ──── HookOutcome::InProgress ──▶ (requeue → re-enter PreScaling)
 │  HookOutcome::Done
 ▼
Scaling    ──── StatefulSet not yet stable  ▶ (requeue → re-enter Scaling)
 │  StatefulSet replicas == desired && pods ready
 ▼
PostScaling ─── HookOutcome::InProgress ──▶ (requeue → re-enter PostScaling)
 │  HookOutcome::Done
 ▼
Idle  (status.replicas = status.desiredReplicas)
```

**`spec.replicas` changes mid-flight** (i.e. while stage is not `Idle`) are rejected by the validating webhook, causing the HPA to record `AbleToScale: False` and back off until the current transition completes.

**Any stage can transition to `Failed`** on `Err(_)` from a hook. In `Failed`, the state machine stops — no automatic retries.

### Recovery from `Failed`

Annotation-driven reset (Kubernetes-idiomatic, non-destructive):

```bash
kubectl annotate stackablescaler nifi-nodes-default \
  autoscaling.stackable.tech/retry: "true"
```

The operator strips the annotation and resets `status.currentState.stage` to `Idle`.

---

## Failure Surfacing

Failures are surfaced in two places:

1. **`StackableScaler.status.currentState`** — `stage: Failed`, `failedAt`, `reason`, `lastTransitionTime`
2. **Cluster CR conditions** — a `ScalingFailed` condition on `NifiCluster.status.conditions` (and equivalent on other operators)

When in `Failed` state, the validating webhook rejects writes to `spec.replicas`, causing the HPA to surface `AbleToScale: False` in its own conditions.

---

## `ScalingHooks` Trait (operator-rs)

```rust
pub enum ScalingDirection { Up, Down }

pub struct ScalingContext<'a> {
    pub client: &'a Client,
    pub role_group: &'a RoleGroupRef<DynamicObject>,
    pub current_replicas: u32,
    pub desired_replicas: u32,
    pub direction: ScalingDirection,  // derived by operator-rs
}

pub enum HookOutcome {
    Done,        // advance to next stage
    InProgress,  // requeue and re-check on next reconcile
}

pub trait ScalingHooks {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Self::Error>;
    async fn post_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Self::Error>;

    /// Called once when entering Failed state. Best-effort — errors are logged only.
    async fn on_failure(
        &self,
        ctx: &ScalingContext<'_>,
        failed_stage: FailedStage,
    ) -> Result<(), Self::Error> {
        Ok(()) // default: no cleanup
    }
}
```

Default implementations of `pre_scale` and `post_scale` return `HookOutcome::Done` immediately, so operators only implement the hooks they need.

---

## `reconcile_scaler` Entry Point (operator-rs)

```rust
pub struct ScalingResult {
    pub action: Action,
    pub condition: ClusterCondition, // operator MUST propagate to cluster CR
}

pub async fn reconcile_scaler<H>(
    scaler: &StackableScaler,
    hooks: &H,
    client: &Client,
) -> Result<ScalingResult, Error>
where
    H: ScalingHooks,
```

Returning `ScalingResult` (rather than just `Action`) forces the operator to handle and propagate the condition — the compiler will warn on unused results.

---

## `JobTracker` Helper (operator-rs)

For the common case of launching a Kubernetes `Job` as the hook implementation:

```rust
pub struct JobTracker;

impl JobTracker {
    /// Ensures the Job exists (creates if absent), checks completion status.
    /// Returns Done on success, InProgress while running, Err on failure.
    /// Cleans up completed/failed Jobs automatically.
    pub async fn start_or_check(
        client: &Client,
        job: Job,
        namespace: &str,
    ) -> Result<HookOutcome, JobTrackerError>;
}
```

The operator constructs the `Job` with product-specific logic and passes it to `JobTracker::start_or_check`, returning the result directly as `HookOutcome`.

---

## `resolve_replicas` Helper (operator-rs)

Prevents operators from accidentally ignoring the scaler when building StatefulSets:

```rust
pub fn resolve_replicas(
    role_group: &RoleGroup<...>,
    scaler: Option<&StackableScaler>,
) -> Option<i32>
```

If `scaler` is `Some`, returns `scaler.status.replicas`. Otherwise falls back to `role_group.replicas`. This is the only supported path for replica resolution — operators should not read `role_group.replicas` directly.

---

## Operator Integration (nifi-operator proof of concept)

### Watch setup (`main.rs`)

```rust
.watches(
    watch_namespace.get_api::<StackableScaler>(&client),
    watcher::Config::default().labels("stackable.tech/cluster-kind=NifiCluster"),
    move |scaler| {
        scaler_store.state().into_iter()
            .filter(|nifi| {
                scaler.spec.cluster_ref.name == nifi.name_any()
                    && scaler.namespace() == nifi.namespace()
            })
            .map(|nifi| ObjectRef::from_obj(&*nifi))
    },
)
```

### Hook implementation (`src/operations/scaling.rs`)

```rust
struct NifiScalingHooks { /* nifi client, cluster ref */ }

impl ScalingHooks for NifiScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        match ctx.direction {
            ScalingDirection::Down =>
                JobTracker::start_or_check(ctx.client, self.build_offload_job(ctx), ns).await,
            ScalingDirection::Up => Ok(HookOutcome::Done),
        }
    }

    async fn post_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        Ok(HookOutcome::Done) // NiFi needs no post-scale for now
    }
}
```

### Reconcile flow changes (`controller.rs`)

```
1. For each role group, fetch StackableScaler if present            [new]
2. For role groups with a scaler:
   a. Call reconcile_scaler(scaler, &NifiScalingHooks { .. }, client) [new]
   b. Propagate ScalingResult.condition to NifiCluster status         [new]
   c. If result.action is not Idle, return early for this role group  [new]
3. Build StatefulSet using resolve_replicas(role_group, scaler)       [modified]
```

StatefulSet reconciliation for a role group only proceeds when the scaler stage is `Idle` or `Scaling`.

---

## Known Limitations

- **Standard HPA has no back-pressure mechanism.** The HPA cannot be told to permanently stop by the scale target. When in `Failed` state, writes to `spec.replicas` are rejected by the webhook, causing the HPA to surface `AbleToScale: False` and back off — but it will keep retrying indefinitely. This is the limit of standard Kubernetes HPA. KEDA or a custom metrics server would be required for tighter lifecycle control.

---

## Rollout Plan

1. Implement `StackableScaler` CRD, state machine, `ScalingHooks` trait, `JobTracker`, and `resolve_replicas` in `operator-rs`
2. Implement mutating + validating webhooks for `StackableScaler` in the commons operator
3. Implement `NifiScalingHooks` and integrate `reconcile_scaler` into `nifi-operator` as proof of concept
4. Validate with a live HPA targeting a NiFi cluster
5. Roll out `ScalingHooks` integration to remaining operators
