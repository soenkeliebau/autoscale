# StackableScaler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

> **Partially superseded (2026-03-19):** This plan was the original implementation guide for
> StackableScaler. It was executed and then superseded by the `ReplicasConfig` rewrite plan at
> `docs/superpowers/plans/2026-03-19-replicas-config-rewrite.md`. The `replicas: 0` activation,
> `clusterRef` fields, and label-based discovery described here are no longer current. The state
> machine, hook framework, and reconciler logic remain largely intact.

**Goal:** Implement the `StackableScaler` CRD, state machine, and hook framework in `operator-rs`, then wire it into `nifi-operator` as the proof-of-concept.

**Architecture:** `StackableScaler` is a new CRD in `operator-rs` exposing a Kubernetes `/scale` subresource. HPAs target it instead of StatefulSets directly. `operator-rs` drives a state machine (`Idle → PreScaling → Scaling → PostScaling → Idle`) and calls a `ScalingHooks` trait implemented per-product. NiFi uses this to offload nodes before scale-down via a Kubernetes Job. The feature activates only when `roleGroup.replicas == 0`.

**Tech Stack:** Rust, kube-rs, k8s-openapi, snafu (errors), schemars (JSON schema), tokio (async). Tests use `rstest` and standard `cargo test`.

**Design doc:** `docs/plans/2026-03-06-stackable-scaler-design.md`

---

## Orientation

Before starting, read these files to understand the patterns used:
- `operator-rs/crates/stackable-operator/src/crd/s3/connection/mod.rs` — CRD definition pattern
- `operator-rs/crates/stackable-operator/src/crd/mod.rs` — existing `ClusterRef` type and module layout
- `operator-rs/crates/stackable-operator/src/status/condition/mod.rs` — `ClusterCondition`, `Degraded`, `Progressing` types
- `nifi-operator/rust/operator-binary/src/operations/graceful_shutdown.rs` — pattern for operations modules
- `nifi-operator/rust/operator-binary/src/controller.rs:482-570` — the rolegroup loop where replica resolution happens
- `nifi-operator/rust/operator-binary/src/main.rs:135-178` — how watches are registered

The test command for operator-rs is: `cargo test -p stackable-operator`
The test command for nifi-operator is: `cargo test -p stackable-nifi-operator`

---

## Task 1: `StackableScaler` CRD types

**Files:**
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs`
- Modify: `operator-rs/crates/stackable-operator/src/crd/mod.rs`

### Step 1: Add `pub mod scaler` to the crd module

In `operator-rs/crates/stackable-operator/src/crd/mod.rs`, add at the top with the other module declarations:

```rust
pub mod scaler;
```

### Step 2: Write the failing tests first

Create `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` with only the tests (it won't compile yet — that's fine):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaler_stage_idle_serializes() {
        let stage = ScalerStage::Idle;
        let json = serde_json::to_string(&stage).unwrap();
        assert_eq!(json, r#"{"stage":"Idle"}"#);
    }

    #[test]
    fn scaler_stage_failed_serializes() {
        let stage = ScalerStage::Failed {
            failed_at: FailedStage::PreScaling,
            reason: "timeout".to_string(),
        };
        let json = serde_json::to_value(&stage).unwrap();
        assert_eq!(json["stage"], "Failed");
        assert_eq!(json["failedAt"], "PreScaling");
        assert_eq!(json["reason"], "timeout");
    }

    #[test]
    fn spec_replicas_zero_means_externally_managed() {
        let spec = StackableScalerSpec {
            replicas: 0,
            cluster_ref: UnknownClusterRef {
                kind: "NifiCluster".to_string(),
                name: "my-nifi".to_string(),
            },
            role: "nodes".to_string(),
            role_group: "default".to_string(),
        };
        assert_eq!(spec.replicas, 0);
    }
}
```

### Step 3: Run to confirm compile failure

```bash
cd operator-rs && cargo test -p stackable-operator scaler 2>&1 | head -20
```

Expected: compile errors — types not defined yet.

### Step 4: Implement the types

Replace the contents of `mod.rs` with the full implementation (keeping the tests at the bottom):

```rust
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod job_tracker;
pub mod reconciler;

/// A type-erased cluster reference used in StackableScaler.
/// Does not carry apiVersion — CRD versioning handles conversions.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnknownClusterRef {
    /// The Kind of the target cluster, e.g. `NifiCluster`.
    pub kind: String,
    /// The name of the target cluster resource.
    pub name: String,
}

/// Spec for a StackableScaler.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StackableScalerSpec {
    /// Desired replica count. Written by the HPA via the /scale subresource.
    /// This field only takes effect when the referenced roleGroup has `replicas: 0`.
    pub replicas: i32,

    /// Reference to the Stackable cluster resource this scaler manages.
    pub cluster_ref: UnknownClusterRef,

    /// The role within the cluster (e.g. `nodes`).
    pub role: String,

    /// The role group within the role (e.g. `default`).
    pub role_group: String,
}

/// Which stage of a scaling operation failed.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum FailedStage {
    PreScaling,
    Scaling,
    PostScaling,
}

/// The current stage of the scaling state machine.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "stage", rename_all = "camelCase")]
pub enum ScalerStage {
    /// No scaling in progress.
    Idle,
    /// Running pre-scale hooks (e.g. node offload). StatefulSet NOT yet changed.
    PreScaling,
    /// StatefulSet replica count has been updated; waiting for it to stabilise.
    Scaling,
    /// StatefulSet is stable at new replica count; running post-scale hooks.
    PostScaling,
    /// A hook or the scaling step failed. State machine is paused.
    Failed {
        #[serde(rename = "failedAt")]
        failed_at: FailedStage,
        reason: String,
    },
}

/// The current state of the scaler, including when it last changed.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalerState {
    #[serde(flatten)]
    pub stage: ScalerStage,
    pub last_transition_time: Time,
}

/// Status of a StackableScaler.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StackableScalerStatus {
    /// Current StatefulSet replica target. This is the value the HPA reads
    /// via the /scale subresource (statusReplicasPath).
    pub replicas: i32,

    /// Pod label selector string for HPA pod counting (labelSelectorPath).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,

    /// The replica count the state machine is working towards.
    /// Set once when transitioning from Idle and cleared on return to Idle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_replicas: Option<i32>,

    /// The current state of the scaling state machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_state: Option<ScalerState>,
}

/// A StackableScaler exposes a /scale subresource so that a Kubernetes
/// HorizontalPodAutoscaler can target it instead of a StatefulSet directly.
/// The Stackable operator for the referenced cluster reads this resource and
/// runs pre/post-scale hooks before propagating replica changes to the
/// underlying StatefulSet.
///
/// A StackableScaler only becomes effective when the targeted roleGroup has
/// `replicas: 0` in the cluster spec. When no StackableScaler is present,
/// `replicas: 0` falls back to the existing behaviour of not managing
/// StatefulSet replicas (allowing raw HPA targeting as before).
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "autoscaling.stackable.tech",
    version = "v1alpha1",
    kind = "StackableScaler",
    namespaced,
    status = "StackableScalerStatus",
    subresource = "scale",
    scale = r#"{"specReplicasPath":".spec.replicas","statusReplicasPath":".status.replicas","labelSelectorPath":".status.selector"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct StackableScalerSpec {
    // NOTE: The field definition above is intentionally duplicated here for
    // the kube derive macro. Remove the standalone struct definition above
    // once kube derive generates it — keep the fields identical.
    pub replicas: i32,
    pub cluster_ref: UnknownClusterRef,
    pub role: String,
    pub role_group: String,
}

// Re-export the generated type at the module level
pub use self::StackableScaler;
```

> **Note on `CustomResource` derive and duplicate spec:** The `#[derive(CustomResource)]` macro generates a `StackableScaler` struct whose spec _is_ `StackableScalerSpec`. Remove the standalone `StackableScalerSpec` struct definition above — keep only the one annotated with `#[kube(...)]`. The split above is for clarity during TDD.

Correct final shape (consolidate before committing):

```rust
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "autoscaling.stackable.tech",
    version = "v1alpha1",
    kind = "StackableScaler",
    namespaced,
    status = "StackableScalerStatus",
    subresource = "scale",
    scale = r#"{"specReplicasPath":".spec.replicas","statusReplicasPath":".status.replicas","labelSelectorPath":".status.selector"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct StackableScalerSpec {
    pub replicas: i32,
    pub cluster_ref: UnknownClusterRef,
    pub role: String,
    pub role_group: String,
}
```

Create the two stub submodules so it compiles:

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/job_tracker.rs
// (empty stub for now)
```

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/reconciler.rs
// (empty stub for now)
```

### Step 5: Run tests

```bash
cd operator-rs && cargo test -p stackable-operator scaler
```

Expected: all 3 tests pass.

### Step 6: Commit

```bash
git add crates/stackable-operator/src/crd/scaler/ crates/stackable-operator/src/crd/mod.rs
git commit -m "feat(operator-rs): add StackableScaler CRD types"
```

---

## Task 2: `ScalingHooks` trait and related types

**Files:**
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/hooks.rs`
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (add `pub mod hooks`)

### Step 1: Write failing tests in `hooks.rs`

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/hooks.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_scale_up() {
        let dir = ScalingDirection::from_replicas(3, 5);
        assert_eq!(dir, ScalingDirection::Up);
    }

    #[test]
    fn direction_scale_down() {
        let dir = ScalingDirection::from_replicas(5, 3);
        assert_eq!(dir, ScalingDirection::Down);
    }
}
```

Add `pub mod hooks;` to `scaler/mod.rs`.

### Step 2: Run to confirm failure

```bash
cd operator-rs && cargo test -p stackable-operator hooks
```

Expected: compile error — types not defined.

### Step 3: Implement

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/hooks.rs
use std::future::Future;

use crate::client::Client;

use super::FailedStage;

/// Whether this is a scale-up or scale-down operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalingDirection {
    Up,
    Down,
}

impl ScalingDirection {
    pub fn from_replicas(current: i32, desired: i32) -> Self {
        if desired >= current { Self::Up } else { Self::Down }
    }
}

/// Context passed to hook implementations.
pub struct ScalingContext<'a> {
    pub client: &'a Client,
    pub namespace: &'a str,
    pub role_group_name: &'a str,  // e.g. "default"
    pub current_replicas: i32,
    pub desired_replicas: i32,
    pub direction: ScalingDirection,
}

/// Return value from a hook invocation.
#[derive(Debug, Eq, PartialEq)]
pub enum HookOutcome {
    /// Hook completed successfully — advance to next stage.
    Done,
    /// Hook still running — operator-rs will requeue and re-call on next reconcile.
    InProgress,
}

/// The result returned from `reconcile_scaler` to the operator.
/// The operator MUST propagate `condition` to the cluster CR status.
pub struct ScalingResult {
    pub action: kube::runtime::controller::Action,
    pub scaling_condition: ScalingCondition,
}

/// Condition to propagate to the cluster CR.
#[derive(Debug)]
pub enum ScalingCondition {
    /// No scaling in progress or just completed successfully.
    Healthy,
    /// Scaling is actively in progress.
    Progressing { stage: String },
    /// Scaling failed — details for the cluster CR condition message.
    Failed { stage: FailedStage, reason: String },
}

/// Trait implemented by each product operator to provide scaling hooks.
///
/// Default implementations return `HookOutcome::Done` immediately so operators
/// only need to override the hooks they actually use.
pub trait ScalingHooks {
    type Error: std::error::Error + Send + Sync + 'static;

    fn pre_scale(
        &self,
        ctx: &ScalingContext<'_>,
    ) -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send {
        async { Ok(HookOutcome::Done) }
    }

    fn post_scale(
        &self,
        ctx: &ScalingContext<'_>,
    ) -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send {
        async { Ok(HookOutcome::Done) }
    }

    /// Called once when entering Failed state. Best-effort: errors are logged only.
    fn on_failure(
        &self,
        ctx: &ScalingContext<'_>,
        failed_stage: &FailedStage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}
```

### Step 4: Run tests

```bash
cd operator-rs && cargo test -p stackable-operator hooks
```

Expected: 2 tests pass.

### Step 5: Commit

```bash
git add crates/stackable-operator/src/crd/scaler/
git commit -m "feat(operator-rs): add ScalingHooks trait and related types"
```

---

## Task 3: `resolve_replicas` helper

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs`

This is a pure function — easiest to TDD.

### Step 1: Write failing tests (add to the `tests` module in `scaler/mod.rs`)

```rust
#[test]
fn resolve_replicas_no_scaler_uses_role_group() {
    assert_eq!(resolve_replicas(Some(3), None), Some(3));
}

#[test]
fn resolve_replicas_with_scaler_uses_status() {
    let mut scaler = StackableScaler::new("test", StackableScalerSpec {
        replicas: 5,
        cluster_ref: UnknownClusterRef { kind: "NifiCluster".into(), name: "n".into() },
        role: "nodes".into(),
        role_group: "default".into(),
    });
    scaler.status = Some(StackableScalerStatus {
        replicas: 3,
        ..Default::default()
    });
    assert_eq!(resolve_replicas(Some(0), Some(&scaler)), Some(3));
}

#[test]
fn resolve_replicas_none_role_group_and_no_scaler() {
    assert_eq!(resolve_replicas(None, None), None);
}

#[test]
fn resolve_replicas_with_scaler_overrides_zero() {
    // roleGroup.replicas must be 0 for scaler to be effective;
    // if it isn't 0, scaler is ignored (validation webhook should prevent
    // this combination, but we defensively fall back)
    let mut scaler = StackableScaler::new("test", StackableScalerSpec {
        replicas: 5,
        cluster_ref: UnknownClusterRef { kind: "NifiCluster".into(), name: "n".into() },
        role: "nodes".into(),
        role_group: "default".into(),
    });
    scaler.status = Some(StackableScalerStatus { replicas: 4, ..Default::default() });
    // replicas != 0 → ignore scaler, use roleGroup value
    assert_eq!(resolve_replicas(Some(3), Some(&scaler)), Some(3));
}
```

### Step 2: Run to confirm failure

```bash
cd operator-rs && cargo test -p stackable-operator resolve_replicas
```

### Step 3: Implement (add to `scaler/mod.rs` outside the test block)

```rust
/// Resolve the replica count for a StatefulSet, taking an optional StackableScaler into account.
///
/// A scaler is only effective when `role_group_replicas` is `Some(0)` — this is the convention
/// that signals "externally managed replicas". In all other cases the role group value is used.
///
/// Call this instead of reading `role_group.replicas` directly whenever building a StatefulSet.
pub fn resolve_replicas(
    role_group_replicas: Option<i32>,
    scaler: Option<&StackableScaler>,
) -> Option<i32> {
    match (role_group_replicas, scaler) {
        (Some(0), Some(s)) => s.status.as_ref().map(|st| st.replicas),
        (replicas, _) => replicas,
    }
}
```

### Step 4: Run tests

```bash
cd operator-rs && cargo test -p stackable-operator resolve_replicas
```

Expected: 4 tests pass.

### Step 5: Commit

```bash
git add crates/stackable-operator/src/crd/scaler/mod.rs
git commit -m "feat(operator-rs): add resolve_replicas helper"
```

---

## Task 4: State machine — `reconcile_scaler`

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/reconciler.rs`

This is the core of operator-rs. It drives the state machine one step per call, writes status back, and returns `ScalingResult`.

### Step 1: Write failing tests

```rust
// At the top of reconciler.rs — these are unit tests for pure transition logic.
// We test the `next_stage` function in isolation before testing the full async reconciler.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::scaler::{FailedStage, ScalerStage};
    use crate::crd::scaler::hooks::HookOutcome;

    #[test]
    fn idle_transitions_to_prescaling_when_replicas_differ() {
        let stage = next_stage(
            ScalerStage::Idle,
            3,     // current
            5,     // desired
            || HookOutcome::Done,   // pre_scale outcome (unused in Idle)
            || HookOutcome::Done,   // post_scale outcome (unused in Idle)
            false, // statefulset_stable
        );
        assert_eq!(stage, NextStage::Transition(ScalerStage::PreScaling));
    }

    #[test]
    fn idle_stays_idle_when_replicas_match() {
        let stage = next_stage(
            ScalerStage::Idle, 3, 3,
            || HookOutcome::Done, || HookOutcome::Done, false,
        );
        assert_eq!(stage, NextStage::NoChange);
    }

    #[test]
    fn prescaling_advances_when_hook_done() {
        let stage = next_stage(
            ScalerStage::PreScaling, 3, 5,
            || HookOutcome::Done, || HookOutcome::Done, false,
        );
        assert_eq!(stage, NextStage::Transition(ScalerStage::Scaling));
    }

    #[test]
    fn prescaling_stays_when_hook_in_progress() {
        let stage = next_stage(
            ScalerStage::PreScaling, 3, 5,
            || HookOutcome::InProgress, || HookOutcome::Done, false,
        );
        assert_eq!(stage, NextStage::Requeue);
    }

    #[test]
    fn scaling_advances_when_statefulset_stable() {
        let stage = next_stage(
            ScalerStage::Scaling, 3, 5,
            || HookOutcome::Done, || HookOutcome::Done, true,
        );
        assert_eq!(stage, NextStage::Transition(ScalerStage::PostScaling));
    }

    #[test]
    fn scaling_requeues_when_not_stable() {
        let stage = next_stage(
            ScalerStage::Scaling, 3, 5,
            || HookOutcome::Done, || HookOutcome::Done, false,
        );
        assert_eq!(stage, NextStage::Requeue);
    }

    #[test]
    fn postscaling_returns_to_idle_when_hook_done() {
        let stage = next_stage(
            ScalerStage::PostScaling, 3, 5,
            || HookOutcome::Done, || HookOutcome::Done, true,
        );
        assert_eq!(stage, NextStage::Transition(ScalerStage::Idle));
    }

    #[test]
    fn failed_stays_failed() {
        let failed = ScalerStage::Failed {
            failed_at: FailedStage::PreScaling,
            reason: "err".to_string(),
        };
        let stage = next_stage(
            failed.clone(), 3, 5,
            || HookOutcome::Done, || HookOutcome::Done, false,
        );
        assert_eq!(stage, NextStage::NoChange);
    }
}
```

### Step 2: Run to confirm failure

```bash
cd operator-rs && cargo test -p stackable-operator reconciler
```

### Step 3: Implement `next_stage` (pure, synchronous — easy to test)

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/reconciler.rs
use crate::crd::scaler::{FailedStage, ScalerStage, StackableScaler, StackableScalerStatus};
use crate::crd::scaler::hooks::{
    HookOutcome, ScalingCondition, ScalingContext, ScalingDirection, ScalingHooks, ScalingResult,
};
use crate::client::Client;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::runtime::controller::Action;
use snafu::{ResultExt, Snafu};
use std::time::Duration;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to patch StackableScaler status"))]
    PatchStatus { source: crate::client::Error },
}

/// Internal decision from `next_stage`.
#[derive(Debug, Eq, PartialEq)]
enum NextStage {
    NoChange,
    Requeue,
    Transition(ScalerStage),
}

/// Pure function: given current state and hook/stability outcomes, return the next stage decision.
/// Hook outcomes are passed as closures so tests can inject them without async.
fn next_stage(
    current: ScalerStage,
    current_replicas: i32,
    desired_replicas: i32,
    pre_outcome: impl FnOnce() -> HookOutcome,
    post_outcome: impl FnOnce() -> HookOutcome,
    statefulset_stable: bool,
) -> NextStage {
    match current {
        ScalerStage::Idle => {
            if current_replicas != desired_replicas {
                NextStage::Transition(ScalerStage::PreScaling)
            } else {
                NextStage::NoChange
            }
        }
        ScalerStage::PreScaling => match pre_outcome() {
            HookOutcome::Done => NextStage::Transition(ScalerStage::Scaling),
            HookOutcome::InProgress => NextStage::Requeue,
        },
        ScalerStage::Scaling => {
            if statefulset_stable {
                NextStage::Transition(ScalerStage::PostScaling)
            } else {
                NextStage::Requeue
            }
        }
        ScalerStage::PostScaling => match post_outcome() {
            HookOutcome::Done => NextStage::Transition(ScalerStage::Idle),
            HookOutcome::InProgress => NextStage::Requeue,
        },
        ScalerStage::Failed { .. } => NextStage::NoChange,
    }
}
```

### Step 4: Run unit tests

```bash
cd operator-rs && cargo test -p stackable-operator reconciler
```

Expected: all 8 tests pass.

### Step 5: Implement the async `reconcile_scaler` entry point

Add below `next_stage` in `reconciler.rs`. This calls hooks asynchronously and patches status:

```rust
/// Requeue interval while a hook is in progress.
const REQUEUE_HOOK_IN_PROGRESS: Duration = Duration::from_secs(10);
/// Requeue interval while waiting for StatefulSet to stabilise.
const REQUEUE_SCALING: Duration = Duration::from_secs(5);

/// Drive the StackableScaler state machine one step forward.
///
/// Call this from the product operator's reconcile function for each role group
/// that has an associated StackableScaler. The returned `ScalingResult.condition`
/// MUST be propagated to the cluster CR status.
pub async fn reconcile_scaler<H>(
    scaler: &StackableScaler,
    hooks: &H,
    client: &Client,
    statefulset_stable: bool,
    selector: &str,
) -> Result<ScalingResult, Error>
where
    H: ScalingHooks,
{
    let status = scaler.status.clone().unwrap_or_default();
    let current_stage = status
        .current_state
        .as_ref()
        .map(|s| s.stage.clone())
        .unwrap_or(ScalerStage::Idle);

    let current_replicas = status.replicas;
    let desired_replicas = scaler.spec.replicas;
    let namespace = scaler.metadata.namespace.as_deref().unwrap_or_default();

    let ctx = ScalingContext {
        client,
        namespace,
        role_group_name: &scaler.spec.role_group,
        current_replicas,
        desired_replicas,
        direction: ScalingDirection::from_replicas(current_replicas, desired_replicas),
    };

    // Run hooks asynchronously and convert Err to Failed stage transition
    let (pre_outcome, pre_err) = match current_stage {
        ScalerStage::PreScaling => match hooks.pre_scale(&ctx).await {
            Ok(o) => (Some(o), None),
            Err(e) => (None, Some((FailedStage::PreScaling, e.to_string()))),
        },
        _ => (None, None),
    };

    let (post_outcome, post_err) = match current_stage {
        ScalerStage::PostScaling => match hooks.post_scale(&ctx).await {
            Ok(o) => (Some(o), None),
            Err(e) => (None, Some((FailedStage::PostScaling, e.to_string()))),
        },
        _ => (None, None),
    };

    // If a hook errored, transition to Failed
    if let Some((failed_at, reason)) = pre_err.or(post_err) {
        let _ = hooks.on_failure(&ctx, &failed_at).await; // best-effort
        let new_status = build_status(
            &status,
            ScalerStage::Failed { failed_at: failed_at.clone(), reason: reason.clone() },
            current_replicas,
            selector,
            desired_replicas,
        );
        patch_status(client, scaler, new_status).await.context(PatchStatusSnafu)?;
        return Ok(ScalingResult {
            action: Action::await_change(),
            scaling_condition: ScalingCondition::Failed { stage: failed_at, reason },
        });
    }

    let next = next_stage(
        current_stage.clone(),
        current_replicas,
        desired_replicas,
        || pre_outcome.clone().unwrap_or(HookOutcome::Done),
        || post_outcome.clone().unwrap_or(HookOutcome::Done),
        statefulset_stable,
    );

    match next {
        NextStage::NoChange => Ok(ScalingResult {
            action: Action::await_change(),
            scaling_condition: ScalingCondition::Healthy,
        }),
        NextStage::Requeue => {
            let interval = match current_stage {
                ScalerStage::Scaling => REQUEUE_SCALING,
                _ => REQUEUE_HOOK_IN_PROGRESS,
            };
            Ok(ScalingResult {
                action: Action::requeue(interval),
                scaling_condition: ScalingCondition::Progressing {
                    stage: format!("{:?}", current_stage),
                },
            })
        }
        NextStage::Transition(new_stage) => {
            // When completing PostScaling → Idle, update replicas to desired
            let new_replicas = match &new_stage {
                ScalerStage::Idle => desired_replicas,
                _ => current_replicas,
            };
            let desired = match &new_stage {
                ScalerStage::Idle => None, // clear desired on completion
                ScalerStage::PreScaling => Some(desired_replicas), // set when first leaving Idle
                _ => status.desired_replicas,
            };
            let condition = match &new_stage {
                ScalerStage::Idle => ScalingCondition::Healthy,
                s => ScalingCondition::Progressing { stage: format!("{:?}", s) },
            };
            let new_status = StackableScalerStatus {
                replicas: new_replicas,
                selector: Some(selector.to_string()),
                desired_replicas: desired,
                current_state: Some(crate::crd::scaler::ScalerState {
                    stage: new_stage,
                    last_transition_time: Time(k8s_openapi::chrono::Utc::now()),
                }),
            };
            patch_status(client, scaler, new_status).await.context(PatchStatusSnafu)?;
            Ok(ScalingResult {
                action: Action::requeue(REQUEUE_SCALING),
                scaling_condition: condition,
            })
        }
    }
}

fn build_status(
    current: &StackableScalerStatus,
    stage: ScalerStage,
    replicas: i32,
    selector: &str,
    desired: i32,
) -> StackableScalerStatus {
    StackableScalerStatus {
        replicas,
        selector: Some(selector.to_string()),
        desired_replicas: Some(desired),
        current_state: Some(crate::crd::scaler::ScalerState {
            stage,
            last_transition_time: Time(k8s_openapi::chrono::Utc::now()),
        }),
    }
}

async fn patch_status(
    client: &Client,
    scaler: &StackableScaler,
    status: StackableScalerStatus,
) -> Result<(), crate::client::Error> {
    client
        .apply_patch_status(
            "stackable-operator",
            scaler,
            &status,
        )
        .await
        .map(|_| ())
}
```

### Step 6: Run all scaler tests

```bash
cd operator-rs && cargo test -p stackable-operator -- crd::scaler
```

Expected: all tests pass, code compiles.

### Step 7: Commit

```bash
git add crates/stackable-operator/src/crd/scaler/reconciler.rs
git commit -m "feat(operator-rs): implement reconcile_scaler state machine"
```

---

## Task 5: `JobTracker` helper

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/job_tracker.rs`

### Step 1: Write failing tests

```rust
// job_tracker.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_name_is_stable_for_same_scaler_and_stage() {
        let name1 = job_name("my-scaler", "pre-scale");
        let name2 = job_name("my-scaler", "pre-scale");
        assert_eq!(name1, name2);
    }

    #[test]
    fn job_name_differs_for_different_stages() {
        let pre = job_name("my-scaler", "pre-scale");
        let post = job_name("my-scaler", "post-scale");
        assert_ne!(pre, post);
    }

    #[test]
    fn job_name_is_valid_dns_label() {
        let name = job_name("my-scaler", "pre-scale");
        assert!(name.len() <= 63);
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    }
}
```

### Step 2: Run to confirm failure

```bash
cd operator-rs && cargo test -p stackable-operator job_tracker
```

### Step 3: Implement

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/job_tracker.rs
use k8s_openapi::api::batch::v1::{Job, JobStatus};
use kube::ResourceExt;
use snafu::{ResultExt, Snafu};

use crate::{client::Client, crd::scaler::hooks::HookOutcome};

#[derive(Debug, Snafu)]
pub enum JobTrackerError {
    #[snafu(display("failed to apply Job '{job_name}'"))]
    ApplyJob {
        source: crate::client::Error,
        job_name: String,
    },
    #[snafu(display("failed to get Job '{job_name}'"))]
    GetJob {
        source: crate::client::Error,
        job_name: String,
    },
    #[snafu(display("Job '{job_name}' failed"))]
    JobFailed { job_name: String },
}

/// Derive a stable, DNS-safe Job name from a scaler name and a stage label.
pub fn job_name(scaler_name: &str, stage: &str) -> String {
    let raw = format!("{scaler_name}-{stage}");
    // Truncate to 63 chars, strip trailing hyphens
    let truncated = &raw[..raw.len().min(63)];
    truncated.trim_end_matches('-').to_string()
}

pub struct JobTracker;

impl JobTracker {
    /// Ensures the Job exists (creates if absent), then checks its completion.
    ///
    /// - Returns `Ok(HookOutcome::Done)` when the Job succeeded.
    /// - Returns `Ok(HookOutcome::InProgress)` while the Job is still running.
    /// - Returns `Err` if the Job failed or could not be applied.
    ///
    /// Completed (successful) Jobs are deleted automatically after returning `Done`.
    pub async fn start_or_check(
        client: &Client,
        job: Job,
        namespace: &str,
    ) -> Result<HookOutcome, JobTrackerError> {
        let name = job.name_any();

        // Apply (server-side apply — idempotent)
        client
            .apply_patch("stackable-operator", &job, &job)
            .await
            .context(ApplyJobSnafu { job_name: name.clone() })?;

        // Fetch current status
        let current: Job = client
            .get(&name, namespace)
            .await
            .context(GetJobSnafu { job_name: name.clone() })?;

        let status = current.status.as_ref();

        if status.and_then(|s| s.succeeded).unwrap_or(0) > 0 {
            // Clean up and signal done
            let _ = client.delete(&current).await; // best-effort
            return Ok(HookOutcome::Done);
        }

        if status.and_then(|s| s.failed).unwrap_or(0) > 0 {
            return Err(JobTrackerError::JobFailed { job_name: name });
        }

        Ok(HookOutcome::InProgress)
    }
}
```

### Step 4: Run tests

```bash
cd operator-rs && cargo test -p stackable-operator job_tracker
```

Expected: 3 tests pass.

### Step 5: Commit

```bash
git add crates/stackable-operator/src/crd/scaler/job_tracker.rs
git commit -m "feat(operator-rs): add JobTracker helper for async job-based hooks"
```

---

## Task 6: Export from operator-rs

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs`

Add public re-exports so operators get a clean import surface:

```rust
// At the bottom of scaler/mod.rs, before #[cfg(test)]:
pub use hooks::{HookOutcome, ScalingCondition, ScalingContext, ScalingDirection, ScalingHooks, ScalingResult};
pub use job_tracker::{JobTracker, JobTrackerError};
pub use reconciler::{reconcile_scaler, Error as ReconcilerError};
```

Verify the whole crate still compiles and all tests pass:

```bash
cd operator-rs && cargo test -p stackable-operator
```

### Commit

```bash
git add crates/stackable-operator/src/crd/scaler/mod.rs
git commit -m "feat(operator-rs): re-export scaler public API"
```

---

## Task 7: `NifiScalingHooks` implementation

**Files:**
- Create: `nifi-operator/rust/operator-binary/src/operations/scaling.rs`
- Modify: `nifi-operator/rust/operator-binary/src/operations/mod.rs`

### Step 1: Add module declaration

In `operations/mod.rs`, add:

```rust
pub mod scaling;
```

### Step 2: Write failing tests

```rust
// operations/scaling.rs
#[cfg(test)]
mod tests {
    use super::*;
    use stackable_operator::crd::scaler::hooks::{HookOutcome, ScalingDirection};

    fn make_ctx<'a>(
        client: &'a stackable_operator::client::Client,
        direction: ScalingDirection,
    ) -> stackable_operator::crd::scaler::ScalingContext<'a> {
        stackable_operator::crd::scaler::ScalingContext {
            client,
            namespace: "default",
            role_group_name: "test",
            current_replicas: if direction == ScalingDirection::Down { 3 } else { 1 },
            desired_replicas: if direction == ScalingDirection::Down { 1 } else { 3 },
            direction,
        }
    }

    // Note: full async hook tests require a k8s client. These tests verify
    // the direction-routing logic by checking the scale-up path returns Done
    // without needing a real client (no job is launched on scale-up).
    #[tokio::test]
    async fn pre_scale_up_returns_done_immediately() {
        // Build a minimal NifiScalingHooks with no real cluster config
        // Scale-up pre_scale should return Done without touching k8s
        // (tested via the no-op path in the implementation)
        // If this test fails, check that ScalingDirection::Up returns Ok(Done)
        // without calling build_offload_job.
        let hooks = NifiScalingHooks {
            cluster_name: "test".to_string(),
            namespace: "default".to_string(),
        };
        // We can't call hooks.pre_scale without a real client, but we can
        // verify the match arm at unit level:
        assert_eq!(
            hooks.pre_scale_direction_outcome(ScalingDirection::Up),
            Some(HookOutcome::Done)
        );
        assert_eq!(
            hooks.pre_scale_direction_outcome(ScalingDirection::Down),
            None  // None = needs to run async job logic
        );
    }
}
```

### Step 3: Implement

```rust
// operations/scaling.rs
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::scaler::{
        ScalingContext,
        hooks::{HookOutcome, ScalingDirection, ScalingHooks},
        job_tracker::JobTracker,
    },
    k8s_openapi::api::batch::v1::{Job, JobSpec},
    k8s_openapi::api::core::v1::{Container, PodSpec, PodTemplateSpec},
    kube::core::ObjectMeta,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("scaling job failed"))]
    JobFailed {
        source: stackable_operator::crd::scaler::job_tracker::JobTrackerError,
    },
}

/// Holds the context needed to build scaling jobs for a NiFi cluster.
pub struct NifiScalingHooks {
    pub cluster_name: String,
    pub namespace: String,
}

impl NifiScalingHooks {
    /// Internal helper for unit-testing direction routing without a real client.
    /// Returns `Some(HookOutcome::Done)` when no async work is needed,
    /// `None` when async job logic must run.
    pub(crate) fn pre_scale_direction_outcome(&self, dir: ScalingDirection) -> Option<HookOutcome> {
        match dir {
            ScalingDirection::Up => Some(HookOutcome::Done), // no pre-scale needed for scale-up
            ScalingDirection::Down => None,
        }
    }

    fn build_offload_job(&self, ctx: &ScalingContext<'_>) -> Job {
        // TODO: Replace the container image and command with the real NiFi
        // offload tooling once the offload mechanism is finalised.
        // The job name is deterministic so JobTracker can find it on requeue.
        let job_name = stackable_operator::crd::scaler::job_tracker::job_name(
            &format!("{}-{}", self.cluster_name, ctx.role_group_name),
            "pre-scale",
        );
        Job {
            metadata: ObjectMeta {
                name: Some(job_name),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(JobSpec {
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        restart_policy: Some("Never".to_string()),
                        containers: vec![Container {
                            name: "offload".to_string(),
                            // Placeholder — replace with actual NiFi offload image/command
                            image: Some("curlimages/curl:latest".to_string()),
                            command: Some(vec![
                                "sh".to_string(),
                                "-c".to_string(),
                                format!(
                                    "echo 'Offloading NiFi node for cluster {} rolegroup {}'",
                                    self.cluster_name, ctx.role_group_name
                                ),
                            ]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

impl ScalingHooks for NifiScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        match ctx.direction {
            ScalingDirection::Up => Ok(HookOutcome::Done),
            ScalingDirection::Down => {
                let job = self.build_offload_job(ctx);
                JobTracker::start_or_check(ctx.client, job, ctx.namespace)
                    .await
                    .context(JobFailedSnafu)
            }
        }
    }

    // post_scale: use default (Done immediately) — NiFi needs no post-scale action currently
}
```

### Step 4: Run tests

```bash
cd nifi-operator && cargo test -p stackable-nifi-operator scaling
```

Expected: 1 test passes, code compiles.

### Step 5: Commit

```bash
git add rust/operator-binary/src/operations/
git commit -m "feat(nifi-operator): implement NifiScalingHooks"
```

---

## Task 8: Watch setup in `main.rs`

**Files:**
- Modify: `nifi-operator/rust/operator-binary/src/main.rs`

### Step 1: Add import

In `main.rs`, add to the imports block:

```rust
use stackable_operator::crd::scaler::StackableScaler;
```

### Step 2: Add the watch after the existing `config_map_store` lines

Find the block:
```rust
let config_map_store = nifi_controller.store();
```

Add after it:
```rust
let scaler_store = nifi_controller.store();
```

Then in the `.watches()` chain, add after the existing ConfigMap watch:

```rust
.watches(
    watch_namespace.get_api::<StackableScaler>(&client),
    // Server-side label filter set by the commons-operator mutating webhook.
    // This ensures each operator only caches its own StackableScalers.
    watcher::Config::default().labels("stackable.tech/cluster-kind=NifiCluster"),
    move |scaler| {
        scaler_store
            .state()
            .into_iter()
            .filter(move |nifi| {
                let Ok(nifi) = &nifi.0 else { return false };
                scaler.spec.cluster_ref.name == nifi.name_any()
                    && scaler.metadata.namespace == nifi.metadata.namespace
            })
            .map(|nifi| ObjectRef::from_obj(&*nifi))
    },
)
```

### Step 3: Compile check

```bash
cd nifi-operator && cargo build -p stackable-nifi-operator 2>&1 | grep -E "^error"
```

Expected: no errors.

### Step 4: Commit

```bash
git add rust/operator-binary/src/main.rs
git commit -m "feat(nifi-operator): watch StackableScaler resources for NifiCluster"
```

---

## Task 9: Controller integration

**Files:**
- Modify: `nifi-operator/rust/operator-binary/src/controller.rs`

This is the most surgical change — only two areas need modification.

### Step 1: Add imports

Add to the `stackable_operator` import block in `controller.rs`:

```rust
use stackable_operator::crd::scaler::{
    StackableScaler, resolve_replicas,
    reconcile_scaler,
    hooks::ScalingCondition,
};
```

### Step 2: Add scaler Error variants

In the `Error` enum in `controller.rs`, add:

```rust
#[snafu(display("failed to fetch StackableScaler for rolegroup {rolegroup}"))]
FetchScaler {
    source: stackable_operator::client::Error,
    rolegroup: RoleGroupRef<v1alpha1::NifiCluster>,
},

#[snafu(display("StackableScaler reconciliation failed for rolegroup {rolegroup}"))]
ScalerReconcile {
    source: stackable_operator::crd::scaler::reconciler::Error,
    rolegroup: RoleGroupRef<v1alpha1::NifiCluster>,
},
```

### Step 3: Replace the replica resolution block

Find this block in `reconcile_nifi` (around line 548–553):

```rust
let role_group = role.role_groups.get(&rolegroup.role_group);
let replicas =
    if cluster_version_update_state == ClusterVersionUpdateState::UpdateRequested {
        Some(0)
    } else {
        role_group.and_then(|rg| rg.replicas).map(i32::from)
    };
```

Replace with:

```rust
let role_group = role.role_groups.get(&rolegroup.role_group);

// Fetch the StackableScaler for this role group, if one exists.
// A scaler is only active when role_group.replicas == Some(0).
let scaler: Option<StackableScaler> = {
    let rg_replicas = role_group.and_then(|rg| rg.replicas);
    if rg_replicas == Some(0) {
        client
            .list_with_label_selector::<StackableScaler>(
                nifi.metadata.namespace.as_deref().context(ObjectHasNoNamespaceSnafu)?,
                &format!(
                    "stackable.tech/cluster-kind=NifiCluster,stackable.tech/cluster-name={}",
                    nifi.name_any()
                ),
            )
            .await
            .context(FetchScalerSnafu { rolegroup: rolegroup.clone() })?
            .into_iter()
            .find(|s| s.spec.role == rolegroup.role && s.spec.role_group == rolegroup.role_group)
    } else {
        None
    }
};

// Run the scaler state machine if a scaler is present.
if let Some(ref s) = scaler {
    let selector = Labels::role_group_selector(
        nifi, APP_NAME, &rolegroup.role, &rolegroup.role_group,
    )
    .context(LabelBuildSnafu)?
    .to_string();

    let scaling_result = reconcile_scaler(
        s,
        &crate::operations::scaling::NifiScalingHooks {
            cluster_name: nifi.name_any(),
            namespace: nifi.metadata.namespace.clone().unwrap_or_default(),
        },
        client,
        // StatefulSet stable = ss_cond_builder already has this info;
        // for now pass true when stage is Scaling and SS ready replicas match.
        // TODO: pass real StatefulSet stability from ss_cond_builder
        false,
        &selector,
    )
    .await
    .context(ScalerReconcileSnafu { rolegroup: rolegroup.clone() })?;

    // Propagate condition to NifiCluster status
    match scaling_result.scaling_condition {
        ScalingCondition::Failed { ref reason, .. } => {
            tracing::warn!(
                rolegroup = %rolegroup,
                reason = %reason,
                "StackableScaler failed"
            );
        }
        _ => {}
    }
}

let replicas = if cluster_version_update_state == ClusterVersionUpdateState::UpdateRequested {
    Some(0)
} else {
    resolve_replicas(
        role_group.and_then(|rg| rg.replicas).map(i32::from),
        scaler.as_ref(),
    )
};
```

> **Note on StatefulSet stability:** The `statefulset_stable` parameter currently passes `false` as a placeholder. Wire it up by reading from the `StatefulSet` object fetched during reconcile, checking that `status.ready_replicas == spec.replicas`. This can be a follow-up commit once the basic flow is validated.

### Step 4: Compile check

```bash
cd nifi-operator && cargo build -p stackable-nifi-operator 2>&1 | grep -E "^error"
```

Fix any compile errors before proceeding.

### Step 5: Run all tests

```bash
cd nifi-operator && cargo test -p stackable-nifi-operator
```

Expected: all tests pass.

### Step 6: Commit

```bash
git add rust/operator-binary/src/controller.rs
git commit -m "feat(nifi-operator): integrate StackableScaler into reconcile loop"
```

---

## Task 10: Follow-up — StatefulSet stability signal

**Files:**
- Modify: `nifi-operator/rust/operator-binary/src/controller.rs`

The `statefulset_stable` parameter in Task 9 was left as `false`. Wire it up:

After the StatefulSet is applied via `cluster_resources.add(client, rg_statefulset)`, read its status:

```rust
let sts_ready = rg_statefulset
    .status
    .as_ref()
    .and_then(|s| s.ready_replicas)
    .unwrap_or(0);
let sts_desired = rg_statefulset.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
let statefulset_stable = sts_ready == sts_desired && sts_desired > 0;
```

Pass `statefulset_stable` into the `reconcile_scaler` call.

```bash
cd nifi-operator && cargo test -p stackable-nifi-operator
git add rust/operator-binary/src/controller.rs
git commit -m "fix(nifi-operator): wire StatefulSet stability signal into scaler"
```

---

## Task 11: Webhook requirements (commons-operator — out of scope for this workspace)

These changes must be implemented in the commons-operator separately. Document them here for reference.

**Mutating webhook (on `StackableScaler` CREATE and UPDATE):**
1. Read `spec.clusterRef.kind` and set label `stackable.tech/cluster-kind: <kind>`
2. On CREATE only: fetch the StatefulSet for the referenced role group and seed `spec.replicas` from its current `.spec.replicas` value, so the HPA starts from the actual running replica count.

**Validating webhook (on `StackableScaler` CREATE):**
1. Look up the referenced cluster CR (using `clusterRef.kind` + `clusterRef.name`)
2. Find the specified `role` + `roleGroup` in the cluster spec
3. Reject if `roleGroup.replicas != 0` with message: `"StackableScaler is only effective when the target roleGroup has replicas: 0"`

**Validating webhook (on `StackableScaler` UPDATE of `spec.replicas`):**
1. If `status.currentState.stage` is not `Idle` and not `Failed`, reject with: `"Cannot update spec.replicas while scaling is in progress (stage: <stage>). Wait for the current operation to complete or enter Failed state."`

---

## Verification

After all tasks complete, verify end-to-end:

1. Deploy a NiFi cluster with `nodes.roleGroups.default.replicas: 0`
2. Create a `StackableScaler` targeting it — confirm `spec.replicas` is seeded from the current StatefulSet
3. Create an HPA targeting the `StackableScaler`
4. Observe HPA updating `spec.replicas` → operator transitions through `PreScaling → Scaling → PostScaling → Idle`
5. Confirm the offload Job runs during scale-down pre-scale phase
6. Confirm StatefulSet replica count only changes after the offload Job completes
