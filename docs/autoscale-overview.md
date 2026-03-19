# StackableScaler — Auto-Scaling Overview

---

## 1. CRD Relations

```
┌─────────────────────┐  writes spec.replicas   ┌─────────────────────────┐
│ HorizontalPod-      │ ──────────────────────►  │ StackableScaler         │
│ Autoscaler (HPA)    │  via /scale subresource  │ (autoscaling.stackable  │
│                     │                          │  .tech/v1alpha1)        │
│  metrics:           │  ◄──────────────────────  │                         │
│   - CPU / memory    │  reads status.replicas   │  spec:                  │
│   - custom metrics  │                          │    replicas: <n>        │
└─────────────────────┘                          │    clusterRef:          │
                                                 │      kind: NifiCluster  │
                                                 │      name: my-nifi      │
                                                 │    role: nodes          │
                                                 │    roleGroup: default   │
                                                 │  status:                │
                                                 │    replicas: <current>  │
                                                 │    desiredReplicas: <n> │
                                                 │    currentState:        │
                                                 │      stage: Idle        │
                                                 │    selector: "app=..."  │
                                                 └───────────┬─────────────┘
                                                             │
                                        operator watches &   │  label selector
                                        reconciles scaler    │  matches cluster
                                                             ▼
┌─────────────────────┐  reconcile   ┌──────────────────────────────────────┐
│ Product Operator    │ ◄──────────  │ Product Cluster CRD                  │
│ (nifi / trino)      │              │ (NifiCluster / TrinoCluster)         │
│                     │              │                                      │
│  - discovers scaler │              │  roleGroups:                         │
│    when replicas: 0 │              │    default:                          │
│  - calls            │              │      replicas: 0  ◄── opt-in signal  │
│    reconcile_scaler │              └──────────────────────────────────────┘
│  - applies STS with │
│    resolved replica  │
│    count             │
└─────────┬───────────┘
          │ owns
          ▼
┌─────────────────────┐
│ StatefulSet          │
│  replicas: <from     │
│   scaler status>     │
└─────────────────────┘
```

**Key convention:** `roleGroup.replicas: 0` signals "externally managed". The operator
looks up a `StackableScaler` via label selectors and reads the effective replica count
from its status.

---

## 2. Control Flow

### State Machine

```
    ┌───────────────────────────────────────────────────────┐
    │                                                       │
    ▼                                                       │
  Idle ──► PreScaling ──► Scaling ──► PostScaling ──► (back to Idle)
              │               │            │
              └───────────────┴────────────┘
                        │
                        ▼
                      Failed ──(retry annotation)──► Idle
```

### Reconcile Loop (per role group)

```
1. Discover StackableScaler
   └─ list by label selector, match clusterRef + role + roleGroup

2. Resolve replicas
   └─ replicas: 0 + scaler found  →  use scaler.status.replicas
   └─ replicas: N (N > 0)         →  use N directly (no scaler)

3. Build & apply StatefulSet with resolved replica count

4. Drive scaler state machine  (reconcile_scaler)
   ┌─────────────┬───────────────────────────────────────────────┐
   │ Stage       │ Action                                        │
   ├─────────────┼───────────────────────────────────────────────┤
   │ Idle        │ if current ≠ desired → transition PreScaling  │
   │ PreScaling  │ call hooks.pre_scale() — Done → Scaling       │
   │             │                         InProgress → requeue  │
   │ Scaling     │ wait for STS convergence → PostScaling        │
   │ PostScaling │ call hooks.post_scale() — Done → Idle         │
   │             │                           InProgress → requeue│
   │ Failed      │ wait for retry annotation → Idle              │
   └─────────────┴───────────────────────────────────────────────┘

5. Admission webhook (commons-operator)
   └─ rejects spec.replicas changes while stage ∈ {PreScaling, Scaling, PostScaling}
   └─ injects stackable.tech/cluster-kind label
```

### Product-Specific Hooks

| Product | pre_scale (scale-down)                                     | post_scale |
|---------|------------------------------------------------------------|------------|
| **NiFi**  | Offload → Disconnect → Delete nodes via NiFi REST API (version-aware: 1.x vs 2.x order) | default (Done) |
| **Trino** | PUT `/v1/info/state` = `SHUTTING_DOWN`, poll until `INACTIVE` | default (Done) |

---

## 3. Code Examples

### 3a. StackableScaler CRD (operator-rs)

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs

#[versioned(version(name = "v1alpha1"))]
#[derive(CustomResource, Deserialize, JsonSchema, Serialize, ...)]
#[kube(
    group = "autoscaling.stackable.tech",
    kind = "StackableScaler",
    status = "StackableScalerStatus",
    // /scale subresource — this is what the HPA targets
    scale = r#"{"specReplicasPath":".spec.replicas",
                "statusReplicasPath":".status.replicas",
                "labelSelectorPath":".status.selector"}"#
)]
pub struct StackableScalerSpec {
    pub replicas: i32,
    pub cluster_ref: UnknownClusterRef,
    pub role: String,
    pub role_group: String,
}

pub enum ScalerStage {
    Idle,
    PreScaling,
    Scaling,
    PostScaling,
    Failed { failed_at: FailedStage, reason: String },
}
```

### 3b. Replica Resolution (operator-rs)

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs

/// replicas: 0 + scaler present → read from scaler status
/// anything else                 → pass through unchanged
pub fn resolve_replicas(
    role_group_replicas: Option<i32>,
    scaler: Option<&v1alpha1::StackableScaler>,
) -> Option<i32> {
    match (role_group_replicas, scaler) {
        (Some(0), Some(s)) => s.status.as_ref().map(|st| st.replicas),
        (replicas, _) => replicas,
    }
}
```

### 3c. ScalingHooks Trait (operator-rs)

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/hooks.rs

pub trait ScalingHooks {
    type Error: std::error::Error + Send + Sync + 'static;

    /// PreScaling stage — e.g. drain/offload nodes before scale-down.
    /// Return Done to advance to Scaling, InProgress to requeue.
    fn pre_scale(&self, ctx: &ScalingContext<'_>)
        -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send
    { async { Ok(HookOutcome::Done) } }  // default: no-op

    /// PostScaling stage — e.g. rebalance after scale-up.
    fn post_scale(&self, ctx: &ScalingContext<'_>)
        -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send
    { async { Ok(HookOutcome::Done) } }

    /// Called on transition to Failed. Best-effort cleanup.
    fn on_failure(&self, ctx: &ScalingContext<'_>, failed_stage: &FailedStage)
        -> impl Future<Output = Result<(), Self::Error>> + Send
    { async { Ok(()) } }
}
```

### 3d. Operator Integration (nifi-operator controller, same pattern for trino)

```rust
// nifi-operator/rust/operator-binary/src/controller.rs  (simplified)

// 1. Discover scaler for role groups with replicas: 0
let scaler: Option<StackableScaler> = if rg_replicas == Some(0) {
    client
        .list_with_label_selector::<StackableScaler>(namespace, &selector)
        .await?
        .into_iter()
        .find(|s| s.spec.cluster_ref.name == nifi.name_any()
                && s.spec.role == rolegroup.role
                && s.spec.role_group == rolegroup.role_group)
} else { None };

// 2. Resolve effective replica count
let replicas = resolve_replicas(rg_replicas.map(i32::from), scaler.as_ref());

// 3. Build StatefulSet with resolved replicas
let rg_statefulset = build_statefulset(replicas, ...)?;
let applied_sts = cluster_resources.add(client, rg_statefulset).await?;

// 4. Drive scaler state machine (calls hooks at right stages)
if let Some(ref s) = scaler {
    let result = reconcile_scaler(
        s,
        &NifiScalingHooks { namespace, credentials, ... },
        client,
        statefulset_stable,
        &selector_string,
    ).await?;
}
```

### 3e. Trino Pre-Scale Hook (graceful shutdown)

```rust
// trino-operator/rust/operator-binary/src/operations/scaling.rs

impl ScalingHooks for TrinoScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        if !ctx.is_scale_down() { return Ok(HookOutcome::Done); }

        let mut any_in_progress = false;
        for ordinal in ctx.removed_ordinals() {
            let client = TrinoWorkerClient::new(&self.worker_base_url(ordinal))?;
            match client.get_state().await? {
                TrinoWorkerState::Active => {
                    client.initiate_shutdown().await?;  // PUT "SHUTTING_DOWN"
                    any_in_progress = true;
                }
                TrinoWorkerState::ShuttingDown => { any_in_progress = true; }
                TrinoWorkerState::Inactive => { /* ready for termination */ }
            }
        }
        Ok(if any_in_progress { HookOutcome::InProgress } else { HookOutcome::Done })
    }
}
```

### 3f. NiFi Pre-Scale Hook (node offload & decommission)

```rust
// nifi-operator/rust/operator-binary/src/operations/scaling.rs  (simplified)

impl ScalingHooks for NifiScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        if !ctx.is_scale_down() { return Ok(HookOutcome::Done); }

        let api = NifiApiClient::connect(self.api_base_url(0), &user, &pass).await?;
        let nodes = api.get_cluster_nodes().await?;

        // Version-aware sequence:
        // NiFi 1.x: CONNECTED → OFFLOADING → OFFLOADED → DISCONNECTING → DISCONNECTED → DELETE
        // NiFi 2.x: CONNECTED → DISCONNECTING → DISCONNECTED → OFFLOADING → OFFLOADED → DELETE
        for target in targets_being_removed {
            match target.status {
                Connected    => { api.offload_or_disconnect(target).await?; }
                Offloading   => { /* still draining, requeue */ }
                Offloaded    => { api.disconnect(target).await?; }
                Disconnected => { api.delete_node(target).await?; }
                // ...
            }
        }
        Ok(if any_in_progress { HookOutcome::InProgress } else { HookOutcome::Done })
    }
}
```

### 3g. Admission Webhook (commons-operator)

```rust
// commons-operator/rust/operator-binary/src/webhooks/scaler_admission.rs  (simplified)

async fn scaler_admission_handler(
    client: Arc<Client>,
    request: AdmissionRequest<StackableScaler>,
) -> AdmissionResponse {
    // On UPDATE: reject spec.replicas changes during active scaling
    if request.operation == Operation::Update {
        if new.spec.replicas != old.spec.replicas {
            let live = api.get(scaler_name).await?;
            if live.status.current_state.stage.is_scaling_in_progress() {
                return deny("Cannot update spec.replicas while scaling is in progress");
            }
        }
    }
    // Mutate: inject cluster-kind label from spec.clusterRef.kind
    patch.add("/metadata/labels/stackable.tech~1cluster-kind", cluster_kind);
    response.with_patch(patch)
}
```
