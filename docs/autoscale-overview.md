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
└─────────────────────┘                          │  status:                │
   ▲                                             │    replicas: <current>  │
   │ created by operator                         │    desiredReplicas: <n> │
   │ for Hpa variant                             │    currentState:        │
   │                                             │      stage: Idle        │
   │                                             │    selector: "app=..."  │
   │                                             └───────────┬─────────────┘
   │                                                         │
   │                                      owner reference    │  operator creates
   │                                      links scaler to    │  and manages via
   │                                      cluster CR         │  ClusterResources
   │                                                         ▼
┌──┴──────────────────┐  reconcile   ┌──────────────────────────────────────┐
│ Product Operator    │ ◄──────────  │ Product Cluster CRD                  │
│ (nifi / trino)      │              │ (NifiCluster / TrinoCluster)         │
│                     │              │                                      │
│  - matches on       │              │  roleGroups:                         │
│    ReplicasConfig   │              │    default:                          │
│  - creates scaler   │              │      replicas: 3          # Fixed    │
│    & HPA via        │              │    scaled:                           │
│    ClusterResources │              │      replicas:             # Hpa     │
│  - calls            │              │        hpa:                          │
│    reconcile_scaler │              │          spec:                       │
│  - applies STS with │              │            maxReplicas: 10           │
│    effective replica │              │    external:                         │
│    count             │              │      replicas:                       │
└─────────┬───────────┘              │        "externallyScaled" # External │
          │ owns                     └──────────────────────────────────────┘
          ▼
┌─────────────────────┐
│ StatefulSet          │
│  replicas: <from     │
│   scaler status or   │
│   Fixed(n) directly> │
└─────────────────────┘
```

**Key design:** Scaling configuration lives entirely in the `replicas` field of the role group
spec via the `ReplicasConfig` enum. The operator creates and manages StackableScaler and HPA
resources as implementation details — users only interact with them directly when using the
`ExternallyScaled` variant.

---

## 2. ReplicasConfig Enum

```yaml
# Static — operator sets StatefulSet replicas directly
replicas: 3                          # → ReplicasConfig::Fixed(3)

# HPA — operator creates StackableScaler + HPA from user-provided spec
replicas:
  hpa:
    spec:
      maxReplicas: 10
      metrics: [...]                 # → ReplicasConfig::Hpa(HpaConfig)

# Auto — operator generates a product-specific HPA (not yet implemented)
replicas:
  auto:
    minReplicas: 2
    maxReplicas: 10                  # → ReplicasConfig::Auto(AutoConfig)

# Externally scaled — operator creates StackableScaler only, user manages HPA/KEDA
replicas: "externallyScaled"         # → ReplicasConfig::ExternallyScaled

# Omitted / null — defaults to Fixed(1)
```

- `Fixed(0)` is rejected by validation.
- `Auto` with `minReplicas: 0` or `maxReplicas < minReplicas` is rejected.
- Bare integers deserialize as `Fixed(n)` via a custom `Deserialize` impl.

---

## 3. Control Flow

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
1. Read ReplicasConfig from role group spec (default: Fixed(1))

2. Match on variant:
   ├─ Fixed(n)
   │   └─ StatefulSet replicas = n, no scaler, no HPA
   │
   ├─ Hpa(hpa_config)
   │   ├─ build_scaler() → cluster_resources.add()
   │   ├─ initialize_scaler_status() if freshly created
   │   ├─ build_hpa_from_user_spec() → cluster_resources.add()
   │   ├─ StatefulSet replicas from scaler status
   │   └─ drive state machine (reconcile_scaler)
   │
   ├─ ExternallyScaled
   │   ├─ build_scaler() → cluster_resources.add()
   │   ├─ initialize_scaler_status() if freshly created
   │   ├─ No HPA (user manages external scaler)
   │   ├─ StatefulSet replicas from scaler status
   │   └─ drive state machine (reconcile_scaler)
   │
   └─ Auto(auto_config)
       └─ Not yet implemented — returns error

3. Apply StatefulSet with effective replica count

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
```

### Resource Lifecycle

- **Scaler and HPA are managed via `ClusterResources.add()`** — labels and owner references
  are set by `build_scaler()` / `build_hpa_from_user_spec()` before passing to `add()`.
- **Orphan cleanup** — switching from `Hpa` to `Fixed` automatically removes the scaler and
  HPA on the next reconcile via `delete_orphaned_resources()`.
- **Watch registration** — `.owns()` on StackableScaler and HPA routes events to the owning
  cluster CR via owner references (no label-based mappers needed).

### Product-Specific Hooks

| Product | pre_scale (scale-down)                                     | post_scale |
|---------|------------------------------------------------------------|------------|
| **NiFi**  | Offload → Disconnect → Delete nodes via NiFi REST API (version-aware: 1.x vs 2.x order) | default (Done) |
| **Trino** | PUT `/v1/info/state` = `SHUTTING_DOWN`, poll until `INACTIVE` | default (Done) |

---

## 4. Code Examples

### 4a. StackableScaler CRD (operator-rs)

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
}

pub enum ScalerStage {
    Idle,
    PreScaling,
    Scaling,
    PostScaling,
    Failed { failed_at: FailedStage, reason: String },
}
```

Identity is conveyed through owner references and standard Stackable labels
(`app.kubernetes.io/name`, `instance`, `managed-by`, `component`, `role-group`),
all set by `build_scaler()`.

### 4b. ReplicasConfig Enum (operator-rs)

```rust
// operator-rs/crates/stackable-operator/src/crd/scaler/replicas_config.rs

pub enum ReplicasConfig {
    Fixed(u16),
    Hpa(HpaConfig),
    Auto(AutoConfig),
    ExternallyScaled,
}

impl Default for ReplicasConfig {
    fn default() -> Self { Self::Fixed(1) }
}
```

Custom `Deserialize` impl handles bare integers (`3` → `Fixed(3)`), strings
(`"externallyScaled"` → `ExternallyScaled`), and tagged objects (`{"hpa": {...}}`).

### 4c. ScalingHooks Trait (operator-rs)

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

### 4d. Operator Integration (nifi-operator controller, same pattern for trino)

```rust
// nifi-operator/rust/operator-binary/src/controller.rs  (simplified)

let replicas_config = role_group
    .and_then(|rg| rg.replicas.clone())
    .unwrap_or_default();

let (replicas, scaler_to_reconcile) = match &replicas_config {
    ReplicasConfig::Fixed(n) => {
        (Some(i32::from(*n)), None)
    }
    ReplicasConfig::Hpa(hpa_config) => {
        // Build and apply scaler
        let scaler = build_scaler(&nifi.name_any(), APP_NAME, namespace,
            &rolegroup.role, &rolegroup.role_group, 1, &owner_ref, OPERATOR_NAME)?;
        let applied_scaler = cluster_resources.add(client, scaler).await?;

        // Initialize status on freshly created scalers to prevent scale-to-zero
        if applied_scaler.status.is_none() {
            initialize_scaler_status(client, &applied_scaler, 1, &selector_string).await?;
        }

        // Build and apply HPA targeting the scaler
        let target_ref = scale_target_ref(&scaler_name, "autoscaling.stackable.tech", "v1alpha1");
        let hpa = build_hpa_from_user_spec(&hpa_config.spec, &target_ref, ...)?;
        cluster_resources.add(client, hpa).await?;

        let replicas = applied_scaler.status.as_ref().map(|st| st.replicas);
        (replicas, Some(applied_scaler))
    }
    ReplicasConfig::ExternallyScaled => {
        // Same as Hpa but no HPA created
        let scaler = build_scaler(...)?;
        let applied_scaler = cluster_resources.add(client, scaler).await?;
        if applied_scaler.status.is_none() {
            initialize_scaler_status(client, &applied_scaler, 1, &selector_string).await?;
        }
        let replicas = applied_scaler.status.as_ref().map(|st| st.replicas);
        (replicas, Some(applied_scaler))
    }
    ReplicasConfig::Auto(_) => return Err(Error::AutoScalingNotYetImplemented { .. }),
};

// Build StatefulSet with effective replicas
let rg_statefulset = build_node_rolegroup_statefulset(replicas, ...)?;
let applied_sts = cluster_resources.add(client, rg_statefulset).await?;

// Drive scaler state machine (calls hooks at right stages)
if let Some(ref s) = scaler_to_reconcile {
    reconcile_scaler(s, &NifiScalingHooks { ... }, client,
        statefulset_stable, &selector_string, &rolegroup.role_group).await?;
}
```

### 4e. Trino Pre-Scale Hook (graceful shutdown)

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

### 4f. NiFi Pre-Scale Hook (node offload & decommission)

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

### 4g. Admission Webhook (commons-operator)

```rust
// commons-operator/rust/operator-binary/src/webhooks/scaler_admission.rs  (simplified)
//
// Uses the MutatingWebhook framework (no ValidatingWebhook in stackable-webhook yet),
// but never returns patches — functionally a validating webhook.

async fn scaler_admission_handler(
    client: Arc<Client>,
    request: AdmissionRequest<StackableScaler>,
) -> AdmissionResponse {
    // On UPDATE: reject spec.replicas changes during active scaling
    if request.operation == Operation::Update {
        if let Some(old) = &request.old_object {
            if scaler.spec.replicas != old.spec.replicas {
                let live = api.get(scaler_name).await?;
                if let Some(stage) = stage.filter(|s| s.is_scaling_in_progress()) {
                    return deny("Cannot update spec.replicas while scaling is in progress");
                }
            }
        }
    }
    // Allow — no patches, no mutations
    AdmissionResponse::from(&request)
}
```
