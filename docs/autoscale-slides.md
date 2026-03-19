---
theme: default
title: StackableScaler — Auto-Scaling
info: HPA-driven auto-scaling for Stackable product operators
drawings:
  persist: false
transition: slide-left
---

# StackableScaler

HPA-driven auto-scaling for Stackable product operators

<div class="abs-br m-6 text-sm opacity-50">
operator-rs &middot; commons-operator &middot; nifi-operator &middot; trino-operator
</div>

<!--
Run with: npx @slidev/cli@0.48.9 docs/autoscale-slides.md
Requires Node >= 18. Versions above 0.48 need Node >= 20.
-->

---

# CRD Relations

```
                          writes spec.replicas
 HorizontalPodAutoscaler ───────────────────────> StackableScaler
   (metrics: CPU, mem)   <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  (autoscaling.stackable.tech)
                          reads status.replicas        │
                                                       │ label selector
                                              ┌────────┘  matches cluster
                                              ▼
 Admission Webhook ──validates──> StackableScaler
 (commons-operator)                    │
                                       │ watched by
                                       ▼
                              Product Operator ──────owns──────> StatefulSet
                               (nifi / trino)                   replicas: <from scaler>
                                       ▲
                                       │ reconcile
                              Product Cluster CRD
                              (roleGroup.replicas: 0)
```

**Key convention:** `roleGroup.replicas: null` signals "externally managed". The operator discovers the matching `StackableScaler` and reads the effective replica count from its status. The HPA writes `spec.replicas` via the `/scale` subresource.

Documentation ref: https://docs.stackable.tech/home/stable/concepts/operations/
---

# CRD Relations (draw.io)

<img src="/images/crd-relations.drawio.png" class="mx-auto h-96" />

**Key convention:** `roleGroup.replicas: null` signals "externally managed". The HPA writes `spec.replicas` via the `/scale` subresource.

---

# StackableScaler CRD

<div class="grid grid-cols-2 gap-4">

<div>

### Spec & Status

```yaml
apiVersion: autoscaling.stackable.tech/v1alpha1
kind: StackableScaler
metadata:
  name: nifi-nodes-scaler
spec:
  replicas: 5          # written by HPA
  clusterRef:
    kind: NifiCluster
    name: my-nifi
  role: nodes
  roleGroup: default
status:
  replicas: 3          # current count
  desiredReplicas: 5
  selector: "app=nifi,..."
  currentState:
    stage: PreScaling
    lastTransitionTime: "2026-03-19T..."
```

</div>
<div>

### Subresources

**/scale** &mdash; HPA reads/writes replicas

```json
{
  "specReplicasPath":   ".spec.replicas",
  "statusReplicasPath": ".status.replicas",
  "labelSelectorPath":  ".status.selector"
}
```

**/status** &mdash; operator patches state

<br>

### Recovery

```bash
# Reset from Failed to Idle
kubectl annotate stackablescaler \
  nifi-nodes-scaler \
  autoscaling.stackable.tech/retry=true
```

</div>
</div>

---

# State Machine

```
    ┌──────────────────────────────────────────────────────┐
    │                                                      │
    ▼           replicas       pre_scale()    STS          │  post_scale()
  Idle ──────> PreScaling ──────────────> Scaling ────> PostScaling
               changed         = Done      converged       = Done
                  │                │                  │
                  │ = Err          │ timeout          │ = Err
                  ▼                ▼                  ▼
                              Failed
                                │
                                │ retry annotation
                                ▼
                              Idle
```

InProgress hook results trigger a requeue (10s for hooks, 5s for STS convergence).

<v-click>

<div class="mt-2 p-3 bg-amber-50 rounded text-sm dark:bg-amber-900">

The **admission webhook** rejects `spec.replicas` changes from the HPA while stage is PreScaling, Scaling, or PostScaling &mdash; preventing mid-operation replica target drift.

</div>

</v-click>

---

# State Machine

<img src="/images/state-machine.png" class="mx-auto h-96" />

InProgress hook results trigger a requeue (10s for hooks, 5s for STS convergence). The **admission webhook** rejects `spec.replicas` changes while scaling is in progress.

---

# Reconcile Loop &mdash; Per Role Group

<img src="/images/flowchart.png" class="mx-auto h-80" />

The two highlighted steps are provided by `operator-rs`: **resolve_replicas** reads `scaler.status.replicas` and **reconcile_scaler** drives the state machine and calls product-specific hooks.

---

# Product-Specific Hooks

<div class="grid grid-cols-2 gap-6 mt-4">

<div>

### NiFi &mdash; Node Decommission

Scale-down sequence differs by version:

**NiFi 1.x**

```
CONNECTED -> OFFLOADING -> OFFLOADED
  -> DISCONNECTING -> DISCONNECTED -> DELETE
```

**NiFi 2.x**

```
CONNECTED -> DISCONNECTING -> DISCONNECTED
  -> OFFLOADING -> OFFLOADED -> DELETE
```

Connects to NiFi REST API via pod-0, identifies target nodes by FQDN, drives each through the full decommission lifecycle.

</div>
<div>

### Trino &mdash; Graceful Shutdown

Single-phase drain via worker REST API:

```
ACTIVE -> SHUTTING_DOWN -> INACTIVE
```

For each removed ordinal:
- **ACTIVE** &rarr; PUT `"SHUTTING_DOWN"`
- **SHUTTING_DOWN** &rarr; wait (requeue)
- **INACTIVE** &rarr; ready for termination

Running queries complete before pod deletion.

</div>
</div>

<div class="mt-4">

| Hook | NiFi | Trino |
|------|------|-------|
| `pre_scale` (down) | Offload + disconnect + delete via NiFi API | PUT SHUTTING_DOWN, poll until INACTIVE |
| `post_scale` | default (Done) | default (Done) |

</div>

---

# Code: CRD & Replica Resolution

<div class="grid grid-cols-2 gap-4">

<div>

### StackableScaler CRD

```rust
#[kube(
    group = "autoscaling.stackable.tech",
    kind = "StackableScaler",
    status = "StackableScalerStatus",
    // /scale subresource -- HPA target
    scale = r#"{
      "specReplicasPath":".spec.replicas",
      "statusReplicasPath":".status.replicas",
      "labelSelectorPath":".status.selector"
    }"#
)]
pub struct StackableScalerSpec {
    pub replicas: i32,
    pub cluster_ref: UnknownClusterRef,
    pub role: String,
    pub role_group: String,
}
```

</div>
<div>

### Replica Resolution

```rust
pub fn resolve_replicas(
    role_group_replicas: Option<i32>,
    scaler: Option<&StackableScaler>,
) -> Option<i32> {
    match (role_group_replicas, scaler) {
        // replicas: 0 + scaler = use scaler
        (Some(0), Some(s)) =>
            s.status.as_ref().map(|st| st.replicas),
        // anything else = pass through
        (replicas, _) => replicas,
    }
}
```

### Stage Enum

```rust
pub enum ScalerStage {
    Idle,
    PreScaling,
    Scaling,
    PostScaling,
    Failed { failed_at: FailedStage,
             reason: String },
}
```

</div>
</div>

---

# Code: ScalingHooks Trait

```rust 
pub trait ScalingHooks {
    type Error: std::error::Error + Send + Sync + 'static;

    /// PreScaling -- e.g. drain/offload nodes before scale-down.
    /// Return Done to advance to Scaling, InProgress to requeue.
    fn pre_scale(&self, ctx: &ScalingContext<'_>)
        -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send
    { async { Ok(HookOutcome::Done) } }

    /// PostScaling -- e.g. rebalance after scale-up.
    fn post_scale(&self, ctx: &ScalingContext<'_>)
        -> impl Future<Output = Result<HookOutcome, Self::Error>> + Send
    { async { Ok(HookOutcome::Done) } }

    /// Called on transition to Failed. Best-effort cleanup.
    fn on_failure(&self, ctx: &ScalingContext<'_>, failed_stage: &FailedStage)
        -> impl Future<Output = Result<(), Self::Error>> + Send
    { async { Ok(()) } }
}
```

---

# Code: Operator Integration

```rust 
// 1. Discover scaler for role groups with replicas: 0
let scaler: Option<StackableScaler> = if rg_replicas == Some(0) {
    client
        .list_with_label_selector::<StackableScaler>(namespace, &selector)
        .await?
        .into_iter()
        .find(|s| s.spec.cluster_ref.name == cluster.name_any()
                && s.spec.role == rolegroup.role
                && s.spec.role_group == rolegroup.role_group)
} else { None };

// 2. Resolve effective replica count
let replicas = resolve_replicas(rg_replicas.map(i32::from), scaler.as_ref());

// 3. Build & apply StatefulSet with resolved replicas
let rg_statefulset = build_statefulset(replicas, ...)?;
let applied_sts = cluster_resources.add(client, rg_statefulset).await?;

// 4. Drive scaler state machine
if let Some(ref s) = scaler {
    let result = reconcile_scaler(
        s,
        &ProductScalingHooks { ... },  // NifiScalingHooks or TrinoScalingHooks
        client, statefulset_stable, &selector_string,
    ).await?;
}
```

<div class="text-sm opacity-70 mt-2">

Same pattern in both nifi-operator and trino-operator &mdash; only the hooks struct differs.

</div>

---

# Code: Trino Hook

```rust 
impl ScalingHooks for TrinoScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        if !ctx.is_scale_down() { return Ok(HookOutcome::Done); }
        let mut any_in_progress = false;

        for ordinal in ctx.removed_ordinals() {
            let client = TrinoWorkerClient::new(&self.worker_base_url(ordinal))?;
            match client.get_state().await? {
                TrinoWorkerState::Active => {
                    client.initiate_shutdown().await?;   // PUT /v1/info/state
                    any_in_progress = true;
                }
                TrinoWorkerState::ShuttingDown => { any_in_progress = true; }
                TrinoWorkerState::Inactive     => { /* ready for termination */ }
            }
        }
        Ok(if any_in_progress { HookOutcome::InProgress } else { HookOutcome::Done })
    }
}
```

<v-click>

<div class="mt-2 p-3 bg-green-50 rounded text-sm dark:bg-green-900">

**InProgress** triggers a 10s requeue. The reconciler re-enters PreScaling and re-polls each worker until all report INACTIVE, then advances to Scaling.

</div>

</v-click>

---

# Code: NiFi Hook (simplified)

```rust 
impl ScalingHooks for NifiScalingHooks {
    type Error = Error;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, Error> {
        if !ctx.is_scale_down() { return Ok(HookOutcome::Done); }

        let creds = resolve_credentials(ctx.client, &self.secret, ctx.namespace).await?;
        let api = NifiApiClient::connect(self.api_base_url(0), &creds.user, &creds.pass).await?;
        let nodes = api.get_cluster_nodes().await?;

        // Version-aware: 1.x offloads first, 2.x disconnects first
        for target in nodes_being_removed(&nodes, ctx) {
            match target.status {
                Connected     => api.start_offload_or_disconnect(&target).await?,
                Offloading    => { /* still draining -- will requeue */ }
                Offloaded     => api.disconnect(&target).await?,
                Disconnecting => { /* still disconnecting -- will requeue */ }
                Disconnected  => api.delete_node(&target).await?,
            }
        }
        Ok(if any_in_progress { HookOutcome::InProgress } else { HookOutcome::Done })
    }
}
```

---

# Code: Admission Webhook

```rust 
async fn scaler_admission_handler(
    client: Arc<Client>,
    request: AdmissionRequest<StackableScaler>,
) -> AdmissionResponse {
    // Validate: reject spec.replicas changes during active scaling
    if request.operation == Operation::Update {
        if new.spec.replicas != old.spec.replicas {
            let live = api.get(scaler_name).await?;
            if live.status.current_state.stage.is_scaling_in_progress() {
                return deny(
                    "Cannot update spec.replicas while scaling is in progress"
                );
            }
        }
    }
    // Mutate: inject cluster-kind label from spec.clusterRef.kind
    patch.add("/metadata/labels/stackable.tech~1cluster-kind", cluster_kind);
    response.with_patch(patch)
}
```

<div class="mt-4 text-sm">

**Why fetch the live object?** Kubernetes strips `.status` from `oldObject` in admission requests for CRDs with a status subresource. The webhook needs to GET the current stage to decide.

</div>
