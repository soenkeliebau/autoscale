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
                                                       │ owner reference
                                              ┌────────┘  links to cluster
                                              ▼
 Admission Webhook ──validates──> StackableScaler
 (commons-operator)                    │
                                       │ .owns()
                                       ▼
                              Product Operator ──────owns──────> StatefulSet
                               (nifi / trino)                   replicas: <from scaler>
                                       ▲
                                       │ reconcile
                              Product Cluster CRD
                              (roleGroup.replicas: { hpa: ... })
```

**Key design:** Scaling is configured via the `ReplicasConfig` enum in the role group `replicas` field. The operator creates and manages StackableScaler and HPA as implementation details. Users only interact with the StackableScaler directly when using `ExternallyScaled`.

---

# CRD Relations (draw.io)

<img src="/images/crd-relations.drawio.png" class="mx-auto h-96" />

**Key design:** Scaling configuration lives entirely in the role group `replicas` field. The HPA writes `spec.replicas` on the StackableScaler via the `/scale` subresource.

---

# ReplicasConfig Enum

<div class="grid grid-cols-2 gap-4">

<div>

### User-Facing Config

```yaml
roleGroups:
  # Static — direct replica count
  static-group:
    replicas: 3             # Fixed(3)

  # HPA — user-provided HPA spec
  scaled-group:
    replicas:
      hpa:
        spec:
          maxReplicas: 10
          metrics: [...]

  # External — user manages HPA/KEDA
  external-group:
    replicas: "externallyScaled"

  # Omitted — defaults to Fixed(1)
  default-group: {}
```

</div>
<div>

### Rust Enum

```rust
pub enum ReplicasConfig {
    Fixed(u16),
    Hpa(HpaConfig),
    Auto(AutoConfig),
    ExternallyScaled,
}

impl Default for ReplicasConfig {
    fn default() -> Self {
        Self::Fixed(1)
    }
}
```

- Bare integers &rarr; `Fixed(n)`
- `"externallyScaled"` &rarr; `ExternallyScaled`
- Tagged objects &rarr; `Hpa`/`Auto`
- `Fixed(0)` rejected by validation
- `Auto` not yet implemented

</div>
</div>

---

# StackableScaler CRD

<div class="grid grid-cols-2 gap-4">

<div>

### Spec & Status

```yaml
apiVersion: autoscaling.stackable.tech/v1alpha1
kind: StackableScaler
metadata:
  name: nifi-nodes-default-scaler
  labels:
    app.kubernetes.io/name: nifi
    app.kubernetes.io/instance: my-nifi
    app.kubernetes.io/component: nodes
    app.kubernetes.io/role-group: default
    app.kubernetes.io/managed-by: nifi-operator
  ownerReferences:
    - kind: NifiCluster
      name: my-nifi
spec:
  replicas: 5          # written by HPA
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

### Identity

- **Owner reference** &rarr; parent cluster CR
- **Labels** &rarr; standard Stackable labels
- Set by `build_scaler()`, validated by `ClusterResources.add()`

### Recovery

```bash
# Reset from Failed to Idle
kubectl annotate stackablescaler \
  nifi-nodes-default-scaler \
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

The operator matches on `ReplicasConfig` to decide what resources to create. For `Hpa` and `ExternallyScaled` variants, **`build_scaler()`** creates the StackableScaler and **`reconcile_scaler()`** drives the state machine with product-specific hooks.

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

# Code: CRD & ReplicasConfig

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
}
```

</div>
<div>

### ReplicasConfig

```rust
pub enum ReplicasConfig {
    Fixed(u16),
    Hpa(HpaConfig),
    Auto(AutoConfig),
    ExternallyScaled,
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
let replicas_config = role_group.and_then(|rg| rg.replicas.clone()).unwrap_or_default();

let (replicas, scaler_to_reconcile) = match &replicas_config {
    ReplicasConfig::Fixed(n) => (Some(i32::from(*n)), None),
    ReplicasConfig::Hpa(hpa_config) => {
        let scaler = build_scaler(&cluster.name_any(), APP_NAME, namespace,
            &rolegroup.role, &rolegroup.role_group, 1, &owner_ref, OPERATOR_NAME)?;
        let applied = cluster_resources.add(client, scaler).await?;
        if applied.status.is_none() {
            initialize_scaler_status(client, &applied, 1, &selector_string).await?;
        }
        let target_ref = scale_target_ref(&scaler_name, "autoscaling.stackable.tech", "v1alpha1");
        let hpa = build_hpa_from_user_spec(&hpa_config.spec, &target_ref, ...)?;
        cluster_resources.add(client, hpa).await?;
        (applied.status.as_ref().map(|st| st.replicas), Some(applied))
    }
    ReplicasConfig::ExternallyScaled => { /* same as Hpa but no HPA created */ }
    ReplicasConfig::Auto(_) => return Err(Error::AutoScalingNotYetImplemented { .. }),
};

let applied_sts = cluster_resources.add(client, build_statefulset(replicas, ...)?).await?;
if let Some(ref s) = scaler_to_reconcile {
    reconcile_scaler(s, &ProductScalingHooks { ... }, client,
        statefulset_stable, &selector_string, &rolegroup.role_group).await?;
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
// Uses MutatingWebhook framework (no ValidatingWebhook in stackable-webhook yet),
// but never returns patches — functionally a validating webhook.

async fn scaler_admission_handler(
    client: Arc<Client>,
    request: AdmissionRequest<StackableScaler>,
) -> AdmissionResponse {
    // Validate: reject spec.replicas changes during active scaling
    if request.operation == Operation::Update {
        if let Some(old) = &request.old_object {
            if scaler.spec.replicas != old.spec.replicas {
                let live = api.get(scaler_name).await?;
                if let Some(stage) = stage.filter(|s| s.is_scaling_in_progress()) {
                    return deny(
                        "Cannot update spec.replicas while scaling is in progress"
                    );
                }
            }
        }
    }
    // Allow — no patches, no mutations
    AdmissionResponse::from(&request)
}
```

<div class="mt-4 text-sm">

**Why fetch the live object?** Kubernetes strips `.status` from `oldObject` in admission requests for CRDs with a status subresource. The webhook needs to GET the current stage to decide.

</div>
