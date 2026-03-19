# ReplicasConfig Interface Rewrite — Design Spec

## Overview

Replace the current `replicas: Option<u16>` field on role groups with a `ReplicasConfig` enum that explicitly models four scaling modes. The operator takes full ownership of StackableScaler and HPA lifecycle via `ClusterResources`, and the admission webhook is demoted from mutating to validating.

## Goals

- Make the user-facing API self-documenting: each scaling mode is an explicit enum variant, not an implicit convention (`replicas: 0`).
- Let the operator own the full lifecycle of StackableScaler and HPA objects through `ClusterResources.add()`, getting orphan cleanup and label validation. The caller (`build_scaler()`, `build_hpa()`) must set labels and owner refs on the objects before passing them to `add()`.
- Simplify watch registration by switching from `.watches()` with a manual mapper to `.owns()`.
- Remove the mutation responsibility from the admission webhook (label injection is no longer needed).

---

## Section 1: ReplicasConfig Enum

Defined in `operator-rs`, used by all product operators in their role group config.

```rust
/// How replicas are determined for a role group.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ReplicasConfig {
    /// Static replica count. The operator sets StatefulSet replicas directly.
    Fixed(u16),

    /// User provides a full HPA spec. The operator creates a StackableScaler
    /// and an HPA targeting it, injecting the correct `scaleTargetRef`.
    Hpa(HpaConfig),

    /// Operator generates a product-specific HPA. The user only provides
    /// min/max bounds.
    Auto(AutoConfig),

    /// The user manages their own scaler (HPA, KEDA, etc.). The operator
    /// creates a StackableScaler but no HPA.
    ExternallyScaled,
}

/// Full HPA spec passthrough — the operator injects `scaleTargetRef`.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaConfig {
    pub spec: HorizontalPodAutoscalerSpec,
}

/// Operator-generated HPA — user only sets replica bounds.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoConfig {
    pub min_replicas: u16,
    pub max_replicas: u16,
}
```

### Field name and defaulting

The role group field stays `replicas: Option<ReplicasConfig>`:

- `replicas: 3` deserializes as `Fixed(3)` via a custom `Deserialize` impl (not `#[serde(untagged)]` — untagged enums produce unhelpful error messages on failure and generate inaccurate JSON schemas). The custom impl tries numeric first, then tagged enum variants.
- `replicas: null` or omitted defaults to `Fixed(1)`, preserving current behavior.
- `Fixed(0)` is rejected by validation.
- `Auto` with `min_replicas: 0` is rejected. `min_replicas >= 1` and `max_replicas >= min_replicas` are enforced.

---

## Section 2: Slimmed-Down StackableScaler CRD

The StackableScaler spec is reduced to a single field. Identity is conveyed through owner references and standard Stackable labels, both set by the caller (`build_scaler()`) before passing the object to `ClusterResources.add()`.

### Before

```rust
pub struct StackableScalerSpec {
    pub replicas: i32,
    pub cluster_ref: UnknownClusterRef,
    pub role: String,
    pub role_group: String,
}
```

### After

```rust
pub struct StackableScalerSpec {
    pub replicas: i32,
}
```

### Identity

- **Owner reference** → points to the parent cluster CR (NifiCluster, TrinoCluster, etc.). Set by `build_scaler()` via `ObjectMetaBuilder`. `ClusterResources.add()` validates but does not inject these.
- **Labels** → standard Stackable labels including role and role-group. Set by `build_scaler()` via `ObjectMetaBuilder`. `ClusterResources.add()` validates that required labels (`app.kubernetes.io/instance`, `app.kubernetes.io/managed-by`, `app.kubernetes.io/name`) are present but does not inject them.

### Status

Unchanged — `replicas`, `desiredReplicas`, `selector`, `currentState` remain as-is.

### Scaler discovery

Changes from label-selector query + `clusterRef`/`role`/`roleGroup` matching to iterating owned StackableScalers (from the `.owns()` reflector store) and matching by the `app.kubernetes.io/role-group` label.

### `resolve_replicas()`

No longer needed as a standalone function. The `ReplicasConfig` match arm already knows the replica source: `Fixed` uses the config value directly; the other three read from scaler status. The function is kept available during the transition until all product operators are updated to the new flow.

### `reconcile_scaler()` parameter changes

With `role_group` and `cluster_ref` removed from the scaler spec, `reconcile_scaler()` can no longer read them from the scaler to populate `ScalingContext`. These become explicit parameters:

```rust
pub async fn reconcile_scaler(
    scaler: &StackableScaler,
    hooks: &impl ScalingHooks,
    client: &Client,
    role_group_name: &str,        // was scaler.spec.role_group
    sts_converged: bool,
    selector_string: &str,
) -> Result<...>
```

`role_group_name` becomes an explicit parameter since it is used in `ScalingContext` (passed to hooks) and was previously read from `scaler.spec.role_group`. The product operator reconciler already has it in scope. `cluster_ref` is not needed — no hook currently uses the cluster name, and if one needs it in the future, the hook struct itself can carry it.

### `ClusterResource` trait implementation

`StackableScaler` and `HorizontalPodAutoscaler` do not currently implement the `ClusterResource` trait required by `ClusterResources.add()`. Implementation work needed:

- **`impl ClusterResource for StackableScaler`** — requires `DeepMerge`, `GetApi`, etc. `DeepMerge` needs a manual impl or derive since it is not automatic for `CustomResource`-derived types.
- **`impl ClusterResource for HorizontalPodAutoscaler`** — same trait bounds.
- **`delete_orphaned_resources()`** — both types must be added to the orphan cleanup `tokio::try_join!` block in `cluster_resources.rs`.

### CRD version

Stays `v1alpha1`. The CRD was never released, so there are no existing objects to migrate.

---

## Section 3: Operator Reconcile Loop

Per role group, the reconciler handles each `ReplicasConfig` variant. The state machine is driven **before** any config changes to let in-progress scaling complete.

### Flow

```
1. Find existing scaler (via owner ref)
2. If scaler exists and scaling in progress:
   → keep StatefulSet in sync with scaler status
   → drive state machine
   → requeue (don't touch HPA or scaler config)
3. If scaler is Idle or doesn't exist:
   → apply full desired state based on ReplicasConfig variant
```

### Per-variant behavior (step 3)

**`Fixed(n)`**
- No scaler, no HPA.
- StatefulSet gets `replicas: n` directly.
- Orphan cleanup removes any previously existing scaler/HPA.

**`Hpa(hpa_config)`**
- Create/update StackableScaler via `cluster_resources.add()`.
- Create/update HPA from user-provided spec, with `scaleTargetRef` overwritten to point at the scaler. Any `scaleTargetRef` in the user-provided `HpaConfig.spec` is silently replaced.
- StatefulSet replicas from scaler status.
- Drive state machine.

**`Auto(auto_config)`**
- Same as `Hpa`, but the HPA spec is generated by a product-specific template function.
- Each product operator defines what metrics and behavior make sense (e.g., CPU for Trino, queue depth for NiFi).

**`ExternallyScaled`**
- Create/update StackableScaler via `cluster_resources.add()`.
- No operator-managed HPA — the user's external scaler (HPA, KEDA, etc.) targets the StackableScaler.
- StatefulSet replicas from scaler status.
- Drive state machine.

### Key properties

- **`ClusterResources` handles orphan cleanup** — `delete_orphaned_resources()` removes resources that were previously added but are no longer present in the current reconcile. Switching from `Hpa` to `Fixed` automatically cleans up the scaler and HPA on the next reconcile. Labels and owner refs are set by the caller (`build_scaler()`, `build_hpa()`), not by `add()` itself.
- **State machine first** — in-progress scaling completes before any config changes land. This prevents interference with NiFi node offloading, Trino worker draining, etc.
- **Scaler initial replicas** — on first creation, `spec.replicas` is set to the current StatefulSet replica count to avoid an immediate scale event.
- **Status initialization** — a freshly created scaler has no status yet (`status: None`). The reconciler must handle this: if `scaler.status` is `None`, immediately patch the status with `replicas` set to the current StatefulSet replica count and `stage: Idle` before proceeding. This prevents reading a default `replicas: 0` and triggering an unintended scale-to-zero.
- **`build_scaler()` in operator-rs** — shared helper that takes role/role-group/labels and produces a minimal StackableScaler with labels and owner ref already set.
- **`product_hpa_template()` per product** — each operator defines the HPA spec for the `Auto` variant.
- **`ClusterStopped` interaction** — when `ClusterResourceApplyStrategy::ClusterStopped` is active, the operator forces StatefulSet replicas to 0. The scaler state machine must not interpret this as a scale-down requiring pre-scale hooks (e.g., NiFi offload). The reconciler still adds the StackableScaler to `cluster_resources` (to prevent orphan deletion on restart) but skips `reconcile_scaler()`. The scaler's `maybe_mutate` is a no-op — stopping is handled by the StatefulSet's `maybe_mutate` setting replicas to 0. The HPA is also still added to `cluster_resources` (not removed) to avoid recreation churn on restart; since the scaler state machine is not driven, HPA writes to `spec.replicas` are harmless.

---

## Section 4: Watch Registration

### Before

Manual `.watches()` with a mapper and label-selector filter:

```rust
.watches(
    Api::<StackableScaler>::all(client.clone()),
    MapperConfig::label_selector("stackable.tech/cluster-kind=NifiCluster"),
)
```

The `cluster-kind` label was injected by the admission webhook to route scaler events to the correct product operator.

### After

Standard `.owns()`:

```rust
Controller::new(Api::<NifiCluster>::all(client.clone()), ...)
    .owns(Api::<StackableScaler>::all(client.clone()), ...)
    .owns(Api::<StatefulSet>::all(client.clone()), ...)
```

### What changes

- **Owner reference is the link** — `build_scaler()` sets owner refs on the object metadata. `.owns()` filters by owner ref automatically.
- **No mapper, no label-based discovery** — the standard kube-rs ownership pattern replaces custom routing logic.
- **`cluster-kind` label no longer needed** — it was only used for the `.watches()` mapper. The admission webhook no longer needs to inject it.

---

## Section 5: Admission Webhook

### Before

Mutating admission webhook in commons-operator with two responsibilities:
1. Validate — reject `spec.replicas` changes while scaling is in progress.
2. Mutate — inject `stackable.tech/cluster-kind` label from `spec.clusterRef.kind`.

### After

Validating admission controller with a single responsibility:

```rust
async fn validate_scaler(
    request: AdmissionRequest<StackableScaler>,
) -> AdmissionResponse {
    if request.operation == Operation::Update {
        if new.spec.replicas != old.spec.replicas {
            let live = api.get(scaler_name).await?;
            if live.is_scaling_in_progress() {
                return deny("Cannot update spec.replicas while scaling is in progress");
            }
        }
    }
    allow()
}
```

### What changes

- **Demoted from `MutatingWebhookConfiguration` to `ValidatingWebhookConfiguration`** — no more JSON patches.
- **Label injection removed** — the only mutation is gone.
- **UPDATE only** — the webhook is registered for `UPDATE` operations only. `CREATE` validation is not needed: the operator creates scalers itself (so spec is always well-formed), and for `ExternallyScaled` the scaler is also operator-created.
- **Live-object fetch remains** — Kubernetes strips `.status` from `oldObject` in admission requests for CRDs with a status subresource, so the webhook still GETs the current stage.
- **Simpler registration** — validating webhooks have no ordering concerns with other mutating webhooks.

---

## Section 6: Migration & Backward Compatibility

### `replicas` field

- **Numeric values stay valid** — `replicas: 3` deserializes as `Fixed(3)`. Existing manifests don't break.
- **`replicas: 0` convention removed** — users should migrate to `ExternallyScaled`, `Hpa`, or `Auto`. `Fixed(0)` is rejected by validation.
- **`replicas: null` / omitted** — defaults to `Fixed(1)`, same as current behavior.

### StackableScaler CRD

Spec fields removed (`clusterRef`, `role`, `roleGroup`), same `v1alpha1` version. Never released — no existing objects to worry about.

### Admission webhook

- Swap `MutatingWebhookConfiguration` for `ValidatingWebhookConfiguration` in the commons-operator Helm chart.
- No dual-running period — the old mutating webhook and the label it injected are removed in the same release.

### Rollout order

1. **operator-rs** — new `ReplicasConfig` type, updated scaler CRD spec, shared `build_scaler()` helper.
2. **commons-operator** — swap webhook from mutating to validating, deploy updated CRD.
3. **Product operators** (nifi, trino) — new reconcile flow with `ClusterResources`-managed scalers, `.owns()` watches.
