# ReplicasConfig Interface Rewrite — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `replicas: Option<u16>` with `ReplicasConfig` enum, make operators own StackableScaler/HPA lifecycle via `ClusterResources`, switch to `.owns()` watches, and demote the admission webhook to validating-only.

**Architecture:** Changes flow bottom-up: operator-rs (shared types + traits), then commons-operator (webhook), then product operators (nifi, trino). The `ReplicasConfig` enum with `Fixed`/`Hpa`/`Auto`/`ExternallyScaled` variants replaces the `replicas: 0` convention. Operators create StackableScaler and HPA via `ClusterResources.add()` with caller-set labels and owner refs. The `.owns()` kube-rs pattern replaces manual `.watches()` + label-based mappers.

**Tech Stack:** Rust, kube-rs, serde, schemars (JsonSchema), k8s-openapi, snafu, operator-rs framework

**Spec:** `docs/superpowers/specs/2026-03-19-replicas-config-design.md`

**Compile breakage note:** Chunks 1-4 form an atomic change set. After Chunk 1, downstream consumers (commons-operator, product operators) will not compile until their respective chunks are applied. This is expected — the repos are submodules in a single workspace and are released together. Intermediate breakage between chunks is acceptable as long as the final state compiles.

**Scope notes:**
- `product_hpa_template()` for the `Auto` variant: each product operator will return an error / log a warning for `Auto` in the initial pass. The per-product HPA template (choosing metrics, stabilization windows, etc.) is a follow-up task after the interface rewrite lands.
- Integration tests for the full scaling flow are out of scope for this plan. Unit tests cover the new types and helpers; end-to-end validation is deferred.

---

## Chunk 1: operator-rs — ReplicasConfig Type & Scaler CRD Changes

All changes in this chunk are in the `operator-rs` submodule at:
`operator-rs/crates/stackable-operator/src/`

### Task 1: Define ReplicasConfig enum

**Files:**
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/replicas_config.rs`
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (add `mod replicas_config; pub use replicas_config::*;`)

- [ ] **Step 1: Write tests for ReplicasConfig deserialization**

In the new file, add a `#[cfg(test)] mod tests` block:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_fixed_from_integer() {
        let config: ReplicasConfig = serde_json::from_str("3").unwrap();
        assert_eq!(config, ReplicasConfig::Fixed(3));
    }

    #[test]
    fn deserialize_fixed_from_object() {
        let config: ReplicasConfig =
            serde_json::from_str(r#"{"fixed": 5}"#).unwrap();
        assert_eq!(config, ReplicasConfig::Fixed(5));
    }

    #[test]
    fn deserialize_externally_scaled() {
        let config: ReplicasConfig =
            serde_json::from_str(r#""externallyScaled""#).unwrap();
        assert_eq!(config, ReplicasConfig::ExternallyScaled);
    }

    #[test]
    fn deserialize_hpa() {
        let json = r#"{"hpa": {"spec": {"maxReplicas": 10}}}"#;
        let config: ReplicasConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, ReplicasConfig::Hpa(_)));
    }

    #[test]
    fn deserialize_auto() {
        let json = r#"{"auto": {"minReplicas": 2, "maxReplicas": 10}}"#;
        let config: ReplicasConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, ReplicasConfig::Auto(AutoConfig { min_replicas: 2, max_replicas: 10 })));
    }

    #[test]
    fn fixed_zero_is_invalid() {
        let config = ReplicasConfig::Fixed(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn auto_min_zero_is_invalid() {
        let config = ReplicasConfig::Auto(AutoConfig {
            min_replicas: 0,
            max_replicas: 5,
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn auto_max_less_than_min_is_invalid() {
        let config = ReplicasConfig::Auto(AutoConfig {
            min_replicas: 5,
            max_replicas: 2,
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn option_none_defaults_to_fixed_1() {
        let json = "null";
        let config: Option<ReplicasConfig> = serde_json::from_str(json).unwrap();
        let resolved = config.unwrap_or_default();
        assert_eq!(resolved, ReplicasConfig::Fixed(1));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd operator-rs && cargo test --lib -p stackable-operator -- replicas_config::tests`
Expected: compilation error — types don't exist yet.

- [ ] **Step 3: Implement ReplicasConfig**

In `replicas_config.rs`:

```rust
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscalerSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::Snafu;

/// How replicas are determined for a role group.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
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

impl Default for ReplicasConfig {
    fn default() -> Self {
        Self::Fixed(1)
    }
}

/// Full HPA spec passthrough — the operator injects `scaleTargetRef`.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaConfig {
    /// The full HPA spec. The operator overwrites `scaleTargetRef` to point
    /// at the StackableScaler.
    pub spec: HorizontalPodAutoscalerSpec,
}

/// Operator-generated HPA — user only sets replica bounds.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoConfig {
    pub min_replicas: u16,
    pub max_replicas: u16,
}

/// Validation errors for [`ReplicasConfig`].
#[derive(Debug, Snafu)]
pub enum ValidationError {
    #[snafu(display("Fixed(0) is not allowed — use ExternallyScaled, Hpa, or Auto instead"))]
    FixedZero,

    #[snafu(display("Auto min_replicas must be >= 1, got {min}"))]
    AutoMinZero { min: u16 },

    #[snafu(display("Auto max_replicas ({max}) must be >= min_replicas ({min})"))]
    AutoMaxLessThanMin { min: u16, max: u16 },
}

impl ReplicasConfig {
    /// Validate configuration constraints.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError::FixedZero`] if variant is `Fixed(0)`.
    /// Returns [`ValidationError::AutoMinZero`] if `Auto` has `min_replicas == 0`.
    /// Returns [`ValidationError::AutoMaxLessThanMin`] if `max_replicas < min_replicas`.
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Fixed(0) => FixedZeroSnafu.fail(),
            Self::Auto(AutoConfig { min_replicas: 0, .. }) => {
                AutoMinZeroSnafu { min: 0u16 }.fail()
            }
            Self::Auto(AutoConfig {
                min_replicas,
                max_replicas,
            }) if max_replicas < min_replicas => AutoMaxLessThanMinSnafu {
                min: *min_replicas,
                max: *max_replicas,
            }
            .fail(),
            _ => Ok(()),
        }
    }
}

/// Custom deserializer: tries integer first (→ Fixed), then tagged enum variants.
impl<'de> Deserialize<'de> for ReplicasConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de;

        let value = serde_json::Value::deserialize(deserializer)?;
        match &value {
            serde_json::Value::Number(n) => {
                let n = n
                    .as_u64()
                    .ok_or_else(|| de::Error::custom("replicas must be a non-negative integer"))?;
                let n = u16::try_from(n).map_err(|_| {
                    de::Error::custom(format!("replicas value {n} exceeds u16::MAX"))
                })?;
                Ok(Self::Fixed(n))
            }
            serde_json::Value::String(s) if s == "externallyScaled" => {
                Ok(Self::ExternallyScaled)
            }
            serde_json::Value::Object(_) => {
                // Try tagged variants: {"fixed": n}, {"hpa": {...}}, {"auto": {...}}
                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                enum Tagged {
                    Fixed(u16),
                    Hpa(HpaConfig),
                    Auto(AutoConfig),
                    ExternallyScaled,
                }
                let tagged: Tagged = serde_json::from_value(value)
                    .map_err(de::Error::custom)?;
                Ok(match tagged {
                    Tagged::Fixed(n) => Self::Fixed(n),
                    Tagged::Hpa(c) => Self::Hpa(c),
                    Tagged::Auto(c) => Self::Auto(c),
                    Tagged::ExternallyScaled => Self::ExternallyScaled,
                })
            }
            _ => Err(de::Error::custom(
                "replicas must be an integer, a string (\"externallyScaled\"), or an object ({\"hpa\": ...}, {\"auto\": ...})",
            )),
        }
    }
}
```

- [ ] **Step 4: Register module in mod.rs**

In `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs`, add:
```rust
mod replicas_config;
pub use replicas_config::{AutoConfig, HpaConfig, ReplicasConfig, ValidationError as ReplicasValidationError};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd operator-rs && cargo test --lib -p stackable-operator -- replicas_config::tests`
Expected: all tests PASS.

- [ ] **Step 6: Run full checks**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`
Expected: clean build, no warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
cd operator-rs
git add crates/stackable-operator/src/crd/scaler/replicas_config.rs crates/stackable-operator/src/crd/scaler/mod.rs
git commit -m "feat: add ReplicasConfig enum with custom deserializer and validation"
```

---

### Task 2: Slim down StackableScalerSpec

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (lines ~200-210: remove `cluster_ref`, `role`, `role_group` from spec struct)
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/reconciler.rs` (line ~217: `role_group_name` from parameter instead of `scaler.spec.role_group`)
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (tests: update all test helper functions that construct `StackableScalerSpec`)

- [ ] **Step 1: Update the spec struct**

In `mod.rs`, remove `cluster_ref`, `role`, `role_group` from the `StackableScalerSpec` struct. Keep only `replicas: i32`.

Also remove the `UnknownClusterRef` struct if it is only used by the scaler spec (check for other usages first).

- [ ] **Step 2: Update reconcile_scaler() signature**

In `reconciler.rs`, change `reconcile_scaler()` to accept `role_group_name: &str` as an explicit parameter. Replace `scaler.spec.role_group` with the new parameter where `ScalingContext` is built (around line 217).

- [ ] **Step 3: Fix all compilation errors**

Update all test helpers and test functions in `mod.rs` and `reconciler.rs` that construct `StackableScalerSpec` — remove the deleted fields.

- [ ] **Step 4: Run tests**

Run: `cd operator-rs && cargo test --all-features`
Expected: all tests pass (some tests may need updating if they tested `cluster_ref`/`role`/`role_group` directly).

- [ ] **Step 5: Run full checks**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
cd operator-rs
git add -A
git commit -m "refactor: slim StackableScalerSpec to just replicas field

Remove cluster_ref, role, role_group from spec. Identity now via
owner reference and labels. reconcile_scaler() takes role_group_name
as explicit parameter."
```

---

### Task 3: Implement ClusterResource for StackableScaler and HPA

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/cluster_resources.rs` (add `impl ClusterResource`, update `delete_orphaned_resources()`)
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/cluster_resource_impl.rs` (DeepMerge impl for StackableScaler)

- [ ] **Step 1: Implement DeepMerge for StackableScaler**

Create `cluster_resource_impl.rs` in the scaler module. Follow the pattern from `crd/listener/listeners/v1alpha1_impl.rs`:

```rust
use k8s_openapi::DeepMerge;
use super::v1alpha1::StackableScaler;

impl DeepMerge for StackableScaler {
    fn merge_from(&mut self, other: Self) {
        DeepMerge::merge_from(&mut self.metadata, other.metadata);
        DeepMerge::merge_from(&mut self.spec.replicas, other.spec.replicas);
        DeepMerge::merge_from(&mut self.status, other.status);
    }
}
```

Register the module in `mod.rs`: `mod cluster_resource_impl;`

- [ ] **Step 2: Add `impl ClusterResource` for StackableScaler**

In `cluster_resources.rs`, add alongside the other impls (around line 217):

```rust
impl ClusterResource for crate::crd::scaler::v1alpha1::StackableScaler {}
```

No `maybe_mutate` override needed — it's a no-op for scalers under `ClusterStopped` (the reconciler handles this by skipping `reconcile_scaler()`).

- [ ] **Step 3: Add `impl ClusterResource` for HorizontalPodAutoscaler**

`HorizontalPodAutoscaler` from `k8s_openapi` already implements `DeepMerge`, `Serialize`, `Deserialize`, `Clone`, `Debug`, and `Resource`. Check if it implements `GetApi` — if not, a manual impl or a wrapper type may be needed.

In `cluster_resources.rs`:

```rust
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

impl ClusterResource for HorizontalPodAutoscaler {}
```

- [ ] **Step 4: Add both types to orphan cleanup**

In `delete_orphaned_resources()` (around line 672), add to the `tokio::try_join!`:

```rust
self.delete_orphaned_resources_of_kind::<crate::crd::scaler::v1alpha1::StackableScaler>(client),
self.delete_orphaned_resources_of_kind::<HorizontalPodAutoscaler>(client),
```

- [ ] **Step 5: Run full checks**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`
Expected: clean build, all tests pass.

- [ ] **Step 6: Commit**

```bash
cd operator-rs
git add -A
git commit -m "feat: implement ClusterResource for StackableScaler and HPA

Adds DeepMerge impl for StackableScaler and registers both types
in delete_orphaned_resources() for proper orphan cleanup."
```

---

### Task 4: Add build_scaler() helper

**Files:**
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/builder.rs`
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (add `mod builder; pub use builder::*;`)

- [ ] **Step 1: Write tests for build_scaler()**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    #[test]
    fn build_scaler_sets_replicas() {
        let scaler = build_scaler(
            "my-nifi", "nifi", "default", "nodes", "default",
            3, &owner_ref(), "nifi-operator",
        ).unwrap();
        assert_eq!(scaler.spec.replicas, 3);
    }

    #[test]
    fn build_scaler_sets_owner_reference() {
        let scaler = build_scaler(
            "my-nifi", "nifi", "default", "nodes", "default",
            3, &owner_ref(), "nifi-operator",
        ).unwrap();
        let owner_refs = scaler.metadata.owner_references.unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].name, "my-nifi");
    }

    #[test]
    fn build_scaler_sets_required_labels() {
        let scaler = build_scaler(
            "my-nifi", "nifi", "default", "nodes", "default",
            3, &owner_ref(), "nifi-operator",
        ).unwrap();
        let labels = scaler.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("app.kubernetes.io/name"), Some(&"nifi".to_string()));
        assert_eq!(labels.get("app.kubernetes.io/instance"), Some(&"my-nifi".to_string()));
        assert_eq!(labels.get("app.kubernetes.io/managed-by"), Some(&"nifi-operator".to_string()));
        assert_eq!(labels.get("app.kubernetes.io/component"), Some(&"nodes".to_string()));
        assert_eq!(labels.get("app.kubernetes.io/role-group"), Some(&"default".to_string()));
    }

    fn owner_ref() -> OwnerReference {
        OwnerReference {
            api_version: "nifi.stackable.tech/v1alpha1".to_string(),
            kind: "NifiCluster".to_string(),
            name: "my-nifi".to_string(),
            uid: "test-uid".to_string(),
            ..Default::default()
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd operator-rs && cargo test --lib -p stackable-operator -- scaler::builder::tests`
Expected: compilation error.

- [ ] **Step 3: Implement build_scaler()**

```rust
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use snafu::{ResultExt, Snafu};

use crate::builder::meta::ObjectMetaBuilder;
use crate::kvp::{Label, Labels};
use super::v1alpha1::{StackableScaler, StackableScalerSpec};

/// Errors from building a StackableScaler.
#[derive(Debug, Snafu)]
pub enum BuildScalerError {
    #[snafu(display("failed to build scaler metadata"))]
    BuildMeta {
        source: crate::builder::meta::Error,
    },

    #[snafu(display("failed to build scaler labels"))]
    BuildLabels {
        source: crate::kvp::LabelError,
    },
}

/// Build a StackableScaler with proper labels and owner reference.
///
/// # Parameters
///
/// - `cluster_name`: Name of the parent cluster CR (e.g., `"my-nifi"`).
/// - `app_name`: Product name for `app.kubernetes.io/name` (e.g., `"nifi"`, `"trino"`).
/// - `namespace`: Namespace of the cluster.
/// - `role`: Role name for `app.kubernetes.io/component` (e.g., `"nodes"`, `"worker"`).
/// - `role_group`: Role group name for `app.kubernetes.io/role-group`.
/// - `initial_replicas`: Initial value for `spec.replicas`.
/// - `owner_ref`: Owner reference pointing to the parent cluster CR.
/// - `managed_by`: Operator name for `app.kubernetes.io/managed-by`.
///
/// # Errors
///
/// Returns [`BuildScalerError`] if label or metadata construction fails.
pub fn build_scaler(
    cluster_name: &str,
    app_name: &str,
    namespace: &str,
    role: &str,
    role_group: &str,
    initial_replicas: i32,
    owner_ref: &OwnerReference,
    managed_by: &str,
) -> Result<StackableScaler, BuildScalerError> {
    let scaler_name = format!("{cluster_name}-{role}-{role_group}-scaler");

    let metadata = ObjectMetaBuilder::new()
        .name(&scaler_name)
        .namespace(namespace)
        .ownerreference(owner_ref.clone())
        .with_label(
            Label::try_from(("app.kubernetes.io/name", app_name))
                .context(BuildLabelsSnafu)?,
        )
        .with_label(
            Label::try_from(("app.kubernetes.io/instance", cluster_name))
                .context(BuildLabelsSnafu)?,
        )
        .with_label(
            Label::try_from(("app.kubernetes.io/managed-by", managed_by))
                .context(BuildLabelsSnafu)?,
        )
        .with_label(
            Label::try_from(("app.kubernetes.io/component", role))
                .context(BuildLabelsSnafu)?,
        )
        .with_label(
            Label::try_from(("app.kubernetes.io/role-group", role_group))
                .context(BuildLabelsSnafu)?,
        )
        .build();

    Ok(StackableScaler {
        metadata,
        spec: StackableScalerSpec {
            replicas: initial_replicas,
        },
        status: None,
    })
}
```

- [ ] **Step 4: Register module and run tests**

Add `mod builder; pub use builder::{build_scaler, BuildScalerError};` to `mod.rs`.

Run: `cd operator-rs && cargo test --lib -p stackable-operator -- scaler::builder::tests`
Expected: all tests PASS.

- [ ] **Step 5: Run full checks**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 6: Commit**

```bash
cd operator-rs
git add -A
git commit -m "feat: add build_scaler() helper for constructing StackableScaler with labels and owner ref"
```

---

### Task 5: Add build_hpa() and initialize_scaler_status() helpers

**Files:**
- Create: `operator-rs/crates/stackable-operator/src/crd/scaler/hpa_builder.rs`
- Modify: `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` (register module)

- [ ] **Step 1: Write tests for build_hpa_from_user_spec()**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_hpa_overwrites_scale_target_ref() {
        let user_spec = HorizontalPodAutoscalerSpec {
            max_replicas: 10,
            scale_target_ref: CrossVersionObjectReference {
                kind: "ShouldBeOverwritten".to_string(),
                name: "ignored".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let target = scale_target_ref("my-scaler", "autoscaling.stackable.tech", "v1alpha1");
        let hpa = build_hpa_from_user_spec(
            &user_spec, &target, "my-nifi", "nifi", "default",
            "nodes", "default", &owner_ref(), "nifi-operator",
        ).unwrap();

        let spec = hpa.spec.unwrap();
        assert_eq!(spec.scale_target_ref.name, "my-scaler");
        assert_eq!(spec.scale_target_ref.kind, "StackableScaler");
    }

    #[test]
    fn scale_target_ref_points_to_scaler() {
        let target = scale_target_ref("my-scaler", "autoscaling.stackable.tech", "v1alpha1");
        assert_eq!(target.kind, "StackableScaler");
        assert_eq!(target.name, "my-scaler");
        assert_eq!(target.api_version, Some("autoscaling.stackable.tech/v1alpha1".to_string()));
    }
}
```

- [ ] **Step 2: Implement build_hpa_from_user_spec() and scale_target_ref()**

```rust
use k8s_openapi::api::autoscaling::v2::{
    CrossVersionObjectReference, HorizontalPodAutoscaler, HorizontalPodAutoscalerSpec,
};

/// Build a CrossVersionObjectReference targeting a StackableScaler.
pub fn scale_target_ref(scaler_name: &str, group: &str, version: &str) -> CrossVersionObjectReference {
    CrossVersionObjectReference {
        kind: "StackableScaler".to_string(),
        name: scaler_name.to_string(),
        api_version: Some(format!("{group}/{version}")),
    }
}

/// Build an HPA from a user-provided spec, overwriting scaleTargetRef.
///
/// Sets proper labels and owner reference matching the `build_scaler()` pattern.
/// Any `scaleTargetRef` in the user-provided spec is silently replaced.
pub fn build_hpa_from_user_spec(
    user_spec: &HorizontalPodAutoscalerSpec,
    target_ref: &CrossVersionObjectReference,
    cluster_name: &str,
    app_name: &str,
    namespace: &str,
    role: &str,
    role_group: &str,
    owner_ref: &OwnerReference,
    managed_by: &str,
) -> Result<HorizontalPodAutoscaler, BuildScalerError> {
    let hpa_name = format!("{cluster_name}-{role}-{role_group}-hpa");
    let metadata = ObjectMetaBuilder::new()
        .name(&hpa_name)
        .namespace(namespace)
        .ownerreference(owner_ref.clone())
        // Same label set as build_scaler() for ClusterResources validation
        .with_label(Label::try_from(("app.kubernetes.io/name", app_name)).context(BuildLabelsSnafu)?)
        .with_label(Label::try_from(("app.kubernetes.io/instance", cluster_name)).context(BuildLabelsSnafu)?)
        .with_label(Label::try_from(("app.kubernetes.io/managed-by", managed_by)).context(BuildLabelsSnafu)?)
        .with_label(Label::try_from(("app.kubernetes.io/component", role)).context(BuildLabelsSnafu)?)
        .with_label(Label::try_from(("app.kubernetes.io/role-group", role_group)).context(BuildLabelsSnafu)?)
        .build();

    let mut spec = user_spec.clone();
    spec.scale_target_ref = target_ref.clone();

    Ok(HorizontalPodAutoscaler {
        metadata,
        spec: Some(spec),
        status: None,
    })
}
```

- [ ] **Step 3: Add initialize_scaler_status() helper**

Add to the same file or to `reconciler.rs`:

```rust
/// Patch a freshly created StackableScaler's status to prevent scale-to-zero.
///
/// On first creation, the scaler has no status. Reading `status.replicas` would
/// yield 0, causing the StatefulSet to scale down. This function initializes
/// the status with the current StatefulSet replica count and `Idle` stage.
pub async fn initialize_scaler_status(
    client: &Client,
    scaler: &StackableScaler,
    current_replicas: i32,
    selector: &str,
) -> Result<(), Error> {
    let status = StackableScalerStatus {
        replicas: current_replicas,
        desired_replicas: current_replicas,
        selector: selector.to_string(),
        current_state: ScalerState {
            stage: ScalerStage::Idle,
            last_transition_time: Some(Time(chrono::Utc::now())),
        },
        previous_replicas: None,
        conditions: vec![],
    };
    // Patch status subresource
    let name = scaler.name_any();
    let namespace = scaler.namespace().unwrap_or_default();
    let api: Api<StackableScaler> = client.get_api(&namespace);
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::apply("stackable-operator"), &Patch::Merge(&patch))
        .await
        .context(...)?;
    Ok(())
}
```

- [ ] **Step 4: Register module and run checks**

Add `mod hpa_builder; pub use hpa_builder::*;` to `mod.rs`.

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 5: Commit**

```bash
cd operator-rs
git add -A
git commit -m "feat: add build_hpa_from_user_spec(), scale_target_ref(), and initialize_scaler_status() helpers"
```

---

### Task 6: Update RoleGroup to use ReplicasConfig

**Files:**
- Modify: `operator-rs/crates/stackable-operator/src/role_utils.rs` (line ~440: change `replicas` type)

- [ ] **Step 1: Change the replicas field type**

In `role_utils.rs`, change:
```rust
pub replicas: Option<u16>,
```
to:
```rust
pub replicas: Option<ReplicasConfig>,
```

Add the import for `ReplicasConfig`.

- [ ] **Step 2: Fix compilation errors**

Follow the compiler — any code reading `replicas` as `Option<u16>` needs updating. Within operator-rs, this is likely `validate_config()` and any tests that construct `RoleGroup`.

For code that previously did `replicas.unwrap_or(1)` to get a count, it now needs to match on the `ReplicasConfig` variant.

- [ ] **Step 3: Run full checks**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 4: Commit**

```bash
cd operator-rs
git add -A
git commit -m "feat: change RoleGroup.replicas from Option<u16> to Option<ReplicasConfig>"
```

---

## Chunk 2: commons-operator — Webhook Demotion

All changes in the `commons-operator` submodule.

### Task 7: Demote admission webhook from mutating to validating

**Files:**
- Modify: `commons-operator/rust/operator-binary/src/webhooks/scaler_admission.rs`
- Modify: `commons-operator/rust/operator-binary/src/webhooks/mod.rs` (if needed to change webhook type registration)

- [ ] **Step 1: Remove mutation logic from the handler**

In `scaler_admission.rs`:
1. Remove the label injection code (the JSON patch that adds `stackable.tech/cluster-kind`).
2. Remove the `CREATE` operation handling — keep only `UPDATE` validation.
3. Remove the `MutatingWebhook` wrapper — use `ValidatingWebhook` instead.
4. Simplify the response: return `allow()` or `deny()`, no patches.

- [ ] **Step 2: Update webhook configuration**

In the webhook config section:
1. Change operations from `["CREATE", "UPDATE"]` to `["UPDATE"]`.
2. Keep `failure_policy: "Fail"` (intentional — blocks HPA replica changes during active scaling).

- [ ] **Step 3: Update webhook registration in mod.rs**

If the webhook creation function signature changes (e.g., returns `ValidatingWebhook` instead of `MutatingWebhook`), update `mod.rs` accordingly.

- [ ] **Step 4: Update Helm chart webhook configuration**

If the commons-operator Helm chart defines the webhook configuration (MutatingWebhookConfiguration), update it to `ValidatingWebhookConfiguration`. Check `commons-operator/deploy/helm/` or equivalent for the webhook manifest template.

Key changes in the Helm template:
- `kind: MutatingWebhookConfiguration` → `kind: ValidatingWebhookConfiguration`
- Remove `reinvocationPolicy` (only applies to mutating webhooks)
- Narrow `operations` from `["CREATE", "UPDATE"]` to `["UPDATE"]`

- [ ] **Step 5: Fix tests**

Update any tests that exercise the mutation (label injection) path. Remove those tests. Keep tests for the validation logic (reject replicas changes during scaling).

- [ ] **Step 6: Run full checks**

Run: `cd commons-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 7: Commit**

```bash
cd commons-operator
git add -A
git commit -m "refactor: demote scaler webhook from mutating to validating

Remove cluster-kind label injection (no longer needed with .owns()).
Narrow to UPDATE operations only. Switch to ValidatingWebhookConfiguration.
Update Helm chart webhook manifest."
```

---

## Chunk 3: nifi-operator — New Reconcile Flow

All changes in the `nifi-operator` submodule.

### Task 8: Update NiFi controller to use ReplicasConfig

**Files:**
- Modify: `nifi-operator/rust/operator-binary/src/controller.rs` (lines ~620-792: scaler discovery, replica resolution, reconcile loop)
- Modify: `nifi-operator/rust/operator-binary/src/main.rs` (lines ~180-196: watch registration)

- [ ] **Step 1: Switch watch registration from .watches() to .owns()**

In `main.rs`, replace the `.watches(StackableScaler, ...)` block with `.owns(Api::<StackableScaler>::all(...), ...)`.

Remove the scaler store and the mapper closure that filtered by `cluster-kind` label.

- [ ] **Step 2: Update the reconcile loop to use ReplicasConfig**

In `controller.rs`, replace the scaler discovery + `resolve_replicas()` pattern with a `match` on `ReplicasConfig`.

The new structure (pseudocode for the per-rolegroup section):

```rust
// Get the ReplicasConfig from the role group
let replicas_config = role_group.replicas.clone().unwrap_or_default();

// Find existing scaler via owned resources
let existing_scaler = find_owned_scaler(client, &nifi, &role_group_ref).await?;

// If scaling in progress, drive state machine and requeue
if let Some(ref scaler) = existing_scaler {
    if scaler.is_scaling_in_progress() {
        let replicas = scaler.status.as_ref().map(|s| s.replicas);
        let sts = build_node_rolegroup_statefulset(replicas, ...)?;
        cluster_resources.add(client, sts).await?;
        reconcile_scaler(scaler, &hooks, client, &role_group_ref.role_group, sts_stable, &selector).await?;
        return Ok(Action::requeue(...));
    }
}

match replicas_config {
    ReplicasConfig::Fixed(n) => {
        let sts = build_node_rolegroup_statefulset(Some(n as i32), ...)?;
        cluster_resources.add(client, sts).await?;
    }
    ReplicasConfig::Hpa(hpa_config) => {
        let scaler = build_scaler(
            &nifi.name_any(), namespace, "nodes", &role_group_ref.role_group,
            current_sts_replicas, &owner_ref, "nifi-operator",
        )?;
        let applied_scaler = cluster_resources.add(client, scaler).await?;

        // Initialize status if freshly created (prevents scale-to-zero)
        if applied_scaler.status.is_none() {
            initialize_scaler_status(client, &applied_scaler, current_sts_replicas, &selector).await?;
        }

        let hpa = build_hpa_from_user_spec(&hpa_config.spec, scale_target_ref(&applied_scaler))?;
        cluster_resources.add(client, hpa).await?;

        let replicas = applied_scaler.status.as_ref().map(|s| s.replicas);
        let sts = build_node_rolegroup_statefulset(replicas, ...)?;
        cluster_resources.add(client, sts).await?;

        reconcile_scaler(&applied_scaler, &NifiScalingHooks { ... }, client, &role_group_ref.role_group, sts_stable, &selector).await?;
    }
    ReplicasConfig::Auto(_auto_config) => {
        // Auto variant is not yet implemented — log a warning and treat as
        // an error until per-product HPA templates are defined in a follow-up.
        return Err(Error::AutoScalingNotYetImplemented { ... });
    }
    ReplicasConfig::ExternallyScaled => {
        // Same as Hpa but no HPA created — only the scaler
        let scaler = build_scaler(
            &nifi.name_any(), "nifi", namespace, "nodes", &role_group_ref.role_group,
            current_sts_replicas, &owner_ref, "nifi-operator",
        )?;
        let applied_scaler = cluster_resources.add(client, scaler).await?;

        if applied_scaler.status.is_none() {
            initialize_scaler_status(client, &applied_scaler, current_sts_replicas, &selector).await?;
        }

        let replicas = applied_scaler.status.as_ref().map(|s| s.replicas);
        let sts = build_node_rolegroup_statefulset(replicas, ...)?;
        cluster_resources.add(client, sts).await?;

        reconcile_scaler(&applied_scaler, &NifiScalingHooks { ... }, client, &role_group_ref.role_group, sts_stable, &selector).await?;
    }
}
```

- [ ] **Step 3: Remove old scaler discovery code**

Remove:
- The `list_with_label_selector::<StackableScaler>` + `.find()` pattern
- The `resolve_replicas()` call
- The `rg_replicas == Some(0)` check

- [ ] **Step 4: Add helper for finding owned scaler**

Use the `.owns()` reflector store (populated by the controller runtime) to find the scaler, avoiding an API list call on every reconcile. The store is available from the controller builder.

Pass the scaler store into the reconciler context (or as a parameter). Then filter locally:

```rust
fn find_owned_scaler(
    scaler_store: &Store<StackableScaler>,
    cluster: &NifiCluster,
    role_group: &str,
) -> Option<Arc<StackableScaler>> {
    let cluster_uid = cluster.metadata.uid.as_deref().unwrap_or_default();
    scaler_store.state().into_iter().find(|s| {
        s.metadata.owner_references.as_ref().map_or(false, |refs| {
            refs.iter().any(|r| r.uid == cluster_uid)
        }) && s.metadata.labels.as_ref().map_or(false, |l| {
            l.get("app.kubernetes.io/role-group").map(String::as_str) == Some(role_group)
        })
    })
}
```

The `.owns()` registration in `main.rs` already populates this store. Thread it through to the reconciler the same way the current code threads the existing scaler store.

- [ ] **Step 5: Handle ClusterStopped**

When the apply strategy is `ClusterStopped`, still add scaler and HPA to `cluster_resources` but skip `reconcile_scaler()`. The StatefulSet's `maybe_mutate` handles setting replicas to 0.

- [ ] **Step 6: Fix compilation errors and run checks**

Run: `cd nifi-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 7: Commit**

```bash
cd nifi-operator
git add -A
git commit -m "feat: switch to ReplicasConfig-based reconcile with .owns() watches

Replace replicas: 0 convention with ReplicasConfig enum matching.
Create StackableScaler and HPA via ClusterResources.add().
Switch from .watches() to .owns() for scaler event routing."
```

---

## Chunk 4: trino-operator — New Reconcile Flow

Same pattern as Chunk 3 but for trino-operator.

### Task 9: Update Trino controller to use ReplicasConfig

**Files:**
- Modify: `trino-operator/rust/operator-binary/src/controller.rs` (lines ~630-797: scaler discovery, replica resolution, reconcile loop)
- Modify: `trino-operator/rust/operator-binary/src/main.rs` (lines ~202-220: watch registration)

- [ ] **Step 1: Switch watch registration from .watches() to .owns()**

Same pattern as Task 8 Step 1: replace `.watches(StackableScaler, ...)` with `.owns(Api::<StackableScaler>::all(...), ...)`.

- [ ] **Step 2: Update the reconcile loop to use ReplicasConfig**

Same pattern as Task 8 Step 2: match on `ReplicasConfig` variant, manage scaler/HPA via `ClusterResources.add()`, drive state machine first if scaling is in progress.

**Important: Trino worker-only guard.** The current code only allows scaling for `TrinoRole::Worker`. Preserve this restriction: for non-worker roles (`Coordinator`), `Hpa`/`Auto`/`ExternallyScaled` variants should return an error. Only `Fixed` is valid for coordinators. Add a validation check at the top of the per-rolegroup section:

```rust
if trino_role != TrinoRole::Worker {
    match &replicas_config {
        ReplicasConfig::Fixed(_) => {} // OK
        other => return Err(Error::ScalingNotSupportedForRole {
            role: trino_role.to_string(),
            config: format!("{other:?}"),
        }),
    }
}
```

- [ ] **Step 3: Remove old scaler discovery code**

Same as Task 8 Step 3.

- [ ] **Step 4: Add find_owned_scaler() helper**

Same pattern as Task 8 Step 4, adapted for `TrinoCluster`.

Note: Consider extracting this to a shared function in operator-rs if the pattern is identical. However, the function is small and product-specific (it references the product cluster type), so duplication is acceptable.

- [ ] **Step 5: Handle ClusterStopped**

Same as Task 8 Step 5.

- [ ] **Step 6: Fix compilation errors and run checks**

Run: `cd trino-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 7: Commit**

```bash
cd trino-operator
git add -A
git commit -m "feat: switch to ReplicasConfig-based reconcile with .owns() watches

Replace replicas: 0 convention with ReplicasConfig enum matching.
Create StackableScaler and HPA via ClusterResources.add().
Switch from .watches() to .owns() for scaler event routing."
```

---

## Chunk 5: Cross-Repo Validation & Cleanup

### Task 10: Build all repos together and validate

- [ ] **Step 1: Ensure operator-rs compiles cleanly**

Run: `cd operator-rs && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features && cargo doc --no-deps --all-features 2>&1 | grep -E "^warning" && echo "Doc warnings found" || echo "Docs clean"`

- [ ] **Step 2: Ensure commons-operator compiles against updated operator-rs**

Run: `cd commons-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 3: Ensure nifi-operator compiles against updated operator-rs**

Run: `cd nifi-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 4: Ensure trino-operator compiles against updated operator-rs**

Run: `cd trino-operator && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

- [ ] **Step 5: Remove resolve_replicas() if no longer used**

Check if `resolve_replicas()` is still referenced anywhere. If not, remove it from `operator-rs/crates/stackable-operator/src/crd/scaler/mod.rs` and its tests.

Run: `cd operator-rs && cargo test --all-features`

- [ ] **Step 6: Final commit if cleanup was needed**

```bash
cd operator-rs
git add -A
git commit -m "chore: remove unused resolve_replicas() function"
```
