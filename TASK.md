# Autoscale Review Issues

Issues found during code review of `feat/autoscale` branches across operator-rs, commons-operator, and nifi-operator.

## Critical

### 1. `danger_accept_invalid_certs(true)` skips all TLS verification — TODO added
**Location:** `nifi-operator/.../operations/nifi_api.rs:213`

The rest of the NiFi operator enforces mTLS with keystores/truststores. The scaling API client bypasses all of that. A `TODO(#1)` comment has been added at the call site. Proper fix requires loading the CA cert from the Stackable secret operator.

## Important

### ~~2. `on_failure` can fire multiple times~~ RESOLVED
**Location:** `operator-rs/.../crd/scaler/reconciler.rs:handle_hook_failure`

Fixed: `Failed` status is now written before calling `on_failure`. If cleanup also fails, the status reason is updated to include the cleanup error for visibility.

### 3. `failure_policy: Fail` blocks HPA when operator is down — TODO added
**Location:** `commons-operator/.../webhooks/scaler_admission.rs:94`

With no `object_selector` and `Fail` policy, any HPA `/scale` write is blocked when the commons-operator is unavailable (rolling update, crash). The restarter uses `Ignore` for this reason. A `TODO(#3)` comment has been added. Requires a design decision on `Ignore` vs narrowing scope.

### 4. `reinvocation_policy: Never` contradicts established pattern — TODO added
**Location:** `commons-operator/.../webhooks/scaler_admission.rs:95`

The restarter explicitly uses `IfNeeded` with a documented rationale. A `TODO(#4)` comment has been added. Requires a decision on whether to align with the restarter pattern.

### ~~5. Empty credentials for non-SingleUser auth~~ RESOLVED
**Location:** `nifi-operator/.../controller.rs:735-767`

Fixed: LDAP and OIDC arms now return `UnsupportedScalerAuthentication` error with a clear message naming the auth method, instead of passing an empty secret name.

### ~~6. Phase 2 wildcard swallows unknown statuses~~ RESOLVED
**Location:** `nifi-operator/.../operations/scaling.rs:250,344`

Fixed in `01ec6fb`: parsing moved to `nifi_api.rs` (unknown API strings rejected as `UnexpectedNodeStatus`), and Phase 2 wildcards replaced with explicit `other => UnexpectedPhaseStatusSnafu` error arms.

### ~~7. Silent skip when FQDN doesn't match~~ RESOLVED
**Location:** `nifi-operator/.../operations/scaling.rs:167-177`

Fixed: Now logs a warning with the expected FQDN, the actual cluster node addresses, and a hint to check service/domain configuration. Not an error because the "already removed" case is a normal occurrence during reconciliation.

## Minor

### ~~8. `is_nifi_2()` string prefix check is fragile~~ RESOLVED
**Location:** `nifi-operator/.../operations/scaling.rs:113`

Fixed: `is_nifi_2()` now parses the major version from the semver string (`split('.').next().parse::<u32>()`) and checks `>= 2`. Falls back to NiFi 1.x behavior on unparseable input.

### ~~9. `pod_fqdn` strips `-headless` suffix~~ RESOLVED
**Location:** `nifi-operator/.../operations/scaling.rs:91`

Fixed: Added `statefulset_name` field to `NifiScalingHooks`, passed from `rolegroup.object_name()` in the controller. `pod_fqdn` uses it directly instead of stripping `-headless`.