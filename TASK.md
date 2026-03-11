# Autoscale Review Issues

Issues found during code review of `feat/autoscale` branches across operator-rs, commons-operator, and nifi-operator.

## Critical

### 1. `danger_accept_invalid_certs(true)` skips all TLS verification
**Location:** `nifi-operator/.../operations/nifi_api.rs:150`

The rest of the NiFi operator enforces mTLS with keystores/truststores. The scaling API client bypasses all of that. Should at minimum be documented as a TODO.

## Important

### 2. `on_failure` can fire multiple times
**Location:** `operator-rs/.../crd/scaler/reconciler.rs:129-142`

If `patch_status` fails after `on_failure` runs, the next reconcile re-enters the same hook failure path and calls `on_failure` again. Either document that `on_failure` must be idempotent, or write the `Failed` status before calling it.

### 3. `failure_policy: Fail` blocks HPA when operator is down
**Location:** `commons-operator/.../webhooks/scaler_admission.rs:65`

With no `object_selector` and `Fail` policy, any HPA `/scale` write is blocked when the commons-operator is unavailable (rolling update, crash). The restarter uses `Ignore` for this reason. Consider `Ignore` or narrowing scope.

### 4. `reinvocation_policy: Never` contradicts established pattern
**Location:** `commons-operator/.../webhooks/scaler_admission.rs:66`

The restarter explicitly uses `IfNeeded` with a documented rationale. No comment explains why this webhook differs.

### 5. Empty credentials for non-SingleUser auth
**Location:** `nifi-operator/.../controller.rs:729-739`

Non-SingleUser auth sets `credentials_secret_name = String::new()`, which later causes a confusing K8s API error when trying to fetch Secret `""`. Should either skip `reconcile_scaler` entirely or return a clear error.

### 6. Phase 2 wildcard swallows unknown statuses
**Location:** `nifi-operator/.../operations/scaling.rs:250,344`

Phase 1 correctly errors on unknown status (`None` from `from_api_str`). Phase 2 silently treats it as `InProgress`, causing infinite requeuing with no error. Should match `None` explicitly and error like Phase 1.

### 7. Silent skip when FQDN doesn't match
**Location:** `nifi-operator/.../operations/scaling.rs:151-154`

If a target node's FQDN doesn't match any NiFi cluster node, it's silently skipped as "already removed". The scaler returns `Done` and advances to `Scaling` -- reducing replicas without offloading. Should log a warning.

## Minor

### 8. `is_nifi_2()` string prefix check is fragile
**Location:** `nifi-operator/.../operations/scaling.rs:115`

Parse semver major version instead of `starts_with("2.")`.

### 9. `pod_fqdn` strips `-headless` suffix
**Location:** `nifi-operator/.../operations/scaling.rs:92-95`

Brittle coupling to naming convention. Pass StatefulSet name separately into `NifiScalingHooks`.
