# StackableScaler Admission Webhook

## Context

The StackableScaler ADR specifies webhook operations (Decisions 4, 8) that are currently unimplemented. Without them:
- Product operators can only watch StackableScalers if users manually set the `stackable.tech/cluster-kind` label
- The HPA can write `spec.replicas` mid-flight, corrupting the state machine

## Approach: Single MutatingWebhook, no external lookups

A `MutatingWebhook` can both mutate and reject (`AdmissionResponse::invalid()`). Both operations only inspect the StackableScaler itself — no StatefulSet or cluster CR lookups needed.

**Dropped features:**
- `replicas: 0` validation on CREATE — Kubernetes eventual consistency should allow creating resources in any order
- `spec.replicas` seeding from StatefulSet — user sets initial replicas in manifest, HPA corrects quickly; avoids StatefulSet lookup complexity

## Operations

| # | Trigger | Action | Source |
|---|---------|--------|--------|
| 1 | CREATE + UPDATE | Inject label `stackable.tech/cluster-kind` from `spec.clusterRef.kind` | ADR Decision 4 |
| 2 | UPDATE | Reject `spec.replicas` changes when stage is not Idle/Failed | ADR Decision 8 |

## Task 1: Webhook handler

**New file:** `commons-operator/rust/operator-binary/src/webhooks/scaler_admission.rs`

Follow the pattern of `restarter_mutate_sts.rs`.

**Webhook configuration:**
```
name: "scaler-admission.stackable.tech"
apiGroups: ["autoscaling.stackable.tech"]
apiVersions: ["v1alpha1"]
resources: ["stackablescalers"]
operations: ["CREATE", "UPDATE"]
failurePolicy: "Fail"
sideEffects: "None"
reinvocationPolicy: "Never"
```

**Handler:**
```
fn handler(_ctx: Arc<()>, request: AdmissionRequest<StackableScaler>) -> AdmissionResponse:

  // --- Validation ---
  // On UPDATE: reject spec.replicas changes during active scaling
  if request.operation == UPDATE:
    old = request.old_object
    new = request.object
    if new.spec.replicas != old.spec.replicas:
      stage = old.status.currentState.stage   // use old_object for server-side status
      if stage not in {Idle, Failed, None}:
        return AdmissionResponse::invalid(
          "Cannot update spec.replicas while scaling is in progress (stage: {stage})")

  // --- Mutation ---
  // Inject cluster-kind label from spec.clusterRef.kind
  scaler = request.object
  patches = []
  if scaler.metadata.labels is None:
    patches.push(Add "/metadata/labels" = {})
  patches.push(Add "/metadata/labels/stackable.tech~1cluster-kind" = scaler.spec.clusterRef.kind)

  return AdmissionResponse::from(&request).with_patch(patches)
```

**Context:** `Arc<()>` — no state needed since there are no external lookups.

**Dependencies:** `json_patch::{Patch, PatchOperation, AddOperation}`, `kube::core::admission::*`, `stackable_operator::crd::scaler::*`

## Task 2: Wire up

**Modify:** `commons-operator/rust/operator-binary/src/webhooks/mod.rs`
- Add `mod scaler_admission;`
- In `create_webhook_server`: call `scaler_admission::create_webhook(client)`, push to webhooks vec

**Modify:** `commons-operator/rust/operator-binary/src/main.rs`
- Add `--disable-scaler-admission-webhook` CLI flag
- Pass to `create_webhook_server`

## Files

| File | Change |
|------|--------|
| `commons-operator/.../webhooks/scaler_admission.rs` | **NEW** |
| `commons-operator/.../webhooks/mod.rs` | Register webhook |
| `commons-operator/.../main.rs` | CLI flag |

## Pattern reference

| What | Where |
|------|-------|
| Webhook config + handler | `commons-operator/.../webhooks/restarter_mutate_sts.rs` |
| Registration | `commons-operator/.../webhooks/mod.rs:21-50` |
| StackableScaler types | `operator-rs/.../crd/scaler/mod.rs` |
| ScalerStage enum | `operator-rs/.../crd/scaler/mod.rs` (Idle, PreScaling, Scaling, PostScaling, Failed) |

## Verification

1. `cargo check -p commons-operator` — compiles
2. Deploy, create StackableScaler without `stackable.tech/cluster-kind` label -> label auto-injected
3. Trigger scale-down, attempt `kubectl edit` to change `spec.replicas` while in PreScaling -> rejected
4. Same change while in Idle or Failed -> allowed
