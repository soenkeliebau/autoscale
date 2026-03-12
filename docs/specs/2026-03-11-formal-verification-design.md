# Formal Verification of StackableScaler: Design Specification

## Context

The StackableScaler is a Kubernetes operator component that mediates between the HPA (Horizontal Pod Autoscaler) and product operators (e.g., NiFi) to perform graceful scaling with pre/post hooks. It involves multiple concurrent actors: the reconciler, an admission webhook, the HPA, the product operator, and the StatefulSet controller. Correctness depends on their interleaving behavior.

Existing Quint specifications in `proof/` model the state machine at an abstract level but miss critical details: multi-actor interleaving, TOCTOU races in the admission webhook, the reconciliation-from-scratch model, `previous_replicas` tracking, and `on_failure` atomicity. These specs are frozen and retained as historical context.

This design specifies a comprehensive formal verification using TLA+/PlusCal that closely follows the actual implementation and models the full distributed-system behavior.

## Tool Choice: TLA+/PlusCal

PlusCal's `process` construct maps directly to independent actors with arbitrary interleaving. TLC (the model checker) is mature, supports fairness-constrained liveness checking, and has decades of industrial use for distributed systems. PlusCal compiles to TLA+, combining readable algorithmic syntax with full TLA+ expressiveness for properties and refinement.

## Architecture: Two-File Layered Approach

### File 1: `StackableScalerFramework.tla`

The core specification. Hooks are abstract (nondeterministic Done/InProgress/Error). Verifies the scaler framework independent of any specific operator.

### File 2: `NiFiScalerRefinement.tla`

Extends File 1. Replaces abstract hooks with the NiFi node lifecycle state machine. Proves the NiFi implementation correctly refines the abstract hook contract. Adds NiFi-specific invariants.

## Shared State (API Server Model)

The Kubernetes API server is modeled as shared global variables. Writes are serialized (each PlusCal label is atomic). Reads are linearizable but can become stale between the read label and subsequent write labels due to interleaving.

### Variables

```
\* StackableScaler resource
spec_replicas           \* spec.replicas (written by HPA via /scale subresource)
status_replicas         \* status.replicas (written by scaler reconciler)
status_desired          \* status.desiredReplicas (NULL when not scaling)
status_previous         \* status.previousReplicas (NULL when not scaling)
status_stage            \* status.currentState.stage
retry_annotation        \* boolean: retry annotation present

\* StatefulSet resource
sts_spec_replicas       \* StatefulSet spec.replicas (written by product operator)
sts_ready_replicas      \* StatefulSet status.readyReplicas (written by STS controller)

\* Webhook state
webhook_available       \* boolean: webhook server is up

\* Auxiliary: tracks previous_replicas value at PreScaling entry for S10 verification
aux_previous_at_entry   \* snapshot of status_previous when PreScaling was entered
```

### Constants

```
MAX_REPLICAS = 3        \* keeps state space tractable
NULL = -1               \* sentinel for Option::None
```

### Initial State

```
spec_replicas       = 1
status_replicas     = 1
status_desired      = NULL
status_previous     = NULL
status_stage        = "Idle"
retry_annotation    = FALSE
sts_spec_replicas   = 1
sts_ready_replicas  = 1
webhook_available   = TRUE
aux_previous_at_entry = NULL
```

### Key Modeling Decisions

- `sts_spec_replicas` is a **separate** variable from `status_replicas`. In the real system, the scaler reconciler writes `status_replicas` to the StackableScaler status, and the **product operator** (a separate reconcile cycle) propagates this to the StatefulSet's `spec.replicas`. These are not atomic. A `ProductOperator` process models this propagation.
- `status_stage` uses string values: `"Idle"`, `"PreScaling"`, `"Scaling"`, `"PostScaling"`, `"FailedAtPre"`, `"FailedAtPost"`.
- The `FailedStage::Scaling` variant exists in the Rust CRD definition but no code path currently transitions to it. It is excluded from the model. If a future code change uses it, the model must be updated.
- `aux_previous_at_entry` is an auxiliary (history) variable used to verify S10. It does not correspond to any real system state.

## Processes

### Reconciler

Models the scaler reconcile loop (`reconcile_scaler`). Controller-runtime serializes reconciles per object, modeled with a `reconcile_running` mutex.

Each reconcile iteration:

1. **Acquire lock** — `await ~reconcile_running; reconcile_running := TRUE`
2. **Read STS** (label `ReadSTS`) — reads `sts_ready_replicas`. In the real code, this comes from the StatefulSet server-side apply response (fresh).
3. **Read Scaler** (label `ReadScaler`) — reads all scaler fields from a single object. In the real code, this is a live API list call (fresh). Both `spec_replicas` and all `status_*` fields come from the same object in the same read. Other actors can interleave between `ReadSTS` and `ReadScaler`.
4. **Decision and write** — based on local copies, transitions the state machine and patches status.
5. **Release lock** — `reconcile_running := FALSE`

Between any read label and the subsequent write label, other processes can modify global state. This captures the "read was fresh but stale by write time" property.

**Stage handling:**

- **Idle**: If `local_status == local_spec`, no-op. Otherwise, transition to PreScaling: set `status_desired := local_spec`, `status_previous := local_status`, `aux_previous_at_entry := local_status`. Note: `status_replicas` is NOT updated here.
- **PreScaling**: Hook outcome chosen nondeterministically. Done → transition to Scaling, update `status_replicas := local_desired` (this is the only point where `status_replicas` changes). InProgress → requeue. Error → write `status_stage := "FailedAtPre"`, then run `on_failure` in a separate label (atomicity: status is already Failed before cleanup runs).
- **Scaling**: Check `statefulset_stable` (computed from `local_sts_ready == local_sts_spec /\ (local_sts_spec > 0 \/ local_desired = 0)`), where `local_sts_spec` is read from `sts_spec_replicas` (not `status_replicas`). Stable → PostScaling. Not stable → requeue.
- **PostScaling**: Hook outcome chosen nondeterministically. Done → transition to Idle, clear `status_desired := NULL` and `status_previous := NULL`. InProgress → requeue. Error → write `status_stage := "FailedAtPost"`, then `on_failure`.
- **Failed**: Check `local_retry`. If retry annotation present: strip annotation (`retry_annotation := FALSE`), reset to Idle, clear `status_desired := NULL` and `status_previous := NULL`. The old desired/previous values from the failed operation are discarded. The next reconcile will read the CURRENT `spec_replicas` (which may have been changed by HPA while in Failed state) and start a fresh scaling operation if needed. Otherwise no-op.

### ProductOperator

Models the product operator (e.g., NiFi) propagating `status_replicas` to the StatefulSet. This is a separate reconcile cycle from the scaler reconciler.

Single label `PropagateToSTS`:
```
PropagateToSTS:
  sts_spec_replicas := status_replicas;
```

This runs nondeterministically, capturing the delay between the scaler writing `status_replicas` and the product operator applying it to the StatefulSet. Between the scaler's status patch and this propagation, `sts_spec_replicas` can differ from `status_replicas`.

### HPAWriter

Models the HPA writing `spec.replicas` through the admission webhook. Three labels capturing the TOCTOU gap:

1. **SelectTarget** — pick `hpa_target` nondeterministically from `0..MAX_REPLICAS`.
2. **WebhookCheck** — if `webhook_available`, read `status_stage` (live GET). Allow if Idle or Failed; deny if PreScaling/Scaling/PostScaling. If webhook unavailable, deny (failure policy = Fail).
3. **APIServerApply** — if allowed, write `spec_replicas := hpa_target`. Between `WebhookCheck` and `APIServerApply`, the reconciler can change `status_stage`, making the webhook's check stale. This is the TOCTOU window.

### STSController

Moves `sts_ready_replicas` one step toward `sts_spec_replicas` per step. Single label `Progress`.

### RetryAnnotator

Models manual retry annotation application. Sets `retry_annotation := TRUE` when `status_stage` is FailedAtPre or FailedAtPost. Single label.

### WebhookLifecycle

Toggles `webhook_available` nondeterministically between TRUE and FALSE. Models crash, restart, and certificate rotation.

## Safety Properties (Invariants)

State invariants must hold in every reachable state.

### State Consistency

| ID | Property | Notes |
|----|----------|-------|
| S1 | `spec_replicas >= 0 /\ status_replicas >= 0 /\ sts_ready_replicas >= 0` | |
| S2 | `status_desired = NULL \/ status_desired >= 0` | |
| S3 | `status_stage = "Idle" /\ status_desired = NULL /\ ~reconcile_running => status_replicas = spec_replicas` | Guarded by `~reconcile_running`: the HPA can write `spec_replicas` while Idle, creating a transient window where `status != spec` until the reconciler runs. The invariant only holds when no reconcile is pending (i.e., the system has quiesced). **Note:** this may still be too strong if the HPA can write between lock release and the next reconcile. If TLC finds a counterexample, weaken further by tracking whether the reconciler has observed the current `spec_replicas`. |
| S4 | `status_stage \in {"PreScaling", "Scaling", "PostScaling"} => status_desired /= NULL` | |
| S5 | `status_stage = "Idle" => status_desired = NULL` | |
| S6 | `status_stage = "Scaling" => status_replicas = status_desired` | |
| S7 | `status_stage = "PostScaling" => status_replicas = status_desired` | |

### Previous Replicas and Direction Stability

| ID | Property | Notes |
|----|----------|-------|
| S8 | `status_stage \in {"PreScaling", "Scaling", "PostScaling"} => status_previous /= NULL` | |
| S9 | `status_stage \in {"PreScaling", "Scaling", "PostScaling"} => status_previous = aux_previous_at_entry` | Uses auxiliary variable. Verifies `previous_replicas` is frozen for the duration of a scaling operation. |
| S10 | `status_stage = "Idle" => status_previous = NULL` | |

### Webhook Enforcement and TOCTOU

| ID | Property | Notes |
|----|----------|-------|
| S11 | **TOCTOU detection property** (expected to find counterexample): `status_stage \in {"PreScaling", "Scaling", "PostScaling"} => spec_replicas = status_desired` | This property WILL be violated if the TOCTOU race in the HPAWriter is reachable. Finding a counterexample proves the race exists. If TLC finds no counterexample, the webhook protection is stronger than expected. Either outcome is informative. |

### Atomicity and Ordering

| ID | Property | Notes |
|----|----------|-------|
| S12 | When Reconciler is at `HandleOnFailure` label, `status_stage \in {"FailedAtPre", "FailedAtPost"}` | Verifies status is already Failed before `on_failure` cleanup runs. |
| S13 | When Reconciler is at `HandlePreScaling` label, `status_stage = "PreScaling"` | Hook called in correct stage. Note: checks the LOCAL stage read, not global, since the reconciler acts on local copies. |
| S14 | When Reconciler is at `HandlePostScaling` label, `status_stage = "PostScaling"` | Same as S13 for post_scale. |

### Transition Validity

| ID | Property | Notes |
|----|----------|-------|
| S15 | Stage transitions form a DAG: Idle→PreScaling→Scaling→PostScaling→Idle, plus PreScaling→FailedAtPre, PostScaling→FailedAtPost, Failed→Idle (via retry). No other transitions. | Encoded by checking that each write to `status_stage` is a valid transition from the previous value. Requires an auxiliary `prev_stage` variable or is verified structurally by the PlusCal code. |

### Scale-to-Zero

| ID | Property | Notes |
|----|----------|-------|
| S16 | `sts_ready_replicas = 0 /\ sts_spec_replicas = 0 /\ status_stage = "Scaling" => status_desired = 0` | Prevents false stability on a fresh zero-replica StatefulSet. The `statefulset_stable` formula requires `status_desired = 0` to consider zero-replica STS stable. |

### Bounds

| ID | Property | Notes |
|----|----------|-------|
| S17 | `status_replicas >= 0 /\ status_replicas <= MAX_REPLICAS` | |
| S18 | `sts_ready_replicas >= 0 /\ sts_ready_replicas <= MAX_REPLICAS` | |

## Temporal Properties (Action and Liveness)

These are checked as TLA+ temporal formulas, not state invariants.

### Action Properties

| ID | Property | Notes |
|----|----------|-------|
| A1 | `[][~webhook_available => spec_replicas' = spec_replicas]_vars` | When webhook is unavailable, `spec_replicas` does not change. Expressed as a temporal action property (box-action). |
| A2 | `[][(status_stage = "Idle" /\ status_stage' = "PreScaling") => status_previous' /= NULL]_vars` | Every Idle→PreScaling transition sets `previous_replicas`. |

### Liveness (Unconditional)

Require weak fairness on the Reconciler process.

| ID | Property | Fairness |
|----|----------|----------|
| L1 | Every scaling operation eventually reaches Idle or Failed | WF on Reconciler |
| L2 | Failed + retry annotation eventually leaves Failed | WF on Reconciler |

### Liveness (Conditional — require environment cooperation)

| ID | Property | Condition |
|----|----------|-----------|
| L3 | PreScaling eventually exits | Hooks eventually return Done or Error |
| L4 | Scaling eventually exits | ProductOperator eventually propagates, STS eventually converges |
| L5 | PostScaling eventually exits | Hooks eventually return Done or Error |
| L6 | `spec_replicas` change in Idle eventually reaches Idle with `status_replicas = spec_replicas` | Hooks succeed, STS converges, ProductOperator runs |

### No Missed Operations

| ID | Property | Notes |
|----|----------|-------|
| L7 | Every Idle→PreScaling transition is followed by `pre_scale` invocation before any further stage transition | Encoded as a temporal leads-to property. |
| L8 | Every Scaling→PostScaling transition is followed by `post_scale` invocation before returning to Idle | Same pattern. |
| L9 | `spec_replicas /= status_replicas` in Idle eventually leads to PreScaling or `spec_replicas` changes back | No scale requests ignored. |

### Non-Starvation

| ID | Property | Notes |
|----|----------|-------|
| L10 | If `webhook_available` and stage is Idle, HPA can eventually write `spec_replicas` | WF on HPAWriter. |

## NiFi Refinement Spec

### Additional Process: NiFiNodeManager

Replaces abstract hook nondeterminism with the NiFi node lifecycle state machine.

**Per-node state** (array indexed by ordinal `0..MAX_REPLICAS-1`):

```
node_status[i]    \* ABSENT | CONNECTING | CONNECTED | OFFLOADING | OFFLOADED | DISCONNECTING | DISCONNECTED
nifi_version      \* 1 | 2
removal_targets   \* set of ordinals being removed
```

Node statuses correspond to the `NifiNodeStatus` enum in `nifi_api.rs`. There is no `DELETED` or `ERROR` status in NiFi — deletion is an API call that removes the node from the cluster, after which it becomes `ABSENT`. Errors arise from API call failures (network, unexpected HTTP responses), modeled as nondeterministic failure of transition actions.

**`removal_targets` initialization and update:**
- Set when the scaler enters PreScaling: `removal_targets := {status_desired .. status_previous - 1}` (the ordinals being removed, highest first per StatefulSet convention).
- Cleared when the scaler returns to Idle: `removal_targets := {}`.
- For scale-up, `removal_targets` is empty (pre_scale returns Done immediately for scale-up in the NiFi implementation).

**Version-specific state transitions:**

- NiFi 1.x: CONNECTED → OFFLOADING → OFFLOADED → DISCONNECTING → DISCONNECTED → (DELETE API call) → ABSENT
- NiFi 2.x: CONNECTED → DISCONNECTING → DISCONNECTED → OFFLOADING → OFFLOADED → (DELETE API call) → ABSENT
- `CONNECTING` nodes are treated as "in progress" — the hook returns `InProgress` without attempting a transition.

**Refinement mapping:**

The abstract hook outcome is computed deterministically from node state:

```
hook_outcome =
  IF removal_targets = {} THEN
    "Done"                             \* scale-up: nothing to do
  ELSE IF \A i \in removal_targets: node_status[i] = "ABSENT" THEN
    "Done"                             \* all target nodes removed
  ELSE IF api_call_failed THEN
    "Error"                            \* nondeterministic API failure
  ELSE
    "InProgress"                       \* nodes still transitioning
```

`api_call_failed` is a nondeterministic boolean chosen each reconcile, modeling NiFi REST API failures (timeouts, unexpected status codes, authentication errors). In the real code, these surface as `Error` variants in the `scaling.rs` error enum.

All framework variables map identically. `removal_targets`, `node_status`, `nifi_version`, and `api_call_failed` are new variables that exist only in the refinement. TLC verifies every behavior of the NiFi spec is a behavior of the abstract spec via the refinement mapping on `hook_outcome`.

### NiFi-Specific Notes

- NiFi's `post_scale` hook uses the trait default (returns `Done` immediately). The refinement spec reflects this: `post_scale` outcome is always `Done`.
- NiFi's `on_failure` hook uses the trait default (no-op). The refinement spec reflects this.
- Only `pre_scale` during scale-down drives the node lifecycle.

### NiFi Safety Invariants

| ID | Property | Notes |
|----|----------|-------|
| N1 | Node status transitions follow version-specific ordering | No CONNECTED→DISCONNECTED skip in 1.x, no CONNECTED→OFFLOADING skip in 2.x, etc. |
| N2 | Ordinal 0 is never in `removal_targets` | Pod-0 hosts the API endpoint used for the REST calls. |
| N3 | `removal_targets = {status_desired .. status_previous - 1}` when non-empty | Correct ordinals targeted. |
| N4 | No node has DELETE API called while still CONNECTED | Data loss: deleted without offload/disconnect. |
| N5 | At the PreScaling→Scaling transition, all nodes in `removal_targets` are ABSENT | All targeted nodes must be fully removed before the StatefulSet is scaled down. This is checked at the moment the hook returns `Done`. |

### NiFi Liveness

| ID | Property | Notes |
|----|----------|-------|
| NL1 | Every OFFLOADING node eventually reaches OFFLOADED (conditional on NiFi API responsiveness) | |
| NL2 | Scale-down eventually removes all targeted nodes (conditional on NiFi API responsiveness) | |

## File Layout

```
proof/
  proof.qnt                          # frozen — historical
  stackable_scaler.qnt               # frozen — historical
  tla/
    StackableScalerFramework.tla      # core spec (PlusCal)
    NiFiScalerRefinement.tla          # NiFi refinement spec
    StackableScalerFramework.cfg      # TLC config: constants, invariants, liveness
    NiFiScalerRefinement.cfg          # TLC config for refinement checking
    Makefile                          # build/run targets
```

### TLC Configuration

`MAX_REPLICAS = 3` keeps state space tractable (~millions of states). The existing Quint specs used `MAX_REPLICAS = 4`; the TLA+ model uses 3 because the multi-actor interleaving significantly increases the state space. TLC runs exhaustive model checking for safety properties and bounded liveness checking with fairness constraints.

### Makefile Targets

- `check-framework` — run TLC on `StackableScalerFramework`
- `check-nifi` — run TLC on `NiFiScalerRefinement`
- `check-all` — both
- `simulate` — random simulation (faster, no exhaustive check)

### Dependencies

- Java 11+ (for TLC)
- `tla2tools.jar` — TLA+ toolbox CLI (downloadable, ~10MB)

### CI Integration (Optional)

GitHub Actions job running `make check-all` on PRs touching `proof/tla/` or scaler implementation files.

## Design Decisions

1. **TLA+/PlusCal over Quint** — PlusCal's `process` construct directly models independent actors. TLC is more mature for liveness checking with fairness.
2. **Two-file layered approach** — Framework spec verifiable independently with small state space. NiFi refinement only adds NiFi-specific state.
3. **No API server process** — Modeled as shared variables with atomic writes. Avoids modeling etcd internals (resource versions, optimistic concurrency) which would explode state space with diminishing returns.
4. **Reconciler mutex** — Prevents exploring impossible concurrent-reconcile states, matching controller-runtime's work queue serialization.
5. **Split reads (ReadSTS + ReadScaler)** — Two labels with interleaving between them, matching the real code where STS and Scaler are read at different times via different mechanisms.
6. **No artificial staleness** — Reads are fresh at time of read. Staleness emerges naturally from interleaving between read and write labels.
7. **ProductOperator as separate process** — The product operator propagates `status_replicas` to `sts_spec_replicas` in a separate reconcile cycle, not atomically with the scaler's status write. This is critical for catching desync bugs between the scaler and StatefulSet.
8. **S11 as TOCTOU detection** — Rather than asserting the webhook prevents all mid-flight writes (which the TOCTOU race can violate), S11 is framed as a detection property. A counterexample proves the race is reachable and quantifies its impact.
9. **Existing Quint specs frozen** — Not maintained, serve as historical reference.
10. **FailedAtScaling excluded** — The `FailedStage::Scaling` variant exists in the CRD but no code path transitions to it. Excluded from the model to avoid exploring unreachable states.
