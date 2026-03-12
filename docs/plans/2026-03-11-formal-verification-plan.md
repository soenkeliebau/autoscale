# StackableScaler Formal Verification Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a comprehensive TLA+/PlusCal formal verification of the StackableScaler state machine with multi-actor interleaving, TOCTOU race detection, and NiFi refinement.

**Architecture:** Two-file layered TLA+ specs. `StackableScalerFramework.tla` models the core scaler with 6 concurrent PlusCal processes (Reconciler, HPAWriter, ProductOperator, STSController, RetryAnnotator, WebhookLifecycle) and abstract hooks. `NiFiScalerRefinement.tla` extends it with the NiFi node lifecycle state machine and proves refinement.

**Tech Stack:** TLA+, PlusCal, TLC model checker (via tla2tools.jar), Java 21, Make

**Spec:** `docs/specs/2026-03-11-formal-verification-design.md`

---

## File Map

| File | Purpose | Task |
|------|---------|------|
| `proof/tla/Makefile` | Build/run targets for TLC | 1 |
| `proof/tla/StackableScalerFramework.tla` | Core PlusCal spec with 6 processes, 18 safety invariants, 12 temporal properties | 2-6 |
| `proof/tla/StackableScalerFramework.cfg` | TLC config: constants, invariants, liveness | 7 |
| `proof/tla/NiFiScalerRefinement.tla` | NiFi refinement: node lifecycle, 5 NiFi invariants, 2 NiFi liveness, refinement proof via INSTANCE | 9 |
| `proof/tla/NiFiScalerRefinement.cfg` | TLC config for refinement checking | 10 |
| `proof/tla/TOCTOU.cfg` | TLC config for TOCTOU race detection (separate run) | 7 |
| `proof/tla/.gitignore` | Exclude tla2tools.jar from version control | 1 |

---

## Chunk 1: Infrastructure and Framework Spec

### Task 1: Project Setup

**Files:**
- Create: `proof/tla/Makefile`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p /home/sliebau/IdeaProjects/autoscale/proof/tla
```

- [ ] **Step 2: Download tla2tools.jar**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
curl -L -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
```

Verify: `java -jar tla2tools.jar -h 2>&1 | head -5` should show TLC usage info.

- [ ] **Step 3: Create .gitignore for tla2tools.jar**

Create `proof/tla/.gitignore`:

```
tla2tools.jar
states/
```

The jar is ~10MB — do not commit binaries to source control. The `download-tools` Makefile target handles fetching it.

- [ ] **Step 4: Write Makefile**

Create `proof/tla/Makefile`:

```makefile
TLA2TOOLS := tla2tools.jar
JAVA := java
TLC_OPTS := -workers auto
TLA2TOOLS_URL := https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

.PHONY: check-framework check-nifi check-toctou check-all simulate clean download-tools

download-tools:
	@test -f $(TLA2TOOLS) || curl -L -o $(TLA2TOOLS) $(TLA2TOOLS_URL)

check-framework: download-tools StackableScalerFramework.tla StackableScalerFramework.cfg
	$(JAVA) -jar $(TLA2TOOLS) -config StackableScalerFramework.cfg $(TLC_OPTS) StackableScalerFramework.tla

check-nifi: download-tools NiFiScalerRefinement.tla NiFiScalerRefinement.cfg
	$(JAVA) -jar $(TLA2TOOLS) -config NiFiScalerRefinement.cfg $(TLC_OPTS) NiFiScalerRefinement.tla

check-toctou: download-tools StackableScalerFramework.tla TOCTOU.cfg
	@echo "--- TOCTOU Race Detection ---"
	$(JAVA) -jar $(TLA2TOOLS) -config TOCTOU.cfg $(TLC_OPTS) StackableScalerFramework.tla

check-all: check-framework check-nifi

simulate: download-tools StackableScalerFramework.tla StackableScalerFramework.cfg
	$(JAVA) -jar $(TLA2TOOLS) -simulate -depth 100 -config StackableScalerFramework.cfg StackableScalerFramework.tla

clean:
	rm -rf states/
```

- [ ] **Step 5: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/Makefile proof/tla/.gitignore
git commit -m "chore: add TLA+ tooling for formal verification"
```

---

### Task 2: Framework Spec — Module Header, Constants, Variables, Init

**Files:**
- Create: `proof/tla/StackableScalerFramework.tla`

- [ ] **Step 1: Write the module shell with PlusCal algorithm block**

Create `proof/tla/StackableScalerFramework.tla`. This step writes the module header, constants, variable declarations, the `define` block (derived state helpers and all safety invariants), and the `Init` action inside the PlusCal `--algorithm` block. No processes yet.

```tla
--------------------------- MODULE StackableScalerFramework ---------------------------
EXTENDS Integers, Sequences, TLC, FiniteSets

CONSTANTS MAX_REPLICAS, NULL

(* --algorithm StackableScaler

variables
    \* StackableScaler resource
    spec_replicas    = 1,
    status_replicas  = 1,
    status_desired   = NULL,
    status_previous  = NULL,
    status_stage     = "Idle",
    retry_annotation = FALSE,

    \* StatefulSet resource
    sts_spec_replicas  = 1,
    sts_ready_replicas = 1,

    \* Webhook state
    webhook_available = TRUE,

    \* Reconciler serialization (controller-runtime work queue)
    reconcile_running = FALSE,

    \* Auxiliary: snapshot of status_previous at PreScaling entry (for S9)
    aux_previous_at_entry = NULL,

    \* Auxiliary: last spec_replicas value observed by reconciler (for S3)
    reconciler_observed_spec = 1;

define
    \* --- Derived state helpers ---

    Stages == {"Idle", "PreScaling", "Scaling", "PostScaling", "FailedAtPre", "FailedAtPost"}
    ActiveStages == {"PreScaling", "Scaling", "PostScaling"}
    FailedStages == {"FailedAtPre", "FailedAtPost"}

    StatefulsetStable(sts_ready, sts_spec, desired) ==
        sts_ready = sts_spec /\ (sts_spec > 0 \/ desired = 0)

    SpecWriteAllowed ==
        status_stage = "Idle" \/ status_stage \in FailedStages

    \* --- Safety Invariants ---

    \* S1: All replica counts non-negative
    S1 == spec_replicas >= 0 /\ status_replicas >= 0 /\ sts_ready_replicas >= 0

    \* S2: desired is NULL or non-negative
    S2 == status_desired = NULL \/ status_desired >= 0

    \* S3: Idle quiescence — when Idle and desired cleared, status matches
    \*     the last spec value the reconciler observed. This is weaker than
    \*     "status = spec" because the HPA can write spec between reconcile
    \*     iterations, creating a transient divergence.
    S3 == (status_stage = "Idle" /\ status_desired = NULL)
          => (status_replicas = reconciler_observed_spec)

    \* S4: Active stages have desired set
    S4 == status_stage \in ActiveStages => status_desired /= NULL

    \* S5: Idle has desired cleared
    S5 == status_stage = "Idle" => status_desired = NULL

    \* S6: Scaling implies status_replicas updated to desired
    S6 == status_stage = "Scaling" => status_replicas = status_desired

    \* S7: PostScaling implies status_replicas == desired
    S7 == status_stage = "PostScaling" => status_replicas = status_desired

    \* S8: Active stages have previous_replicas set
    S8 == status_stage \in ActiveStages => status_previous /= NULL

    \* S9: previous_replicas frozen during scaling operation (uses auxiliary)
    S9 == status_stage \in ActiveStages => status_previous = aux_previous_at_entry

    \* S10: Idle has previous cleared
    S10 == status_stage = "Idle" => status_previous = NULL

    \* S11: TOCTOU detection (EXPECTED to find counterexample)
    \* If this holds, webhook protection is airtight. If violated, the TOCTOU race is real.
    S11_TOCTOU == status_stage \in ActiveStages => spec_replicas = status_desired

    \* S16: Scale-to-zero guard
    S16 == (sts_ready_replicas = 0 /\ sts_spec_replicas = 0 /\ status_stage = "Scaling")
           => status_desired = 0

    \* S17-S18: Bounds
    S17 == status_replicas >= 0 /\ status_replicas <= MAX_REPLICAS
    S18 == sts_ready_replicas >= 0 /\ sts_ready_replicas <= MAX_REPLICAS

    \* Composite safety (excludes S11 which is tested separately)
    SafetyInvariant == S1 /\ S2 /\ S3 /\ S4 /\ S5 /\ S6 /\ S7
                       /\ S8 /\ S9 /\ S10 /\ S16 /\ S17 /\ S18
end define;

\* Processes will be added in subsequent tasks.
\* For now, a minimal single-step process to allow TLC compilation.

process Placeholder = "placeholder"
begin
    Skip:
        skip;
end process;

end algorithm; *)

\* BEGIN TRANSLATION - generated by TLA+ tools
\* (This section will be auto-generated by the PlusCal translator)
\* END TRANSLATION

=============================================================================
```

- [ ] **Step 2: Translate PlusCal and verify compilation**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal StackableScalerFramework.tla
```

Expected: PlusCal translator produces the TLA+ translation between the `BEGIN TRANSLATION` and `END TRANSLATION` markers. No errors.

- [ ] **Step 3: Run TLC with minimal config to verify module loads**

Create a temporary minimal `StackableScalerFramework.cfg`:

```
CONSTANTS
    MAX_REPLICAS = 3
    NULL = -1

INVARIANT
    SafetyInvariant
```

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg StackableScalerFramework.tla
```

Expected: TLC runs, explores a small number of states (just the Placeholder process), and reports "Model checking completed. No error has been found." This verifies the module, constants, and invariant definitions are syntactically correct.

- [ ] **Step 4: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla proof/tla/StackableScalerFramework.cfg
git commit -m "feat: add framework spec skeleton with variables, init, and safety invariants"
```

---

### Task 3: Framework Spec — Reconciler Process

**Files:**
- Modify: `proof/tla/StackableScalerFramework.tla` (replace Placeholder process)

The Reconciler is the most complex process. It models the full scaler state machine with split reads, stage dispatch, hook nondeterminism, and on_failure atomicity.

- [ ] **Step 1: Replace the Placeholder process with the Reconciler**

Remove the `process Placeholder` block. Add the full Reconciler process:

```tla
process Reconciler = "reconciler"
variables
    local_stage = "Idle",
    local_spec = 0,
    local_status = 0,
    local_desired = NULL,
    local_previous = NULL,
    local_sts_ready = 0,
    local_sts_spec = 0,
    local_retry = FALSE,
    hook_outcome = "Done";
begin
    ReconcileLoop:
    while TRUE do
        \* Acquire serialization lock
        AcquireLock:
            await ~reconcile_running;
            reconcile_running := TRUE;

        \* Read STS (from server-side apply response — fresh)
        ReadSTS:
            local_sts_ready := sts_ready_replicas;
            local_sts_spec  := sts_spec_replicas;

        \* Read Scaler (live API list call — fresh, but interleaving possible since ReadSTS)
        ReadScaler:
            local_stage    := status_stage;
            local_spec     := spec_replicas;
            local_status   := status_replicas;
            local_desired  := status_desired;
            local_previous := status_previous;
            local_retry    := retry_annotation;
            reconciler_observed_spec := spec_replicas;

        \* Dispatch based on stage
        CheckStage:
            if local_stage \in FailedStages then
                goto HandleFailed;
            elsif local_stage = "Idle" then
                goto HandleIdle;
            elsif local_stage = "PreScaling" then
                goto HandlePreScaling;
            elsif local_stage = "Scaling" then
                goto HandleScaling;
            elsif local_stage = "PostScaling" then
                goto HandlePostScaling;
            end if;

        \* --- Idle ---
        HandleIdle:
            if local_status = local_spec then
                \* No-op: await external change
                goto ReleaseLock;
            else
                \* Transition to PreScaling
                status_stage    := "PreScaling";
                status_desired  := local_spec;
                status_previous := local_status;
                aux_previous_at_entry := local_status;
                goto ReleaseLock;
            end if;

        \* --- PreScaling ---
        HandlePreScaling:
            \* Hook outcome chosen nondeterministically
            with outcome \in {"Done", "InProgress", "Error"} do
                hook_outcome := outcome;
            end with;

        PreScalingAct:
            if hook_outcome = "Done" then
                \* CRITICAL: update status_replicas NOW (only place this happens).
                \* Uses local_desired (read from status_desired in ReadScaler).
                \* Since only the Reconciler writes status_desired (in HandleIdle of
                \* a previous iteration), and reconciles are serialized by the mutex,
                \* local_desired == status_desired is guaranteed here. S6 checks this.
                status_stage    := "Scaling";
                status_replicas := local_desired;
                goto ReleaseLock;
            elsif hook_outcome = "InProgress" then
                \* Requeue — stage unchanged
                goto ReleaseLock;
            else
                \* Error: write Failed BEFORE on_failure (atomicity invariant)
                status_stage := "FailedAtPre";
                goto HandleOnFailure;
            end if;

        \* --- Scaling ---
        HandleScaling:
            if StatefulsetStable(local_sts_ready, local_sts_spec, local_desired) then
                status_stage := "PostScaling";
            end if;
            \* else: requeue, no state change
            goto ReleaseLock;

        \* --- PostScaling ---
        HandlePostScaling:
            with outcome \in {"Done", "InProgress", "Error"} do
                hook_outcome := outcome;
            end with;

        PostScalingAct:
            if hook_outcome = "Done" then
                status_stage    := "Idle";
                status_desired  := NULL;
                status_previous := NULL;
                goto ReleaseLock;
            elsif hook_outcome = "InProgress" then
                goto ReleaseLock;
            else
                status_stage := "FailedAtPost";
                goto HandleOnFailure;
            end if;

        \* --- Failed ---
        HandleFailed:
            if local_retry then
                retry_annotation := FALSE;
                status_stage     := "Idle";
                status_desired   := NULL;
                status_previous  := NULL;
            end if;
            \* else: no-op, stay failed
            goto ReleaseLock;

        \* --- on_failure (runs AFTER status already written to Failed) ---
        HandleOnFailure:
            \* on_failure outcome does not change stage (status already Failed)
            \* This is a separate label to model the atomicity gap
            skip;
            goto ReleaseLock;

        \* Release serialization lock
        ReleaseLock:
            reconcile_running := FALSE;
    end while;
end process;
```

- [ ] **Step 2: Translate and compile**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal StackableScalerFramework.tla
```

Expected: No translation errors. The `BEGIN TRANSLATION` section is populated.

- [ ] **Step 3: Run TLC to verify invariants with Reconciler only**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg StackableScalerFramework.tla
```

Expected: TLC reports no invariant violations. The Reconciler alone (no HPA writes) should satisfy all safety properties since `spec_replicas` never changes from 1.

Note: `CHECK_DEADLOCKS FALSE` in the `.cfg` file prevents TLC from treating terminal states as errors. All looping processes prevent actual deadlocks, but the Placeholder may terminate.

- [ ] **Step 4: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla
git commit -m "feat: add Reconciler process to framework spec"
```

---

### Task 4: Framework Spec — Remaining Processes

**Files:**
- Modify: `proof/tla/StackableScalerFramework.tla` (add 5 processes before `end algorithm`)

- [ ] **Step 1: Add HPAWriter process**

Insert before `end algorithm;`:

```tla
process HPAWriter = "hpa"
variables
    hpa_target = 0,
    webhook_response = "Deny";
begin
    HPALoop:
    while TRUE do
        SelectTarget:
            with t \in 0..MAX_REPLICAS do
                hpa_target := t;
            end with;

        WebhookCheck:
            if ~webhook_available then
                webhook_response := "Deny";
            elsif status_stage \in ActiveStages then
                webhook_response := "Deny";
            else
                webhook_response := "Allow";
            end if;

        APIServerApply:
            if webhook_response = "Allow" /\ hpa_target /= spec_replicas then
                spec_replicas := hpa_target;
            end if;
    end while;
end process;
```

- [ ] **Step 2: Add ProductOperator process**

```tla
process ProductOperator = "product_op"
begin
    ProductOpLoop:
    while TRUE do
        PropagateToSTS:
            sts_spec_replicas := status_replicas;
    end while;
end process;
```

- [ ] **Step 3: Add STSController process**

```tla
process STSController = "sts_ctrl"
begin
    STSLoop:
    while TRUE do
        Progress:
            if sts_ready_replicas < sts_spec_replicas then
                sts_ready_replicas := sts_ready_replicas + 1;
            elsif sts_ready_replicas > sts_spec_replicas then
                sts_ready_replicas := sts_ready_replicas - 1;
            end if;
    end while;
end process;
```

- [ ] **Step 4: Add RetryAnnotator process**

```tla
process RetryAnnotator = "retry"
begin
    RetryLoop:
    while TRUE do
        ApplyRetry:
            if status_stage \in FailedStages then
                retry_annotation := TRUE;
            end if;
    end while;
end process;
```

- [ ] **Step 5: Add WebhookLifecycle process**

```tla
process WebhookLifecycle = "webhook_lc"
begin
    WebhookLoop:
    while TRUE do
        Toggle:
            with avail \in {TRUE, FALSE} do
                webhook_available := avail;
            end with;
    end while;
end process;
```

- [ ] **Step 6: Translate and compile**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal StackableScalerFramework.tla
```

Expected: No errors. All 6 processes translated.

- [ ] **Step 7: Run TLC — initial full model check**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg -workers auto StackableScalerFramework.tla 2>&1 | tail -30
```

Expected outcomes:
- **SafetyInvariant passes** (S1-S10, S16-S18 all hold) — OR — TLC finds a counterexample, which means we have a real bug in the model or the implementation.
- **S3 may fail** — if the HPA writes between `ReleaseLock` and the next `AcquireLock`, the guard `~reconcile_running` is FALSE but the reconciler hasn't observed the new spec yet. If this happens, weaken S3 by adding an auxiliary variable `reconciler_observed_spec` that tracks the last `spec_replicas` value the reconciler read. Replace S3 with: `status_stage = "Idle" /\ status_desired = NULL => status_replicas = reconciler_observed_spec`.
- Run time: may take minutes with 6 processes and MAX_REPLICAS=3. If >10 minutes, reduce to MAX_REPLICAS=2 for iterating and use 3 for final verification.

Analyze any counterexamples carefully. A counterexample trace shows the exact interleaving that violates the property — this is the primary value of model checking.

- [ ] **Step 8: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla
git commit -m "feat: add all framework processes (HPAWriter, ProductOp, STS, Retry, Webhook)"
```

---

### Task 5: Framework Spec — S12-S15 Label-Based Invariants

**Files:**
- Modify: `proof/tla/StackableScalerFramework.tla` (add to `define` block)

S12-S15 check properties that depend on which label the Reconciler is currently at. In TLA+, the PlusCal translator generates a `pc` variable that tracks the current program counter for each process.

- [ ] **Step 1: Add label-based invariants to the define block**

Add after the `SafetyInvariant` definition:

```tla
    \* S12: on_failure label implies status already Failed
    S12 == pc["reconciler"] = "HandleOnFailure"
           => status_stage \in FailedStages

    \* S13: PreScaling hook only called in PreScaling stage
    \* (checks that when at HandlePreScaling, the global stage is PreScaling)
    S13 == pc["reconciler"] = "HandlePreScaling"
           => status_stage = "PreScaling"

    \* S14: PostScaling hook only called in PostScaling stage
    S14 == pc["reconciler"] = "HandlePostScaling"
           => status_stage = "PostScaling"

    \* S15: Transition validity — stage changes only along valid edges.
    \* Verified structurally by PlusCal code. As an additional check,
    \* we verify no direct Idle→Scaling, Idle→PostScaling, etc. transitions.
    \* This is encoded as: if stage is X, the next stage write must be Y.
    \* (Verified by the composite invariant; each stage handler only writes
    \* valid successors. TLC will catch any structural error.)

    \* Combined label-based invariant
    LabelInvariant == S12 /\ S13 /\ S14
```

- [ ] **Step 2: Update SafetyInvariant to include label-based checks**

Update the `SafetyInvariant` definition:

```tla
    SafetyInvariant == S1 /\ S2 /\ S3 /\ S4 /\ S5 /\ S6 /\ S7
                       /\ S8 /\ S9 /\ S10 /\ S16 /\ S17 /\ S18
                       /\ S12 /\ S13 /\ S14
```

- [ ] **Step 3: Translate and run TLC**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal StackableScalerFramework.tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg -workers auto StackableScalerFramework.tla 2>&1 | tail -30
```

Expected: All invariants pass. S12-S14 verify the atomicity and ordering guarantees.

Note: S13 and S14 check that the GLOBAL `status_stage` matches when the reconciler is at the hook label. Since other processes can modify `status_stage` between the reconciler's read and the hook label... actually, only the reconciler writes `status_stage`. So this should hold. If TLC finds a counterexample, it means the model has a bug (another process writing `status_stage`).

- [ ] **Step 4: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla
git commit -m "feat: add label-based safety invariants S12-S14"
```

---

### Task 6: Framework Spec — Temporal Properties

**Files:**
- Modify: `proof/tla/StackableScalerFramework.tla` (add temporal formulas after `END TRANSLATION`)

Temporal properties (liveness, action properties) are defined as TLA+ formulas OUTSIDE the PlusCal algorithm block, after the `END TRANSLATION` marker.

- [ ] **Step 1: Add action properties A1-A2**

After the `END TRANSLATION` comment and before the `=====` footer, add:

```tla
\* --- vars tuple for temporal formulas ---
\* Note: PlusCal translator generates a `vars` tuple, but it may use a different name.
\* If the translator generates `vars` already, remove this definition.
\* If it uses a different name, rename references below accordingly.
\* Verify after translation by searching for "vars ==" in the generated TLA+.

\* --- Action Properties ---

\* A1: Webhook unavailable => spec_replicas unchanged
A1 == [][~webhook_available => spec_replicas' = spec_replicas]_vars

\* A2: Every Idle->PreScaling transition sets previous_replicas
A2 == [][(status_stage = "Idle" /\ status_stage' = "PreScaling")
         => status_previous' /= NULL]_vars
```

- [ ] **Step 2: Add liveness properties L1-L6**

```tla
\* --- Liveness Properties ---
\* These require fairness constraints (specified in .cfg file)

\* L1: Every scaling operation eventually reaches Idle or Failed
L1 == [](status_stage \in ActiveStages
         ~> (status_stage = "Idle" \/ status_stage \in FailedStages))

\* L2: Failed + retry annotation eventually leaves Failed
L2 == []((status_stage \in FailedStages /\ retry_annotation)
         ~> status_stage = "Idle")

\* L3: PreScaling eventually exits
L3 == [](status_stage = "PreScaling" ~> status_stage /= "PreScaling")

\* L4: Scaling eventually exits
L4 == [](status_stage = "Scaling" ~> status_stage /= "Scaling")

\* L5: PostScaling eventually exits
L5 == [](status_stage = "PostScaling" ~> status_stage /= "PostScaling")

\* L6: End-to-end: Idle with mismatch eventually reaches Idle with match
\* (Conditional on hooks succeeding and STS converging)
L6 == []((status_stage = "Idle" /\ spec_replicas /= status_replicas)
         ~> (status_stage = "Idle" /\ spec_replicas = status_replicas))
```

- [ ] **Step 3: Add no-missed-operations properties L7-L9**

```tla
\* L7: Every Idle->PreScaling is followed by pre_scale invocation
\* Encoded: PreScaling is eventually followed by HandlePreScaling label
L7 == [](status_stage = "PreScaling"
         ~> (pc["reconciler"] = "HandlePreScaling"
             \/ status_stage /= "PreScaling"))

\* L8: Every PostScaling is followed by post_scale invocation
L8 == [](status_stage = "PostScaling"
         ~> (pc["reconciler"] = "HandlePostScaling"
             \/ status_stage /= "PostScaling"))

\* L9: Mismatch in Idle eventually leads to PreScaling
L9 == []((status_stage = "Idle" /\ spec_replicas /= status_replicas)
         ~> (status_stage = "PreScaling"
             \/ spec_replicas = status_replicas))

\* L10: HPA can eventually write when Idle and webhook available
L10 == []((webhook_available /\ status_stage = "Idle")
          ~> (spec_replicas /= status_replicas
              \/ status_stage /= "Idle"))
```

- [ ] **Step 4: Add S11 TOCTOU detection as a separate named property**

```tla
\* S11: TOCTOU detection — check if webhook protection can be bypassed
\* Run as a separate invariant to get the counterexample trace
TOCTOU_Detection == S11_TOCTOU
```

- [ ] **Step 5: Translate (temporal formulas don't need PlusCal translation)**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal StackableScalerFramework.tla
```

No change expected in the TRANSLATION section — temporal formulas are pure TLA+.

- [ ] **Step 6: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla
git commit -m "feat: add temporal properties (action, liveness, TOCTOU detection)"
```

---

### Task 7: Framework TLC Configuration

**Files:**
- Modify: `proof/tla/StackableScalerFramework.cfg`

- [ ] **Step 1: Write the full TLC config**

Replace `proof/tla/StackableScalerFramework.cfg`:

```
\* StackableScaler Framework — TLC Configuration

CONSTANTS
    MAX_REPLICAS = 3
    NULL = -1

\* Safety invariants (checked in every state)
INVARIANT
    SafetyInvariant

\* Temporal/liveness properties (checked over behaviors)
\* Note: liveness checking requires fairness; enable with PROPERTIES.
\* Start with safety only. Uncomment PROPERTIES for liveness runs.
\* Liveness runs are significantly slower.

\* PROPERTY
\*     A1
\*     A2
\*     L1
\*     L2
\*     L3
\*     L4
\*     L5
\*     L6
\*     L7
\*     L8
\*     L9
\*     L10

CHECK_DEADLOCKS FALSE
```

- [ ] **Step 2: Run safety-only model check**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg -workers auto StackableScalerFramework.tla 2>&1 | tail -30
```

Expected: "Model checking completed. No error has been found." Report the number of distinct states explored and wall-clock time.

If invariant violations are found, analyze the counterexample trace. Common issues:
- S3 violated: HPA wrote between ReleaseLock and next AcquireLock → weaken S3
- S6/S7 violated: status_replicas not updated correctly at PreScaling→Scaling → fix Reconciler
- S9 violated: aux_previous_at_entry not tracking correctly → fix auxiliary update

- [ ] **Step 3: Create TOCTOU detection config and run**

Create `proof/tla/TOCTOU.cfg`:

```
CONSTANTS
    MAX_REPLICAS = 2
    NULL = -1

INVARIANT
    TOCTOU_Detection

CHECK_DEADLOCKS FALSE
```

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config TOCTOU.cfg -workers auto StackableScalerFramework.tla 2>&1 | tail -50
```

Expected: TLC finds a counterexample trace showing the TOCTOU race:
1. System in Idle, spec=1, status=1
2. HPA enters SelectTarget, picks target=2
3. WebhookCheck reads stage=Idle → Allow
4. Reconciler AcquireLock, ReadSTS, ReadScaler (spec still 1), HandleIdle — wait, spec is still 1 here...

Actually, the race requires: HPA's WebhookCheck sees Idle, then Reconciler transitions to PreScaling, then HPA's APIServerApply writes. This means:
1. HPA: SelectTarget(2), WebhookCheck sees Idle → Allow
2. Reconciler: some other event triggers PreScaling
3. HPA: APIServerApply writes spec=2

But wait — for the Reconciler to transition to PreScaling, spec must already differ from status. The HPA hasn't written yet. So the race requires a PRIOR HPA write or another source of spec change.

The actual TOCTOU trace would be:
1. HPA writes spec=2 (first write, allowed)
2. Reconciler transitions to PreScaling (desired=2)
3. Another HPA: SelectTarget(3), WebhookCheck sees PreScaling → Deny
4. Reconciler completes scaling to Idle
5. Another HPA: SelectTarget(3), WebhookCheck sees Idle → Allow
6. Reconciler transitions to PreScaling (desired=3)... this is normal operation.

The true TOCTOU requires interleaving within a SINGLE HPA write cycle:
1. Reconciler is in Idle, spec=status=1
2. HPA1: SelectTarget(2), WebhookCheck sees Idle → Allow
3. HPA1 hasn't applied yet (between WebhookCheck and APIServerApply)
4. Meanwhile, HPA2 writes spec=3 (or the reconciler observes a previous write)
5. Reconciler transitions to PreScaling(desired=3)
6. HPA1: APIServerApply writes spec=2 (overwrites 3!)

With only one HPAWriter process, the race may not manifest. Consider adding a second HPAWriter instance to model concurrent HPA writes if the single-instance check finds no violation.

Save the TOCTOU counterexample trace (or the absence of one) for the spec review.

- [ ] **Step 4: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.cfg proof/tla/TOCTOU.cfg
git commit -m "feat: add TLC configs for framework spec and TOCTOU detection"
```

---

### Task 8: Debug and Stabilize Framework Spec

**Files:**
- Modify: `proof/tla/StackableScalerFramework.tla` (as needed for fixes)
- Modify: `proof/tla/StackableScalerFramework.cfg` (as needed)

This task is iterative. Work through any TLC errors or counterexamples from Task 7.

- [ ] **Step 1: Verify S3 passes with the pre-fixed auxiliary variable**

S3 was pre-weakened in Task 2 to use `reconciler_observed_spec` (the last `spec_replicas` value the reconciler read) instead of the current `spec_replicas`. This should prevent the known false positive where HPA writes between reconcile iterations. If S3 still fails, the auxiliary variable update in `ReadScaler` may be in the wrong place — verify it's set in the same atomic step as the other reads.

- [ ] **Step 2: Address any other counterexamples**

For each counterexample:
1. Read the trace carefully — it shows the exact interleaving
2. Determine if it's a model bug or a real implementation concern
3. If model bug: fix the PlusCal code
4. If real concern: document it and decide whether to weaken the property or flag as a finding

- [ ] **Step 3: Enable liveness checking**

Once safety passes, uncomment the `PROPERTY` section in `StackableScalerFramework.cfg`. Liveness checking requires fairness. Add to the .cfg:

```
PROPERTY
    L1
    L3
    L4
    L5
```

Start with L1, L3-L5 (the unconditional/structural ones). L6, L9, L10 may require fairness constraints that need careful specification.

**Important:** Liveness properties L3-L5 will FAIL if hooks can return `InProgress` forever. The model's nondeterministic hook outcome includes `InProgress` as an option every time, so without fairness constraints, TLC can construct an infinite trace where the hook always returns `InProgress`. To fix:

Option A: Add weak fairness on the hook outcome reaching `Done` or `Error` — but this is an environment assumption, not a system guarantee.

Option B: Express L3-L5 as conditional: `[](status_stage = "PreScaling" /\ <>(\E outcome \in {"Done", "Error"}: hook_outcome = outcome)) ~> status_stage /= "PreScaling")`

Option C: For initial verification, only check L1/L2 with weak fairness on all processes, and check L3-L5 via simulation (`make simulate`) rather than exhaustive model checking.

Start with Option C for pragmatism. Document the limitation.

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config StackableScalerFramework.cfg -workers auto StackableScalerFramework.tla 2>&1 | tail -50
```

- [ ] **Step 4: Run simulation for conditional liveness**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -simulate -depth 200 -config StackableScalerFramework.cfg StackableScalerFramework.tla 2>&1 | tail -20
```

Simulation runs random traces — useful for finding shallow liveness bugs without exhaustive state exploration.

- [ ] **Step 5: Document findings**

Create a brief findings summary at the top of `StackableScalerFramework.tla` as a comment block:

```tla
\* --- Verification Results ---
\* Safety: [PASS/FAIL] — X distinct states, Y seconds
\* S11 TOCTOU: [counterexample found / no counterexample] — describe trace
\* Liveness: [results]
\* Findings: [any real bugs or design concerns discovered]
```

- [ ] **Step 6: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/StackableScalerFramework.tla proof/tla/StackableScalerFramework.cfg
git commit -m "feat: stabilize framework spec, document verification results"
```

---

## Chunk 2: NiFi Refinement Spec

### Task 9: NiFi Refinement Spec

**Files:**
- Create: `proof/tla/NiFiScalerRefinement.tla`

- [ ] **Step 1: Write the NiFi refinement module**

Create `proof/tla/NiFiScalerRefinement.tla`:

```tla
--------------------------- MODULE NiFiScalerRefinement ---------------------------
EXTENDS Integers, Sequences, TLC, FiniteSets

CONSTANTS MAX_REPLICAS, NULL

\* Node statuses — matches NifiNodeStatus enum in nifi_api.rs
NodeStatuses == {"ABSENT", "CONNECTING", "CONNECTED",
                 "OFFLOADING", "OFFLOADED",
                 "DISCONNECTING", "DISCONNECTED"}

\* Valid transitions for NiFi 1.x
V1Transitions == {
    <<"CONNECTED", "OFFLOADING">>,
    <<"OFFLOADING", "OFFLOADED">>,
    <<"OFFLOADED", "DISCONNECTING">>,
    <<"DISCONNECTING", "DISCONNECTED">>,
    <<"DISCONNECTED", "ABSENT">>    \* DELETE API call
}

\* Valid transitions for NiFi 2.x
V2Transitions == {
    <<"CONNECTED", "DISCONNECTING">>,
    <<"DISCONNECTING", "DISCONNECTED">>,
    <<"DISCONNECTED", "OFFLOADING">>,
    <<"OFFLOADING", "OFFLOADED">>,
    <<"OFFLOADED", "ABSENT">>       \* DELETE API call
}

(* --algorithm NiFiScaler

variables
    \* --- All framework variables (same initial state) ---
    spec_replicas    = 1,
    status_replicas  = 1,
    status_desired   = NULL,
    status_previous  = NULL,
    status_stage     = "Idle",
    retry_annotation = FALSE,
    sts_spec_replicas  = 1,
    sts_ready_replicas = 1,
    webhook_available = TRUE,
    reconcile_running = FALSE,
    aux_previous_at_entry = NULL,

    \* --- NiFi-specific variables ---
    node_status    = [i \in 0..(MAX_REPLICAS-1) |-> "CONNECTED"],
    nifi_version   = 1,              \* 1 or 2
    removal_targets = {},
    api_call_failed = FALSE;

define
    \* Import framework invariant definitions (copy from Framework spec)
    \* ... (all S1-S18 definitions same as StackableScalerFramework)

    Stages == {"Idle", "PreScaling", "Scaling", "PostScaling", "FailedAtPre", "FailedAtPost"}
    ActiveStages == {"PreScaling", "Scaling", "PostScaling"}
    FailedStages == {"FailedAtPre", "FailedAtPost"}

    StatefulsetStable(sts_ready, sts_spec, desired) ==
        sts_ready = sts_spec /\ (sts_spec > 0 \/ desired = 0)

    SpecWriteAllowed ==
        status_stage = "Idle" \/ status_stage \in FailedStages

    \* Compute valid transitions based on version
    ValidTransitions == IF nifi_version = 1 THEN V1Transitions ELSE V2Transitions

    \* Terminal status before DELETE for this version
    PreDeleteStatus == IF nifi_version = 1 THEN "DISCONNECTED" ELSE "OFFLOADED"

    \* Compute hook outcome deterministically from node state
    NiFiPreScaleOutcome ==
        IF removal_targets = {} THEN "Done"
        ELSE IF \A i \in removal_targets: node_status[i] = "ABSENT" THEN "Done"
        ELSE IF api_call_failed THEN "Error"
        ELSE "InProgress"

    \* --- NiFi Safety Invariants ---

    \* N1: Node transitions follow version-specific ordering
    \* (Verified structurally by the NiFiNodeManager process)

    \* N2: Pod-0 never in removal targets
    N2 == 0 \notin removal_targets

    \* N3: Correct ordinals targeted
    N3 == removal_targets /= {}
          => removal_targets = status_desired..(status_previous - 1)

    \* N5: At PreScaling->Scaling, all targets are ABSENT when hook returns Done
    \* Checked as label-based invariant: when reconciler is at PreScalingAct
    \* and hook_outcome is "Done", all removal_targets must be ABSENT.
    \* (Structurally enforced by NiFiPreScaleOutcome, but verified explicitly.)
    N5 == (pc["reconciler"] = "PreScalingAct" /\ hook_outcome = "Done"
           /\ removal_targets /= {})
          => \A i \in removal_targets: node_status[i] = "ABSENT"

    NiFiSafetyInvariant == N2 /\ N3 /\ N5
end define;

\* --- Reconciler (same as framework, but uses NiFiPreScaleOutcome for pre_scale) ---

process Reconciler = "reconciler"
variables
    local_stage = "Idle",
    local_spec = 0,
    local_status = 0,
    local_desired = NULL,
    local_previous = NULL,
    local_sts_ready = 0,
    local_sts_spec = 0,
    local_retry = FALSE,
    hook_outcome = "Done";
begin
    ReconcileLoop:
    while TRUE do
        AcquireLock:
            await ~reconcile_running;
            reconcile_running := TRUE;

        ReadSTS:
            local_sts_ready := sts_ready_replicas;
            local_sts_spec  := sts_spec_replicas;

        ReadScaler:
            local_stage    := status_stage;
            local_spec     := spec_replicas;
            local_status   := status_replicas;
            local_desired  := status_desired;
            local_previous := status_previous;
            local_retry    := retry_annotation;

        CheckStage:
            if local_stage \in FailedStages then
                goto HandleFailed;
            elsif local_stage = "Idle" then
                goto HandleIdle;
            elsif local_stage = "PreScaling" then
                goto HandlePreScaling;
            elsif local_stage = "Scaling" then
                goto HandleScaling;
            elsif local_stage = "PostScaling" then
                goto HandlePostScaling;
            end if;

        HandleIdle:
            if local_status = local_spec then
                goto ReleaseLock;
            else
                status_stage    := "PreScaling";
                status_desired  := local_spec;
                status_previous := local_status;
                aux_previous_at_entry := local_status;
                \* NiFi-specific: set removal targets for scale-down
                if local_spec < local_status then
                    removal_targets := local_spec..(local_status - 1);
                else
                    removal_targets := {};
                end if;
                goto ReleaseLock;
            end if;

        HandlePreScaling:
            \* NiFi: use deterministic outcome from node state
            \* First, choose whether API call fails this reconcile
            with failed \in {TRUE, FALSE} do
                api_call_failed := failed;
            end with;

        PreScalingCompute:
            hook_outcome := NiFiPreScaleOutcome;

        PreScalingAct:
            if hook_outcome = "Done" then
                status_stage    := "Scaling";
                status_replicas := local_desired;
                goto ReleaseLock;
            elsif hook_outcome = "InProgress" then
                goto ReleaseLock;
            else
                status_stage := "FailedAtPre";
                goto HandleOnFailure;
            end if;

        HandleScaling:
            if StatefulsetStable(local_sts_ready, local_sts_spec, local_desired) then
                status_stage := "PostScaling";
            end if;
            goto ReleaseLock;

        HandlePostScaling:
            \* NiFi: post_scale always returns Done (trait default)
            hook_outcome := "Done";

        PostScalingAct:
            status_stage    := "Idle";
            status_desired  := NULL;
            status_previous := NULL;
            removal_targets := {};
            goto ReleaseLock;

        HandleFailed:
            if local_retry then
                retry_annotation := FALSE;
                status_stage     := "Idle";
                status_desired   := NULL;
                status_previous  := NULL;
                removal_targets  := {};
            end if;
            goto ReleaseLock;

        HandleOnFailure:
            skip;
            goto ReleaseLock;

        ReleaseLock:
            reconcile_running := FALSE;
    end while;
end process;

\* --- NiFi Node Manager: drives node state transitions ---

process NiFiNodeManager = "nifi_mgr"
begin
    NiFiLoop:
    while TRUE do
        DriveTransitions:
            if status_stage = "PreScaling" /\ removal_targets /= {} then
                \* Advance one node by one step
                with node \in removal_targets do
                    with transition \in ValidTransitions do
                        if transition[1] = node_status[node] then
                            node_status[node] := transition[2];
                        end if;
                    end with;
                end with;
            end if;
    end while;
end process;

\* --- Remaining processes identical to framework ---

process HPAWriter = "hpa"
variables hpa_target = 0, webhook_response = "Deny";
begin
    HPALoop:
    while TRUE do
        SelectTarget:
            with t \in 0..MAX_REPLICAS do hpa_target := t; end with;
        WebhookCheck:
            if ~webhook_available then webhook_response := "Deny";
            elsif status_stage \in ActiveStages then webhook_response := "Deny";
            else webhook_response := "Allow"; end if;
        APIServerApply:
            if webhook_response = "Allow" /\ hpa_target /= spec_replicas then
                spec_replicas := hpa_target;
            end if;
    end while;
end process;

process ProductOperator = "product_op"
begin ProductOpLoop: while TRUE do
    PropagateToSTS: sts_spec_replicas := status_replicas;
end while; end process;

process STSController = "sts_ctrl"
begin STSLoop: while TRUE do
    Progress:
        if sts_ready_replicas < sts_spec_replicas then sts_ready_replicas := sts_ready_replicas + 1;
        elsif sts_ready_replicas > sts_spec_replicas then sts_ready_replicas := sts_ready_replicas - 1;
        end if;
end while; end process;

process RetryAnnotator = "retry"
begin RetryLoop: while TRUE do
    ApplyRetry:
        if status_stage \in FailedStages then retry_annotation := TRUE; end if;
end while; end process;

process WebhookLifecycle = "webhook_lc"
begin WebhookLoop: while TRUE do
    Toggle: with avail \in {TRUE, FALSE} do webhook_available := avail; end with;
end while; end process;

end algorithm; *)

\* BEGIN TRANSLATION
\* END TRANSLATION

\* --- Refinement: prove NiFi spec refines the framework spec ---
\* The NiFi spec's hook_outcome is deterministic (from NiFiPreScaleOutcome).
\* The framework spec's hook_outcome is nondeterministic.
\* Every behavior of the NiFi spec must be a behavior of the framework spec.
\* We instantiate the framework spec mapping hook_outcome to NiFiPreScaleOutcome.

Framework == INSTANCE StackableScalerFramework

\* Refinement property: every NiFi behavior satisfies the framework spec
RefinementProperty == Framework!Spec

\* --- NiFi Action Properties ---

\* N1: Node transitions follow version-specific ordering
N1_Action == [][\A i \in 0..(MAX_REPLICAS-1):
    node_status[i] /= node_status'[i] =>
    <<node_status[i], node_status'[i]>> \in ValidTransitions]_vars

\* N4: No DELETE (transition to ABSENT) while CONNECTED
N4_Action == [][\A i \in 0..(MAX_REPLICAS-1):
    node_status[i] = "CONNECTED" => node_status'[i] /= "ABSENT"]_vars

\* --- NiFi Liveness Properties ---

\* NL1: Every OFFLOADING node eventually reaches OFFLOADED
\* (Conditional on NiFi API being responsive — requires fairness on NiFiNodeManager)
NL1 == [](\A i \in 0..(MAX_REPLICAS-1):
    node_status[i] = "OFFLOADING"
    ~> (node_status[i] = "OFFLOADED" \/ node_status[i] = "ABSENT"))

\* NL2: Scale-down eventually removes all targeted nodes
\* (Conditional on NiFi API being responsive)
NL2 == [](removal_targets /= {}
         ~> \A i \in removal_targets: node_status[i] = "ABSENT")

=============================================================================
```

- [ ] **Step 2: Translate and compile**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -pcal NiFiScalerRefinement.tla
```

Expected: No errors. All processes translated.

- [ ] **Step 3: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/NiFiScalerRefinement.tla
git commit -m "feat: add NiFi refinement spec with node lifecycle"
```

---

### Task 10: NiFi TLC Config and Verification

**Files:**
- Create: `proof/tla/NiFiScalerRefinement.cfg`

- [ ] **Step 1: Write the NiFi TLC config**

Create `proof/tla/NiFiScalerRefinement.cfg`:

```
\* NiFi Scaler Refinement — TLC Configuration

CONSTANTS
    MAX_REPLICAS = 2
    NULL = -1

INVARIANT
    NiFiSafetyInvariant

PROPERTY
    N1_Action
    N4_Action
    \* RefinementProperty    \* Uncomment for refinement checking (slower)
    \* NL1                   \* Uncomment for liveness (requires fairness)
    \* NL2                   \* Uncomment for liveness (requires fairness)

CHECK_DEADLOCKS FALSE
```

Note: `MAX_REPLICAS = 2` for the NiFi spec because the per-node state array significantly increases the state space. With 7 possible statuses per node and 2 nodes, that's 7^2 = 49 additional states per framework state.

**Important**: `RefinementProperty` requires that `StackableScalerFramework.tla` is in the same directory. The `INSTANCE` declaration will resolve it automatically. For refinement checking, the framework spec's `Spec` operator (Init /\ [][Next]_vars with fairness) must be defined — verify it exists in the PlusCal translator output.

- [ ] **Step 2: Run safety check**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config NiFiScalerRefinement.cfg -workers auto NiFiScalerRefinement.tla 2>&1 | tail -30
```

Expected: NiFiSafetyInvariant (N2 /\ N3 /\ N5) passes. Report state count.

If N2 fails (ordinal 0 in removal targets), the `removal_targets` computation in HandleIdle is wrong. Scaling from 1→0 would include ordinal 0. This is correct behavior for scale-to-zero (ordinal 0 IS removed). N2 needs to be conditional: `status_desired > 0 => 0 \notin removal_targets`. Fix the `define` block accordingly.

If N3 fails, the `removal_targets` set computation doesn't match `status_desired..(status_previous - 1)`. Check PlusCal's `..` operator (inclusive both ends in TLA+) and adjust.

If N5 fails, the NiFiPreScaleOutcome returns "Done" before all nodes are ABSENT — fix the outcome computation.

- [ ] **Step 3: Run N1 and N4 action properties**

N1_Action and N4_Action are already in the PROPERTY section. Run:

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
java -jar tla2tools.jar -config NiFiScalerRefinement.cfg -workers auto NiFiScalerRefinement.tla 2>&1 | tail -30
```

Expected: Both N1 (version-specific ordering) and N4 (no premature delete) pass — the NiFiNodeManager only transitions along valid edges defined in `ValidTransitions`, so CONNECTED→ABSENT never happens and no steps are skipped.

- [ ] **Step 4: Debug and fix any issues**

Common issues:
- `removal_targets` range computation may need adjustment for PlusCal's `..` operator (inclusive both ends in TLA+)
- `node_status` array indexing — ensure 0-based indexing matches
- `NiFiPreScaleOutcome` may reference stale `removal_targets` if read at wrong time — verify it uses current global state

- [ ] **Step 5: Document NiFi verification results**

Add a comment block at the top of `NiFiScalerRefinement.tla`:

```tla
\* --- NiFi Verification Results ---
\* Safety: [PASS/FAIL] — X distinct states
\* N2 (pod-0 safe): [PASS/conditional]
\* N4 (no premature delete): [PASS/FAIL]
\* Findings: [any issues]
```

- [ ] **Step 6: Commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/NiFiScalerRefinement.tla proof/tla/NiFiScalerRefinement.cfg
git commit -m "feat: verify NiFi refinement spec, document results"
```

---

### Task 11: Final Cleanup and Documentation

**Files:**
- Modify: `proof/tla/Makefile` (add TOCTOU target)

- [ ] **Step 1: Run full verification suite**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
make check-all 2>&1 | tail -10
```

Both should pass.

- [ ] **Step 2: Run TOCTOU detection**

```bash
cd /home/sliebau/IdeaProjects/autoscale/proof/tla
make check-toctou 2>&1 | tail -30
```

Document whether a counterexample was found and what it means.

- [ ] **Step 3: Final commit**

```bash
cd /home/sliebau/IdeaProjects/autoscale
git add proof/tla/
git commit -m "feat: complete formal verification suite with TOCTOU detection"
```
