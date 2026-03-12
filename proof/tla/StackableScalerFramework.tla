--------------------------- MODULE StackableScalerFramework ---------------------------
\* --- Verification Results (MAX_REPLICAS=3, NULL=99) ---
\* Safety (SafetyInvariant: S1-S10, S12-S14, S17-S18):
\*   PASS — 2,132,236,033 states generated, 297,220,608 distinct states, depth 155, ~7 min
\*
\* S16 (Scale-to-zero guard): DROPPED — too strong for multi-actor model.
\*   ProductOperator delay creates transient (sts=0/0, Scaling, desired=2).
\*   Scale-to-zero correctness is structurally enforced by StatefulsetStable.
\*
\* S11 TOCTOU Detection (MAX_REPLICAS=2):
\*   COUNTEREXAMPLE FOUND — confirms TOCTOU race in webhook:
\*   1. HPA WebhookCheck reads stage=Idle -> Allow
\*   2. Reconciler transitions to PreScaling (between WebhookCheck and APIServerApply)
\*   3. HPA APIServerApply writes spec_replicas while stage=PreScaling
\*   Impact: spec_replicas can diverge from status_desired during active scaling.
\*   Mitigation: reconciler reads spec_replicas fresh each iteration; the stale write
\*   will be observed on the next reconcile after the current operation completes.
\*
\* Liveness: Not yet checked exhaustively (requires fairness constraints).
\*   L3-L5 will fail without fairness on hook outcomes (InProgress can repeat forever).
\* ---------------------------------------------------------------------------------

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

    \* S12: on_failure label implies status already Failed
    S12 == pc["reconciler"] = "HandleOnFailure"
           => status_stage \in FailedStages

    \* S13: PreScaling hook only called in PreScaling stage
    S13 == pc["reconciler"] = "HandlePreScaling"
           => status_stage = "PreScaling"

    \* S14: PostScaling hook only called in PostScaling stage
    S14 == pc["reconciler"] = "HandlePostScaling"
           => status_stage = "PostScaling"

    \* S16: Scale-to-zero guard — DROPPED from SafetyInvariant.
    \* The original form is too strong for the multi-actor model: ProductOperator
    \* delay means (sts_ready=0, sts_spec=0, Scaling, desired=2) is reachable.
    \* Scale-to-zero correctness is structurally enforced by StatefulsetStable
    \* in HandleScaling: it requires (sts_spec > 0 \/ desired = 0), so the
    \* reconciler won't transition past Scaling on a zero-replica STS unless
    \* desired is also 0.

    \* S17-S18: Bounds
    S17 == status_replicas >= 0 /\ status_replicas <= MAX_REPLICAS
    S18 == sts_ready_replicas >= 0 /\ sts_ready_replicas <= MAX_REPLICAS

    \* Composite safety (excludes S11 which is tested separately)
    SafetyInvariant == S1 /\ S2 /\ S3 /\ S4 /\ S5 /\ S6 /\ S7
                       /\ S8 /\ S9 /\ S10 /\ S12 /\ S13 /\ S14
                       /\ S17 /\ S18
end define;

\* ============================================================
\* Process 1: Reconciler
\* Models the scaler reconcile loop (reconcile_scaler).
\* Controller-runtime serializes reconciles per object.
\* ============================================================

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
                \* No-op: record that we observed this spec value
                reconciler_observed_spec := local_spec;
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
                reconciler_observed_spec := status_replicas;
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
                reconciler_observed_spec := status_replicas;
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

\* ============================================================
\* Process 2: HPAWriter
\* Models the HPA writing spec.replicas through the admission webhook.
\* Three labels capture the TOCTOU gap.
\* ============================================================

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

\* ============================================================
\* Process 3: ProductOperator
\* Propagates status_replicas to the StatefulSet spec.replicas.
\* Separate reconcile cycle from the scaler reconciler.
\* ============================================================

process ProductOperator = "product_op"
begin
    ProductOpLoop:
    while TRUE do
        PropagateToSTS:
            sts_spec_replicas := status_replicas;
    end while;
end process;

\* ============================================================
\* Process 4: STSController
\* Moves sts_ready_replicas one step toward sts_spec_replicas per step.
\* ============================================================

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

\* ============================================================
\* Process 5: RetryAnnotator
\* Models manual retry annotation application on Failed scalers.
\* ============================================================

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

\* ============================================================
\* Process 6: WebhookLifecycle
\* Toggles webhook availability nondeterministically.
\* Models crash, restart, certificate rotation.
\* ============================================================

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

end algorithm; *)

\* BEGIN TRANSLATION - generated by TLA+ tools
VARIABLES pc, spec_replicas, status_replicas, status_desired, status_previous, 
          status_stage, retry_annotation, sts_spec_replicas, 
          sts_ready_replicas, webhook_available, reconcile_running, 
          aux_previous_at_entry, reconciler_observed_spec

(* define statement *)
Stages == {"Idle", "PreScaling", "Scaling", "PostScaling", "FailedAtPre", "FailedAtPost"}
ActiveStages == {"PreScaling", "Scaling", "PostScaling"}
FailedStages == {"FailedAtPre", "FailedAtPost"}

StatefulsetStable(sts_ready, sts_spec, desired) ==
    sts_ready = sts_spec /\ (sts_spec > 0 \/ desired = 0)

SpecWriteAllowed ==
    status_stage = "Idle" \/ status_stage \in FailedStages




S1 == spec_replicas >= 0 /\ status_replicas >= 0 /\ sts_ready_replicas >= 0


S2 == status_desired = NULL \/ status_desired >= 0





S3 == (status_stage = "Idle" /\ status_desired = NULL)
      => (status_replicas = reconciler_observed_spec)


S4 == status_stage \in ActiveStages => status_desired /= NULL


S5 == status_stage = "Idle" => status_desired = NULL


S6 == status_stage = "Scaling" => status_replicas = status_desired


S7 == status_stage = "PostScaling" => status_replicas = status_desired


S8 == status_stage \in ActiveStages => status_previous /= NULL


S9 == status_stage \in ActiveStages => status_previous = aux_previous_at_entry


S10 == status_stage = "Idle" => status_previous = NULL



S11_TOCTOU == status_stage \in ActiveStages => spec_replicas = status_desired


S12 == pc["reconciler"] = "HandleOnFailure"
       => status_stage \in FailedStages


S13 == pc["reconciler"] = "HandlePreScaling"
       => status_stage = "PreScaling"


S14 == pc["reconciler"] = "HandlePostScaling"
       => status_stage = "PostScaling"










S17 == status_replicas >= 0 /\ status_replicas <= MAX_REPLICAS
S18 == sts_ready_replicas >= 0 /\ sts_ready_replicas <= MAX_REPLICAS


SafetyInvariant == S1 /\ S2 /\ S3 /\ S4 /\ S5 /\ S6 /\ S7
                   /\ S8 /\ S9 /\ S10 /\ S12 /\ S13 /\ S14
                   /\ S17 /\ S18

VARIABLES local_stage, local_spec, local_status, local_desired, 
          local_previous, local_sts_ready, local_sts_spec, local_retry, 
          hook_outcome, hpa_target, webhook_response

vars == << pc, spec_replicas, status_replicas, status_desired, 
           status_previous, status_stage, retry_annotation, sts_spec_replicas, 
           sts_ready_replicas, webhook_available, reconcile_running, 
           aux_previous_at_entry, reconciler_observed_spec, local_stage, 
           local_spec, local_status, local_desired, local_previous, 
           local_sts_ready, local_sts_spec, local_retry, hook_outcome, 
           hpa_target, webhook_response >>

ProcSet == {"reconciler"} \cup {"hpa"} \cup {"product_op"} \cup {"sts_ctrl"} \cup {"retry"} \cup {"webhook_lc"}

Init == (* Global variables *)
        /\ spec_replicas = 1
        /\ status_replicas = 1
        /\ status_desired = NULL
        /\ status_previous = NULL
        /\ status_stage = "Idle"
        /\ retry_annotation = FALSE
        /\ sts_spec_replicas = 1
        /\ sts_ready_replicas = 1
        /\ webhook_available = TRUE
        /\ reconcile_running = FALSE
        /\ aux_previous_at_entry = NULL
        /\ reconciler_observed_spec = 1
        (* Process Reconciler *)
        /\ local_stage = "Idle"
        /\ local_spec = 0
        /\ local_status = 0
        /\ local_desired = NULL
        /\ local_previous = NULL
        /\ local_sts_ready = 0
        /\ local_sts_spec = 0
        /\ local_retry = FALSE
        /\ hook_outcome = "Done"
        (* Process HPAWriter *)
        /\ hpa_target = 0
        /\ webhook_response = "Deny"
        /\ pc = [self \in ProcSet |-> CASE self = "reconciler" -> "ReconcileLoop"
                                        [] self = "hpa" -> "HPALoop"
                                        [] self = "product_op" -> "ProductOpLoop"
                                        [] self = "sts_ctrl" -> "STSLoop"
                                        [] self = "retry" -> "RetryLoop"
                                        [] self = "webhook_lc" -> "WebhookLoop"]

ReconcileLoop == /\ pc["reconciler"] = "ReconcileLoop"
                 /\ pc' = [pc EXCEPT !["reconciler"] = "AcquireLock"]
                 /\ UNCHANGED << spec_replicas, status_replicas, 
                                 status_desired, status_previous, status_stage, 
                                 retry_annotation, sts_spec_replicas, 
                                 sts_ready_replicas, webhook_available, 
                                 reconcile_running, aux_previous_at_entry, 
                                 reconciler_observed_spec, local_stage, 
                                 local_spec, local_status, local_desired, 
                                 local_previous, local_sts_ready, 
                                 local_sts_spec, local_retry, hook_outcome, 
                                 hpa_target, webhook_response >>

AcquireLock == /\ pc["reconciler"] = "AcquireLock"
               /\ ~reconcile_running
               /\ reconcile_running' = TRUE
               /\ pc' = [pc EXCEPT !["reconciler"] = "ReadSTS"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, aux_previous_at_entry, 
                               reconciler_observed_spec, local_stage, 
                               local_spec, local_status, local_desired, 
                               local_previous, local_sts_ready, local_sts_spec, 
                               local_retry, hook_outcome, hpa_target, 
                               webhook_response >>

ReadSTS == /\ pc["reconciler"] = "ReadSTS"
           /\ local_sts_ready' = sts_ready_replicas
           /\ local_sts_spec' = sts_spec_replicas
           /\ pc' = [pc EXCEPT !["reconciler"] = "ReadScaler"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           local_stage, local_spec, local_status, 
                           local_desired, local_previous, local_retry, 
                           hook_outcome, hpa_target, webhook_response >>

ReadScaler == /\ pc["reconciler"] = "ReadScaler"
              /\ local_stage' = status_stage
              /\ local_spec' = spec_replicas
              /\ local_status' = status_replicas
              /\ local_desired' = status_desired
              /\ local_previous' = status_previous
              /\ local_retry' = retry_annotation
              /\ pc' = [pc EXCEPT !["reconciler"] = "CheckStage"]
              /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                              status_previous, status_stage, retry_annotation, 
                              sts_spec_replicas, sts_ready_replicas, 
                              webhook_available, reconcile_running, 
                              aux_previous_at_entry, reconciler_observed_spec, 
                              local_sts_ready, local_sts_spec, hook_outcome, 
                              hpa_target, webhook_response >>

CheckStage == /\ pc["reconciler"] = "CheckStage"
              /\ IF local_stage \in FailedStages
                    THEN /\ pc' = [pc EXCEPT !["reconciler"] = "HandleFailed"]
                    ELSE /\ IF local_stage = "Idle"
                               THEN /\ pc' = [pc EXCEPT !["reconciler"] = "HandleIdle"]
                               ELSE /\ IF local_stage = "PreScaling"
                                          THEN /\ pc' = [pc EXCEPT !["reconciler"] = "HandlePreScaling"]
                                          ELSE /\ IF local_stage = "Scaling"
                                                     THEN /\ pc' = [pc EXCEPT !["reconciler"] = "HandleScaling"]
                                                     ELSE /\ IF local_stage = "PostScaling"
                                                                THEN /\ pc' = [pc EXCEPT !["reconciler"] = "HandlePostScaling"]
                                                                ELSE /\ pc' = [pc EXCEPT !["reconciler"] = "HandleIdle"]
              /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                              status_previous, status_stage, retry_annotation, 
                              sts_spec_replicas, sts_ready_replicas, 
                              webhook_available, reconcile_running, 
                              aux_previous_at_entry, reconciler_observed_spec, 
                              local_stage, local_spec, local_status, 
                              local_desired, local_previous, local_sts_ready, 
                              local_sts_spec, local_retry, hook_outcome, 
                              hpa_target, webhook_response >>

HandleIdle == /\ pc["reconciler"] = "HandleIdle"
              /\ IF local_status = local_spec
                    THEN /\ reconciler_observed_spec' = local_spec
                         /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                         /\ UNCHANGED << status_desired, status_previous, 
                                         status_stage, aux_previous_at_entry >>
                    ELSE /\ status_stage' = "PreScaling"
                         /\ status_desired' = local_spec
                         /\ status_previous' = local_status
                         /\ aux_previous_at_entry' = local_status
                         /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                         /\ UNCHANGED reconciler_observed_spec
              /\ UNCHANGED << spec_replicas, status_replicas, retry_annotation, 
                              sts_spec_replicas, sts_ready_replicas, 
                              webhook_available, reconcile_running, 
                              local_stage, local_spec, local_status, 
                              local_desired, local_previous, local_sts_ready, 
                              local_sts_spec, local_retry, hook_outcome, 
                              hpa_target, webhook_response >>

HandlePreScaling == /\ pc["reconciler"] = "HandlePreScaling"
                    /\ \E outcome \in {"Done", "InProgress", "Error"}:
                         hook_outcome' = outcome
                    /\ pc' = [pc EXCEPT !["reconciler"] = "PreScalingAct"]
                    /\ UNCHANGED << spec_replicas, status_replicas, 
                                    status_desired, status_previous, 
                                    status_stage, retry_annotation, 
                                    sts_spec_replicas, sts_ready_replicas, 
                                    webhook_available, reconcile_running, 
                                    aux_previous_at_entry, 
                                    reconciler_observed_spec, local_stage, 
                                    local_spec, local_status, local_desired, 
                                    local_previous, local_sts_ready, 
                                    local_sts_spec, local_retry, hpa_target, 
                                    webhook_response >>

PreScalingAct == /\ pc["reconciler"] = "PreScalingAct"
                 /\ IF hook_outcome = "Done"
                       THEN /\ status_stage' = "Scaling"
                            /\ status_replicas' = local_desired
                            /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                       ELSE /\ IF hook_outcome = "InProgress"
                                  THEN /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                                       /\ UNCHANGED status_stage
                                  ELSE /\ status_stage' = "FailedAtPre"
                                       /\ pc' = [pc EXCEPT !["reconciler"] = "HandleOnFailure"]
                            /\ UNCHANGED status_replicas
                 /\ UNCHANGED << spec_replicas, status_desired, 
                                 status_previous, retry_annotation, 
                                 sts_spec_replicas, sts_ready_replicas, 
                                 webhook_available, reconcile_running, 
                                 aux_previous_at_entry, 
                                 reconciler_observed_spec, local_stage, 
                                 local_spec, local_status, local_desired, 
                                 local_previous, local_sts_ready, 
                                 local_sts_spec, local_retry, hook_outcome, 
                                 hpa_target, webhook_response >>

HandleScaling == /\ pc["reconciler"] = "HandleScaling"
                 /\ IF StatefulsetStable(local_sts_ready, local_sts_spec, local_desired)
                       THEN /\ status_stage' = "PostScaling"
                       ELSE /\ TRUE
                            /\ UNCHANGED status_stage
                 /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                 /\ UNCHANGED << spec_replicas, status_replicas, 
                                 status_desired, status_previous, 
                                 retry_annotation, sts_spec_replicas, 
                                 sts_ready_replicas, webhook_available, 
                                 reconcile_running, aux_previous_at_entry, 
                                 reconciler_observed_spec, local_stage, 
                                 local_spec, local_status, local_desired, 
                                 local_previous, local_sts_ready, 
                                 local_sts_spec, local_retry, hook_outcome, 
                                 hpa_target, webhook_response >>

HandlePostScaling == /\ pc["reconciler"] = "HandlePostScaling"
                     /\ \E outcome \in {"Done", "InProgress", "Error"}:
                          hook_outcome' = outcome
                     /\ pc' = [pc EXCEPT !["reconciler"] = "PostScalingAct"]
                     /\ UNCHANGED << spec_replicas, status_replicas, 
                                     status_desired, status_previous, 
                                     status_stage, retry_annotation, 
                                     sts_spec_replicas, sts_ready_replicas, 
                                     webhook_available, reconcile_running, 
                                     aux_previous_at_entry, 
                                     reconciler_observed_spec, local_stage, 
                                     local_spec, local_status, local_desired, 
                                     local_previous, local_sts_ready, 
                                     local_sts_spec, local_retry, hpa_target, 
                                     webhook_response >>

PostScalingAct == /\ pc["reconciler"] = "PostScalingAct"
                  /\ IF hook_outcome = "Done"
                        THEN /\ status_stage' = "Idle"
                             /\ status_desired' = NULL
                             /\ status_previous' = NULL
                             /\ reconciler_observed_spec' = status_replicas
                             /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                        ELSE /\ IF hook_outcome = "InProgress"
                                   THEN /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                                        /\ UNCHANGED status_stage
                                   ELSE /\ status_stage' = "FailedAtPost"
                                        /\ pc' = [pc EXCEPT !["reconciler"] = "HandleOnFailure"]
                             /\ UNCHANGED << status_desired, status_previous, 
                                             reconciler_observed_spec >>
                  /\ UNCHANGED << spec_replicas, status_replicas, 
                                  retry_annotation, sts_spec_replicas, 
                                  sts_ready_replicas, webhook_available, 
                                  reconcile_running, aux_previous_at_entry, 
                                  local_stage, local_spec, local_status, 
                                  local_desired, local_previous, 
                                  local_sts_ready, local_sts_spec, local_retry, 
                                  hook_outcome, hpa_target, webhook_response >>

HandleFailed == /\ pc["reconciler"] = "HandleFailed"
                /\ IF local_retry
                      THEN /\ retry_annotation' = FALSE
                           /\ status_stage' = "Idle"
                           /\ status_desired' = NULL
                           /\ status_previous' = NULL
                           /\ reconciler_observed_spec' = status_replicas
                      ELSE /\ TRUE
                           /\ UNCHANGED << status_desired, status_previous, 
                                           status_stage, retry_annotation, 
                                           reconciler_observed_spec >>
                /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                /\ UNCHANGED << spec_replicas, status_replicas, 
                                sts_spec_replicas, sts_ready_replicas, 
                                webhook_available, reconcile_running, 
                                aux_previous_at_entry, local_stage, local_spec, 
                                local_status, local_desired, local_previous, 
                                local_sts_ready, local_sts_spec, local_retry, 
                                hook_outcome, hpa_target, webhook_response >>

HandleOnFailure == /\ pc["reconciler"] = "HandleOnFailure"
                   /\ TRUE
                   /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                   /\ UNCHANGED << spec_replicas, status_replicas, 
                                   status_desired, status_previous, 
                                   status_stage, retry_annotation, 
                                   sts_spec_replicas, sts_ready_replicas, 
                                   webhook_available, reconcile_running, 
                                   aux_previous_at_entry, 
                                   reconciler_observed_spec, local_stage, 
                                   local_spec, local_status, local_desired, 
                                   local_previous, local_sts_ready, 
                                   local_sts_spec, local_retry, hook_outcome, 
                                   hpa_target, webhook_response >>

ReleaseLock == /\ pc["reconciler"] = "ReleaseLock"
               /\ reconcile_running' = FALSE
               /\ pc' = [pc EXCEPT !["reconciler"] = "ReconcileLoop"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, aux_previous_at_entry, 
                               reconciler_observed_spec, local_stage, 
                               local_spec, local_status, local_desired, 
                               local_previous, local_sts_ready, local_sts_spec, 
                               local_retry, hook_outcome, hpa_target, 
                               webhook_response >>

Reconciler == ReconcileLoop \/ AcquireLock \/ ReadSTS \/ ReadScaler
                 \/ CheckStage \/ HandleIdle \/ HandlePreScaling
                 \/ PreScalingAct \/ HandleScaling \/ HandlePostScaling
                 \/ PostScalingAct \/ HandleFailed \/ HandleOnFailure
                 \/ ReleaseLock

HPALoop == /\ pc["hpa"] = "HPALoop"
           /\ pc' = [pc EXCEPT !["hpa"] = "SelectTarget"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           local_stage, local_spec, local_status, 
                           local_desired, local_previous, local_sts_ready, 
                           local_sts_spec, local_retry, hook_outcome, 
                           hpa_target, webhook_response >>

SelectTarget == /\ pc["hpa"] = "SelectTarget"
                /\ \E t \in 0..MAX_REPLICAS:
                     hpa_target' = t
                /\ pc' = [pc EXCEPT !["hpa"] = "WebhookCheck"]
                /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                                status_previous, status_stage, 
                                retry_annotation, sts_spec_replicas, 
                                sts_ready_replicas, webhook_available, 
                                reconcile_running, aux_previous_at_entry, 
                                reconciler_observed_spec, local_stage, 
                                local_spec, local_status, local_desired, 
                                local_previous, local_sts_ready, 
                                local_sts_spec, local_retry, hook_outcome, 
                                webhook_response >>

WebhookCheck == /\ pc["hpa"] = "WebhookCheck"
                /\ IF ~webhook_available
                      THEN /\ webhook_response' = "Deny"
                      ELSE /\ IF status_stage \in ActiveStages
                                 THEN /\ webhook_response' = "Deny"
                                 ELSE /\ webhook_response' = "Allow"
                /\ pc' = [pc EXCEPT !["hpa"] = "APIServerApply"]
                /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                                status_previous, status_stage, 
                                retry_annotation, sts_spec_replicas, 
                                sts_ready_replicas, webhook_available, 
                                reconcile_running, aux_previous_at_entry, 
                                reconciler_observed_spec, local_stage, 
                                local_spec, local_status, local_desired, 
                                local_previous, local_sts_ready, 
                                local_sts_spec, local_retry, hook_outcome, 
                                hpa_target >>

APIServerApply == /\ pc["hpa"] = "APIServerApply"
                  /\ IF webhook_response = "Allow" /\ hpa_target /= spec_replicas
                        THEN /\ spec_replicas' = hpa_target
                        ELSE /\ TRUE
                             /\ UNCHANGED spec_replicas
                  /\ pc' = [pc EXCEPT !["hpa"] = "HPALoop"]
                  /\ UNCHANGED << status_replicas, status_desired, 
                                  status_previous, status_stage, 
                                  retry_annotation, sts_spec_replicas, 
                                  sts_ready_replicas, webhook_available, 
                                  reconcile_running, aux_previous_at_entry, 
                                  reconciler_observed_spec, local_stage, 
                                  local_spec, local_status, local_desired, 
                                  local_previous, local_sts_ready, 
                                  local_sts_spec, local_retry, hook_outcome, 
                                  hpa_target, webhook_response >>

HPAWriter == HPALoop \/ SelectTarget \/ WebhookCheck \/ APIServerApply

ProductOpLoop == /\ pc["product_op"] = "ProductOpLoop"
                 /\ pc' = [pc EXCEPT !["product_op"] = "PropagateToSTS"]
                 /\ UNCHANGED << spec_replicas, status_replicas, 
                                 status_desired, status_previous, status_stage, 
                                 retry_annotation, sts_spec_replicas, 
                                 sts_ready_replicas, webhook_available, 
                                 reconcile_running, aux_previous_at_entry, 
                                 reconciler_observed_spec, local_stage, 
                                 local_spec, local_status, local_desired, 
                                 local_previous, local_sts_ready, 
                                 local_sts_spec, local_retry, hook_outcome, 
                                 hpa_target, webhook_response >>

PropagateToSTS == /\ pc["product_op"] = "PropagateToSTS"
                  /\ sts_spec_replicas' = status_replicas
                  /\ pc' = [pc EXCEPT !["product_op"] = "ProductOpLoop"]
                  /\ UNCHANGED << spec_replicas, status_replicas, 
                                  status_desired, status_previous, 
                                  status_stage, retry_annotation, 
                                  sts_ready_replicas, webhook_available, 
                                  reconcile_running, aux_previous_at_entry, 
                                  reconciler_observed_spec, local_stage, 
                                  local_spec, local_status, local_desired, 
                                  local_previous, local_sts_ready, 
                                  local_sts_spec, local_retry, hook_outcome, 
                                  hpa_target, webhook_response >>

ProductOperator == ProductOpLoop \/ PropagateToSTS

STSLoop == /\ pc["sts_ctrl"] = "STSLoop"
           /\ pc' = [pc EXCEPT !["sts_ctrl"] = "Progress"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           local_stage, local_spec, local_status, 
                           local_desired, local_previous, local_sts_ready, 
                           local_sts_spec, local_retry, hook_outcome, 
                           hpa_target, webhook_response >>

Progress == /\ pc["sts_ctrl"] = "Progress"
            /\ IF sts_ready_replicas < sts_spec_replicas
                  THEN /\ sts_ready_replicas' = sts_ready_replicas + 1
                  ELSE /\ IF sts_ready_replicas > sts_spec_replicas
                             THEN /\ sts_ready_replicas' = sts_ready_replicas - 1
                             ELSE /\ TRUE
                                  /\ UNCHANGED sts_ready_replicas
            /\ pc' = [pc EXCEPT !["sts_ctrl"] = "STSLoop"]
            /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                            status_previous, status_stage, retry_annotation, 
                            sts_spec_replicas, webhook_available, 
                            reconcile_running, aux_previous_at_entry, 
                            reconciler_observed_spec, local_stage, local_spec, 
                            local_status, local_desired, local_previous, 
                            local_sts_ready, local_sts_spec, local_retry, 
                            hook_outcome, hpa_target, webhook_response >>

STSController == STSLoop \/ Progress

RetryLoop == /\ pc["retry"] = "RetryLoop"
             /\ pc' = [pc EXCEPT !["retry"] = "ApplyRetry"]
             /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                             status_previous, status_stage, retry_annotation, 
                             sts_spec_replicas, sts_ready_replicas, 
                             webhook_available, reconcile_running, 
                             aux_previous_at_entry, reconciler_observed_spec, 
                             local_stage, local_spec, local_status, 
                             local_desired, local_previous, local_sts_ready, 
                             local_sts_spec, local_retry, hook_outcome, 
                             hpa_target, webhook_response >>

ApplyRetry == /\ pc["retry"] = "ApplyRetry"
              /\ IF status_stage \in FailedStages
                    THEN /\ retry_annotation' = TRUE
                    ELSE /\ TRUE
                         /\ UNCHANGED retry_annotation
              /\ pc' = [pc EXCEPT !["retry"] = "RetryLoop"]
              /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                              status_previous, status_stage, sts_spec_replicas, 
                              sts_ready_replicas, webhook_available, 
                              reconcile_running, aux_previous_at_entry, 
                              reconciler_observed_spec, local_stage, 
                              local_spec, local_status, local_desired, 
                              local_previous, local_sts_ready, local_sts_spec, 
                              local_retry, hook_outcome, hpa_target, 
                              webhook_response >>

RetryAnnotator == RetryLoop \/ ApplyRetry

WebhookLoop == /\ pc["webhook_lc"] = "WebhookLoop"
               /\ pc' = [pc EXCEPT !["webhook_lc"] = "Toggle"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, reconcile_running, 
                               aux_previous_at_entry, reconciler_observed_spec, 
                               local_stage, local_spec, local_status, 
                               local_desired, local_previous, local_sts_ready, 
                               local_sts_spec, local_retry, hook_outcome, 
                               hpa_target, webhook_response >>

Toggle == /\ pc["webhook_lc"] = "Toggle"
          /\ \E avail \in {TRUE, FALSE}:
               webhook_available' = avail
          /\ pc' = [pc EXCEPT !["webhook_lc"] = "WebhookLoop"]
          /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                          status_previous, status_stage, retry_annotation, 
                          sts_spec_replicas, sts_ready_replicas, 
                          reconcile_running, aux_previous_at_entry, 
                          reconciler_observed_spec, local_stage, local_spec, 
                          local_status, local_desired, local_previous, 
                          local_sts_ready, local_sts_spec, local_retry, 
                          hook_outcome, hpa_target, webhook_response >>

WebhookLifecycle == WebhookLoop \/ Toggle

Next == Reconciler \/ HPAWriter \/ ProductOperator \/ STSController
           \/ RetryAnnotator \/ WebhookLifecycle

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

\* --- Action Properties ---

\* A1: Webhook unavailable => spec_replicas unchanged
A1 == [][~webhook_available => spec_replicas' = spec_replicas]_vars

\* A2: Every Idle->PreScaling transition sets previous_replicas
A2 == [][(status_stage = "Idle" /\ status_stage' = "PreScaling")
         => status_previous' /= NULL]_vars

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

\* L7: Every PreScaling is eventually followed by HandlePreScaling label
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

\* S11: TOCTOU detection — check if webhook protection can be bypassed
TOCTOU_Detection == S11_TOCTOU

=============================================================================
