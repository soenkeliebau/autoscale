--------------------------- MODULE NiFiScalerRefinement ---------------------------
\* --- Verification Results (MAX_REPLICAS=2, NULL=99) ---
\* Safety (NiFiSafetyInvariant: N2, N3, N5):
\*   PASS — 7,912,799,234 states generated, 889,789,440 distinct states, depth 189, ~27 min
\*   Covers both NiFi v1.x and v2.x node lifecycle orderings.
\*
\* N2 (Pod-0 protection): pod 0 is never added to removal_targets.
\* N3 (Correct ordinals): removal targets are always the highest-indexed pods.
\* N5 (All targets ABSENT before scaling): PreScaling hook only returns Done
\*     when all removal_targets have node_status = "ABSENT".
\*
\* Liveness (N1_Action, N4_Action): Not yet checked exhaustively (runtime > 30 min).
\* ---------------------------------------------------------------------------------

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
    <<"DISCONNECTED", "ABSENT">>
}

\* Valid transitions for NiFi 2.x
V2Transitions == {
    <<"CONNECTED", "DISCONNECTING">>,
    <<"DISCONNECTING", "DISCONNECTED">>,
    <<"DISCONNECTED", "OFFLOADING">>,
    <<"OFFLOADING", "OFFLOADED">>,
    <<"OFFLOADED", "ABSENT">>
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
    reconciler_observed_spec = 1,

    \* --- NiFi-specific variables ---
    node_status    = [i \in 0..(MAX_REPLICAS-1) |-> "CONNECTED"],
    nifi_version   \in {1, 2},
    removal_targets = {},
    api_call_failed = FALSE;

define
    \* --- Framework helpers (same as StackableScalerFramework) ---
    Stages == {"Idle", "PreScaling", "Scaling", "PostScaling", "FailedAtPre", "FailedAtPost"}
    ActiveStages == {"PreScaling", "Scaling", "PostScaling"}
    FailedStages == {"FailedAtPre", "FailedAtPost"}

    StatefulsetStable(sts_ready, sts_spec, desired) ==
        sts_ready = sts_spec /\ (sts_spec > 0 \/ desired = 0)

    SpecWriteAllowed ==
        status_stage = "Idle" \/ status_stage \in FailedStages

    \* --- NiFi-specific helpers ---
    ValidTransitions == IF nifi_version = 1 THEN V1Transitions ELSE V2Transitions
    PreDeleteStatus == IF nifi_version = 1 THEN "DISCONNECTED" ELSE "OFFLOADED"

    \* Compute hook outcome deterministically from node state
    NiFiPreScaleOutcome ==
        IF removal_targets = {} THEN "Done"
        ELSE IF \A i \in removal_targets: node_status[i] = "ABSENT" THEN "Done"
        ELSE IF api_call_failed THEN "Error"
        ELSE "InProgress"

    \* --- NiFi Safety Invariants ---

    \* N2: Pod-0 never in removal targets (conditional: only when not scaling to 0)
    N2 == status_desired /= NULL /\ status_desired > 0
          => 0 \notin removal_targets

    \* N3: Correct ordinals targeted
    N3 == (removal_targets /= {} /\ status_desired /= NULL /\ status_previous /= NULL)
          => removal_targets = status_desired..(status_previous - 1)

    \* N5: defined after END TRANSLATION (needs process-local hook_outcome)

    NiFiSafetyInvariant_Define == N2 /\ N3
end define;

\* ============================================================
\* Process: Reconciler (NiFi-specific: deterministic hooks)
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
                reconciler_observed_spec := local_spec;
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

        \* --- PreScaling (NiFi: deterministic hook from node state) ---
        HandlePreScaling:
            \* Choose whether API call fails this reconcile
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

        \* --- Scaling ---
        HandleScaling:
            if StatefulsetStable(local_sts_ready, local_sts_spec, local_desired) then
                status_stage := "PostScaling";
            end if;
            goto ReleaseLock;

        \* --- PostScaling (NiFi: post_scale always returns Done) ---
        HandlePostScaling:
            hook_outcome := "Done";

        PostScalingAct:
            status_stage    := "Idle";
            status_desired  := NULL;
            status_previous := NULL;
            removal_targets := {};
            reconciler_observed_spec := status_replicas;
            goto ReleaseLock;

        \* --- Failed ---
        HandleFailed:
            if local_retry then
                retry_annotation := FALSE;
                status_stage     := "Idle";
                status_desired   := NULL;
                status_previous  := NULL;
                removal_targets  := {};
                reconciler_observed_spec := status_replicas;
            end if;
            goto ReleaseLock;

        \* --- on_failure (NiFi: trait default, no-op) ---
        HandleOnFailure:
            skip;
            goto ReleaseLock;

        ReleaseLock:
            reconcile_running := FALSE;
    end while;
end process;

\* ============================================================
\* Process: NiFi Node Manager — drives node state transitions
\* ============================================================

process NiFiNodeManager = "nifi_mgr"
begin
    NiFiLoop:
    while TRUE do
        DriveTransitions:
            if status_stage = "PreScaling" /\ removal_targets /= {} then
                \* Advance one node by one step along version-specific path
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

\* ============================================================
\* Remaining processes — identical to framework spec
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

process ProductOperator = "product_op"
begin
    ProductOpLoop:
    while TRUE do
        PropagateToSTS:
            sts_spec_replicas := status_replicas;
    end while;
end process;

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
          aux_previous_at_entry, reconciler_observed_spec, node_status, 
          nifi_version, removal_targets, api_call_failed

(* define statement *)
Stages == {"Idle", "PreScaling", "Scaling", "PostScaling", "FailedAtPre", "FailedAtPost"}
ActiveStages == {"PreScaling", "Scaling", "PostScaling"}
FailedStages == {"FailedAtPre", "FailedAtPost"}

StatefulsetStable(sts_ready, sts_spec, desired) ==
    sts_ready = sts_spec /\ (sts_spec > 0 \/ desired = 0)

SpecWriteAllowed ==
    status_stage = "Idle" \/ status_stage \in FailedStages


ValidTransitions == IF nifi_version = 1 THEN V1Transitions ELSE V2Transitions
PreDeleteStatus == IF nifi_version = 1 THEN "DISCONNECTED" ELSE "OFFLOADED"


NiFiPreScaleOutcome ==
    IF removal_targets = {} THEN "Done"
    ELSE IF \A i \in removal_targets: node_status[i] = "ABSENT" THEN "Done"
    ELSE IF api_call_failed THEN "Error"
    ELSE "InProgress"




N2 == status_desired /= NULL /\ status_desired > 0
      => 0 \notin removal_targets


N3 == (removal_targets /= {} /\ status_desired /= NULL /\ status_previous /= NULL)
      => removal_targets = status_desired..(status_previous - 1)



NiFiSafetyInvariant_Define == N2 /\ N3

VARIABLES local_stage, local_spec, local_status, local_desired, 
          local_previous, local_sts_ready, local_sts_spec, local_retry, 
          hook_outcome, hpa_target, webhook_response

vars == << pc, spec_replicas, status_replicas, status_desired, 
           status_previous, status_stage, retry_annotation, sts_spec_replicas, 
           sts_ready_replicas, webhook_available, reconcile_running, 
           aux_previous_at_entry, reconciler_observed_spec, node_status, 
           nifi_version, removal_targets, api_call_failed, local_stage, 
           local_spec, local_status, local_desired, local_previous, 
           local_sts_ready, local_sts_spec, local_retry, hook_outcome, 
           hpa_target, webhook_response >>

ProcSet == {"reconciler"} \cup {"nifi_mgr"} \cup {"hpa"} \cup {"product_op"} \cup {"sts_ctrl"} \cup {"retry"} \cup {"webhook_lc"}

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
        /\ node_status = [i \in 0..(MAX_REPLICAS-1) |-> "CONNECTED"]
        /\ nifi_version \in {1, 2}
        /\ removal_targets = {}
        /\ api_call_failed = FALSE
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
                                        [] self = "nifi_mgr" -> "NiFiLoop"
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
                                 reconciler_observed_spec, node_status, 
                                 nifi_version, removal_targets, 
                                 api_call_failed, local_stage, local_spec, 
                                 local_status, local_desired, local_previous, 
                                 local_sts_ready, local_sts_spec, local_retry, 
                                 hook_outcome, hpa_target, webhook_response >>

AcquireLock == /\ pc["reconciler"] = "AcquireLock"
               /\ ~reconcile_running
               /\ reconcile_running' = TRUE
               /\ pc' = [pc EXCEPT !["reconciler"] = "ReadSTS"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, aux_previous_at_entry, 
                               reconciler_observed_spec, node_status, 
                               nifi_version, removal_targets, api_call_failed, 
                               local_stage, local_spec, local_status, 
                               local_desired, local_previous, local_sts_ready, 
                               local_sts_spec, local_retry, hook_outcome, 
                               hpa_target, webhook_response >>

ReadSTS == /\ pc["reconciler"] = "ReadSTS"
           /\ local_sts_ready' = sts_ready_replicas
           /\ local_sts_spec' = sts_spec_replicas
           /\ pc' = [pc EXCEPT !["reconciler"] = "ReadScaler"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           node_status, nifi_version, removal_targets, 
                           api_call_failed, local_stage, local_spec, 
                           local_status, local_desired, local_previous, 
                           local_retry, hook_outcome, hpa_target, 
                           webhook_response >>

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
                              node_status, nifi_version, removal_targets, 
                              api_call_failed, local_sts_ready, local_sts_spec, 
                              hook_outcome, hpa_target, webhook_response >>

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
                              node_status, nifi_version, removal_targets, 
                              api_call_failed, local_stage, local_spec, 
                              local_status, local_desired, local_previous, 
                              local_sts_ready, local_sts_spec, local_retry, 
                              hook_outcome, hpa_target, webhook_response >>

HandleIdle == /\ pc["reconciler"] = "HandleIdle"
              /\ IF local_status = local_spec
                    THEN /\ reconciler_observed_spec' = local_spec
                         /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                         /\ UNCHANGED << status_desired, status_previous, 
                                         status_stage, aux_previous_at_entry, 
                                         removal_targets >>
                    ELSE /\ status_stage' = "PreScaling"
                         /\ status_desired' = local_spec
                         /\ status_previous' = local_status
                         /\ aux_previous_at_entry' = local_status
                         /\ IF local_spec < local_status
                               THEN /\ removal_targets' = local_spec..(local_status - 1)
                               ELSE /\ removal_targets' = {}
                         /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                         /\ UNCHANGED reconciler_observed_spec
              /\ UNCHANGED << spec_replicas, status_replicas, retry_annotation, 
                              sts_spec_replicas, sts_ready_replicas, 
                              webhook_available, reconcile_running, 
                              node_status, nifi_version, api_call_failed, 
                              local_stage, local_spec, local_status, 
                              local_desired, local_previous, local_sts_ready, 
                              local_sts_spec, local_retry, hook_outcome, 
                              hpa_target, webhook_response >>

HandlePreScaling == /\ pc["reconciler"] = "HandlePreScaling"
                    /\ \E failed \in {TRUE, FALSE}:
                         api_call_failed' = failed
                    /\ pc' = [pc EXCEPT !["reconciler"] = "PreScalingCompute"]
                    /\ UNCHANGED << spec_replicas, status_replicas, 
                                    status_desired, status_previous, 
                                    status_stage, retry_annotation, 
                                    sts_spec_replicas, sts_ready_replicas, 
                                    webhook_available, reconcile_running, 
                                    aux_previous_at_entry, 
                                    reconciler_observed_spec, node_status, 
                                    nifi_version, removal_targets, local_stage, 
                                    local_spec, local_status, local_desired, 
                                    local_previous, local_sts_ready, 
                                    local_sts_spec, local_retry, hook_outcome, 
                                    hpa_target, webhook_response >>

PreScalingCompute == /\ pc["reconciler"] = "PreScalingCompute"
                     /\ hook_outcome' = NiFiPreScaleOutcome
                     /\ pc' = [pc EXCEPT !["reconciler"] = "PreScalingAct"]
                     /\ UNCHANGED << spec_replicas, status_replicas, 
                                     status_desired, status_previous, 
                                     status_stage, retry_annotation, 
                                     sts_spec_replicas, sts_ready_replicas, 
                                     webhook_available, reconcile_running, 
                                     aux_previous_at_entry, 
                                     reconciler_observed_spec, node_status, 
                                     nifi_version, removal_targets, 
                                     api_call_failed, local_stage, local_spec, 
                                     local_status, local_desired, 
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
                                 reconciler_observed_spec, node_status, 
                                 nifi_version, removal_targets, 
                                 api_call_failed, local_stage, local_spec, 
                                 local_status, local_desired, local_previous, 
                                 local_sts_ready, local_sts_spec, local_retry, 
                                 hook_outcome, hpa_target, webhook_response >>

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
                                 reconciler_observed_spec, node_status, 
                                 nifi_version, removal_targets, 
                                 api_call_failed, local_stage, local_spec, 
                                 local_status, local_desired, local_previous, 
                                 local_sts_ready, local_sts_spec, local_retry, 
                                 hook_outcome, hpa_target, webhook_response >>

HandlePostScaling == /\ pc["reconciler"] = "HandlePostScaling"
                     /\ hook_outcome' = "Done"
                     /\ pc' = [pc EXCEPT !["reconciler"] = "PostScalingAct"]
                     /\ UNCHANGED << spec_replicas, status_replicas, 
                                     status_desired, status_previous, 
                                     status_stage, retry_annotation, 
                                     sts_spec_replicas, sts_ready_replicas, 
                                     webhook_available, reconcile_running, 
                                     aux_previous_at_entry, 
                                     reconciler_observed_spec, node_status, 
                                     nifi_version, removal_targets, 
                                     api_call_failed, local_stage, local_spec, 
                                     local_status, local_desired, 
                                     local_previous, local_sts_ready, 
                                     local_sts_spec, local_retry, hpa_target, 
                                     webhook_response >>

PostScalingAct == /\ pc["reconciler"] = "PostScalingAct"
                  /\ status_stage' = "Idle"
                  /\ status_desired' = NULL
                  /\ status_previous' = NULL
                  /\ removal_targets' = {}
                  /\ reconciler_observed_spec' = status_replicas
                  /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                  /\ UNCHANGED << spec_replicas, status_replicas, 
                                  retry_annotation, sts_spec_replicas, 
                                  sts_ready_replicas, webhook_available, 
                                  reconcile_running, aux_previous_at_entry, 
                                  node_status, nifi_version, api_call_failed, 
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
                           /\ removal_targets' = {}
                           /\ reconciler_observed_spec' = status_replicas
                      ELSE /\ TRUE
                           /\ UNCHANGED << status_desired, status_previous, 
                                           status_stage, retry_annotation, 
                                           reconciler_observed_spec, 
                                           removal_targets >>
                /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                /\ UNCHANGED << spec_replicas, status_replicas, 
                                sts_spec_replicas, sts_ready_replicas, 
                                webhook_available, reconcile_running, 
                                aux_previous_at_entry, node_status, 
                                nifi_version, api_call_failed, local_stage, 
                                local_spec, local_status, local_desired, 
                                local_previous, local_sts_ready, 
                                local_sts_spec, local_retry, hook_outcome, 
                                hpa_target, webhook_response >>

HandleOnFailure == /\ pc["reconciler"] = "HandleOnFailure"
                   /\ TRUE
                   /\ pc' = [pc EXCEPT !["reconciler"] = "ReleaseLock"]
                   /\ UNCHANGED << spec_replicas, status_replicas, 
                                   status_desired, status_previous, 
                                   status_stage, retry_annotation, 
                                   sts_spec_replicas, sts_ready_replicas, 
                                   webhook_available, reconcile_running, 
                                   aux_previous_at_entry, 
                                   reconciler_observed_spec, node_status, 
                                   nifi_version, removal_targets, 
                                   api_call_failed, local_stage, local_spec, 
                                   local_status, local_desired, local_previous, 
                                   local_sts_ready, local_sts_spec, 
                                   local_retry, hook_outcome, hpa_target, 
                                   webhook_response >>

ReleaseLock == /\ pc["reconciler"] = "ReleaseLock"
               /\ reconcile_running' = FALSE
               /\ pc' = [pc EXCEPT !["reconciler"] = "ReconcileLoop"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, aux_previous_at_entry, 
                               reconciler_observed_spec, node_status, 
                               nifi_version, removal_targets, api_call_failed, 
                               local_stage, local_spec, local_status, 
                               local_desired, local_previous, local_sts_ready, 
                               local_sts_spec, local_retry, hook_outcome, 
                               hpa_target, webhook_response >>

Reconciler == ReconcileLoop \/ AcquireLock \/ ReadSTS \/ ReadScaler
                 \/ CheckStage \/ HandleIdle \/ HandlePreScaling
                 \/ PreScalingCompute \/ PreScalingAct \/ HandleScaling
                 \/ HandlePostScaling \/ PostScalingAct \/ HandleFailed
                 \/ HandleOnFailure \/ ReleaseLock

NiFiLoop == /\ pc["nifi_mgr"] = "NiFiLoop"
            /\ pc' = [pc EXCEPT !["nifi_mgr"] = "DriveTransitions"]
            /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                            status_previous, status_stage, retry_annotation, 
                            sts_spec_replicas, sts_ready_replicas, 
                            webhook_available, reconcile_running, 
                            aux_previous_at_entry, reconciler_observed_spec, 
                            node_status, nifi_version, removal_targets, 
                            api_call_failed, local_stage, local_spec, 
                            local_status, local_desired, local_previous, 
                            local_sts_ready, local_sts_spec, local_retry, 
                            hook_outcome, hpa_target, webhook_response >>

DriveTransitions == /\ pc["nifi_mgr"] = "DriveTransitions"
                    /\ IF status_stage = "PreScaling" /\ removal_targets /= {}
                          THEN /\ \E node \in removal_targets:
                                    \E transition \in ValidTransitions:
                                      IF transition[1] = node_status[node]
                                         THEN /\ node_status' = [node_status EXCEPT ![node] = transition[2]]
                                         ELSE /\ TRUE
                                              /\ UNCHANGED node_status
                          ELSE /\ TRUE
                               /\ UNCHANGED node_status
                    /\ pc' = [pc EXCEPT !["nifi_mgr"] = "NiFiLoop"]
                    /\ UNCHANGED << spec_replicas, status_replicas, 
                                    status_desired, status_previous, 
                                    status_stage, retry_annotation, 
                                    sts_spec_replicas, sts_ready_replicas, 
                                    webhook_available, reconcile_running, 
                                    aux_previous_at_entry, 
                                    reconciler_observed_spec, nifi_version, 
                                    removal_targets, api_call_failed, 
                                    local_stage, local_spec, local_status, 
                                    local_desired, local_previous, 
                                    local_sts_ready, local_sts_spec, 
                                    local_retry, hook_outcome, hpa_target, 
                                    webhook_response >>

NiFiNodeManager == NiFiLoop \/ DriveTransitions

HPALoop == /\ pc["hpa"] = "HPALoop"
           /\ pc' = [pc EXCEPT !["hpa"] = "SelectTarget"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           node_status, nifi_version, removal_targets, 
                           api_call_failed, local_stage, local_spec, 
                           local_status, local_desired, local_previous, 
                           local_sts_ready, local_sts_spec, local_retry, 
                           hook_outcome, hpa_target, webhook_response >>

SelectTarget == /\ pc["hpa"] = "SelectTarget"
                /\ \E t \in 0..MAX_REPLICAS:
                     hpa_target' = t
                /\ pc' = [pc EXCEPT !["hpa"] = "WebhookCheck"]
                /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                                status_previous, status_stage, 
                                retry_annotation, sts_spec_replicas, 
                                sts_ready_replicas, webhook_available, 
                                reconcile_running, aux_previous_at_entry, 
                                reconciler_observed_spec, node_status, 
                                nifi_version, removal_targets, api_call_failed, 
                                local_stage, local_spec, local_status, 
                                local_desired, local_previous, local_sts_ready, 
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
                                reconciler_observed_spec, node_status, 
                                nifi_version, removal_targets, api_call_failed, 
                                local_stage, local_spec, local_status, 
                                local_desired, local_previous, local_sts_ready, 
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
                                  reconciler_observed_spec, node_status, 
                                  nifi_version, removal_targets, 
                                  api_call_failed, local_stage, local_spec, 
                                  local_status, local_desired, local_previous, 
                                  local_sts_ready, local_sts_spec, local_retry, 
                                  hook_outcome, hpa_target, webhook_response >>

HPAWriter == HPALoop \/ SelectTarget \/ WebhookCheck \/ APIServerApply

ProductOpLoop == /\ pc["product_op"] = "ProductOpLoop"
                 /\ pc' = [pc EXCEPT !["product_op"] = "PropagateToSTS"]
                 /\ UNCHANGED << spec_replicas, status_replicas, 
                                 status_desired, status_previous, status_stage, 
                                 retry_annotation, sts_spec_replicas, 
                                 sts_ready_replicas, webhook_available, 
                                 reconcile_running, aux_previous_at_entry, 
                                 reconciler_observed_spec, node_status, 
                                 nifi_version, removal_targets, 
                                 api_call_failed, local_stage, local_spec, 
                                 local_status, local_desired, local_previous, 
                                 local_sts_ready, local_sts_spec, local_retry, 
                                 hook_outcome, hpa_target, webhook_response >>

PropagateToSTS == /\ pc["product_op"] = "PropagateToSTS"
                  /\ sts_spec_replicas' = status_replicas
                  /\ pc' = [pc EXCEPT !["product_op"] = "ProductOpLoop"]
                  /\ UNCHANGED << spec_replicas, status_replicas, 
                                  status_desired, status_previous, 
                                  status_stage, retry_annotation, 
                                  sts_ready_replicas, webhook_available, 
                                  reconcile_running, aux_previous_at_entry, 
                                  reconciler_observed_spec, node_status, 
                                  nifi_version, removal_targets, 
                                  api_call_failed, local_stage, local_spec, 
                                  local_status, local_desired, local_previous, 
                                  local_sts_ready, local_sts_spec, local_retry, 
                                  hook_outcome, hpa_target, webhook_response >>

ProductOperator == ProductOpLoop \/ PropagateToSTS

STSLoop == /\ pc["sts_ctrl"] = "STSLoop"
           /\ pc' = [pc EXCEPT !["sts_ctrl"] = "Progress"]
           /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                           status_previous, status_stage, retry_annotation, 
                           sts_spec_replicas, sts_ready_replicas, 
                           webhook_available, reconcile_running, 
                           aux_previous_at_entry, reconciler_observed_spec, 
                           node_status, nifi_version, removal_targets, 
                           api_call_failed, local_stage, local_spec, 
                           local_status, local_desired, local_previous, 
                           local_sts_ready, local_sts_spec, local_retry, 
                           hook_outcome, hpa_target, webhook_response >>

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
                            reconciler_observed_spec, node_status, 
                            nifi_version, removal_targets, api_call_failed, 
                            local_stage, local_spec, local_status, 
                            local_desired, local_previous, local_sts_ready, 
                            local_sts_spec, local_retry, hook_outcome, 
                            hpa_target, webhook_response >>

STSController == STSLoop \/ Progress

RetryLoop == /\ pc["retry"] = "RetryLoop"
             /\ pc' = [pc EXCEPT !["retry"] = "ApplyRetry"]
             /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                             status_previous, status_stage, retry_annotation, 
                             sts_spec_replicas, sts_ready_replicas, 
                             webhook_available, reconcile_running, 
                             aux_previous_at_entry, reconciler_observed_spec, 
                             node_status, nifi_version, removal_targets, 
                             api_call_failed, local_stage, local_spec, 
                             local_status, local_desired, local_previous, 
                             local_sts_ready, local_sts_spec, local_retry, 
                             hook_outcome, hpa_target, webhook_response >>

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
                              reconciler_observed_spec, node_status, 
                              nifi_version, removal_targets, api_call_failed, 
                              local_stage, local_spec, local_status, 
                              local_desired, local_previous, local_sts_ready, 
                              local_sts_spec, local_retry, hook_outcome, 
                              hpa_target, webhook_response >>

RetryAnnotator == RetryLoop \/ ApplyRetry

WebhookLoop == /\ pc["webhook_lc"] = "WebhookLoop"
               /\ pc' = [pc EXCEPT !["webhook_lc"] = "Toggle"]
               /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                               status_previous, status_stage, retry_annotation, 
                               sts_spec_replicas, sts_ready_replicas, 
                               webhook_available, reconcile_running, 
                               aux_previous_at_entry, reconciler_observed_spec, 
                               node_status, nifi_version, removal_targets, 
                               api_call_failed, local_stage, local_spec, 
                               local_status, local_desired, local_previous, 
                               local_sts_ready, local_sts_spec, local_retry, 
                               hook_outcome, hpa_target, webhook_response >>

Toggle == /\ pc["webhook_lc"] = "Toggle"
          /\ \E avail \in {TRUE, FALSE}:
               webhook_available' = avail
          /\ pc' = [pc EXCEPT !["webhook_lc"] = "WebhookLoop"]
          /\ UNCHANGED << spec_replicas, status_replicas, status_desired, 
                          status_previous, status_stage, retry_annotation, 
                          sts_spec_replicas, sts_ready_replicas, 
                          reconcile_running, aux_previous_at_entry, 
                          reconciler_observed_spec, node_status, nifi_version, 
                          removal_targets, api_call_failed, local_stage, 
                          local_spec, local_status, local_desired, 
                          local_previous, local_sts_ready, local_sts_spec, 
                          local_retry, hook_outcome, hpa_target, 
                          webhook_response >>

WebhookLifecycle == WebhookLoop \/ Toggle

Next == Reconciler \/ NiFiNodeManager \/ HPAWriter \/ ProductOperator
           \/ STSController \/ RetryAnnotator \/ WebhookLifecycle

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

\* N5: At PreScaling->Scaling, all targets are ABSENT when hook returns Done
\* (Defined here because hook_outcome is a process-local variable)
N5 == (pc["reconciler"] = "PreScalingAct" /\ hook_outcome = "Done"
       /\ removal_targets /= {})
      => \A i \in removal_targets: node_status[i] = "ABSENT"

\* Combined NiFi safety invariant
NiFiSafetyInvariant == NiFiSafetyInvariant_Define /\ N5

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
NL1 == [](\A i \in 0..(MAX_REPLICAS-1):
    node_status[i] = "OFFLOADING"
    ~> (node_status[i] = "OFFLOADED" \/ node_status[i] = "ABSENT"))

\* NL2: Scale-down eventually removes all targeted nodes
NL2 == [](removal_targets /= {}
         ~> \A i \in removal_targets: node_status[i] = "ABSENT")

=============================================================================
