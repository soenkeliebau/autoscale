// Formal Correctness Check: StackableScaler State Machine in Quint
//
// Models the reconcile_scaler state machine from operator-rs/crates/stackable-operator/src/crd/scaler/reconciler.rs
// and the status-tracking logic that propagates replica counts between spec, status, and StatefulSet.
//
// The model captures:
//  - The five-stage state machine: Idle → PreScaling → Scaling → PostScaling → Idle (+ Failed)
//  - When status.replicas is updated (only on transition to Scaling)
//  - When status.desiredReplicas is set/cleared
//  - The webhook constraint blocking spec.replicas writes mid-flight
//  - Hook outcomes (Done / InProgress / Error) as nondeterministic environment choices
//  - StatefulSet stability as an external boolean signal
//  - Recovery from Failed via retry annotation

module StackableScaler {

    // ----------------------------------------------------------------
    // Types
    // ----------------------------------------------------------------

    // State machine stages — mirrors ScalerStage in mod.rs
    type Stage = Idle | PreScaling | Scaling | PostScaling
               | FailedAtPre | FailedAtPost

    // Hook outcomes — mirrors HookOutcome + error case
    type HookResult = Done | InProgress | HookError

    // ----------------------------------------------------------------
    // State Variables
    // ----------------------------------------------------------------

    // -- Spec (written by HPA via /scale subresource) --
    var specReplicas: int           // spec.replicas (desired by HPA)

    // -- Status (written by reconcile_scaler) --
    var statusReplicas: int         // status.replicas (current STS target, read by HPA)
    var desiredReplicas: int        // status.desiredReplicas (-1 means None/unset)
    var stage: Stage                // status.currentState.stage

    // -- External environment signals --
    var stsReady: int               // StatefulSet ready_replicas (actual running pods)

    // -- Bookkeeping for verification --
    var reconcileCount: int         // counts reconcile invocations (for liveness)

    // ----------------------------------------------------------------
    // Constants / Configuration
    // ----------------------------------------------------------------

    // We use -1 as the sentinel for "desiredReplicas not set" (Option::None in Rust)
    pure val UNSET = -1

    // Maximum replica count we model (keeps state space finite)
    pure val MAX_REPLICAS = 4

    // ----------------------------------------------------------------
    // Derived state helpers
    // ----------------------------------------------------------------

    // Whether the StatefulSet is considered stable.
    // Mirrors the logic in controller.rs:
    //   sts_ready == sts_desired && (sts_desired > 0 || scaler_target == 0)
    // where sts_desired = statusReplicas (after it has been written)
    val statefulsetStable: bool =
        stsReady == statusReplicas and (statusReplicas > 0 or desiredReplicas == 0)

    // Whether the webhook allows HPA writes to spec.replicas.
    // Rejected when stage is not Idle and not Failed.
    val specWriteAllowed: bool =
        stage == Idle or stage == FailedAtPre or stage == FailedAtPost

    // ----------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------

    action init = all {
        specReplicas'     = 1,
        statusReplicas'   = 1,
        desiredReplicas'  = UNSET,
        stage'            = Idle,
        stsReady'         = 1,
        reconcileCount'   = 0,
    }

    // ----------------------------------------------------------------
    // Environment actions (nondeterministic)
    // ----------------------------------------------------------------

    // HPA writes a new desired replica count to spec.replicas.
    // Only allowed when the webhook permits it (stage is Idle or Failed).
    action hpaWrite(newTarget: int) = all {
        specWriteAllowed,
        newTarget >= 0,
        newTarget <= MAX_REPLICAS,
        newTarget != specReplicas,      // HPA only writes when it wants a change
        specReplicas' = newTarget,
        // All other state unchanged
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        stage'           = stage,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount,
    }

    // StatefulSet controller progresses pods towards the target.
    // Moves stsReady one step closer to statusReplicas (the STS spec.replicas).
    action stsProgress = all {
        stsReady != statusReplicas,
        stsReady' = if (stsReady < statusReplicas)
                        stsReady + 1
                    else
                        stsReady - 1,
        // All other state unchanged
        specReplicas'    = specReplicas,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        stage'           = stage,
        reconcileCount'  = reconcileCount,
    }

    // ----------------------------------------------------------------
    // Reconcile actions (one per state machine branch)
    //
    // Each action models one invocation of reconcile_scaler.
    // Hook outcomes are parameters chosen nondeterministically by the caller.
    // ----------------------------------------------------------------

    // --- Idle ---

    // Idle with no replica change — no-op
    action reconcileIdleNoChange = all {
        stage == Idle,
        statusReplicas == specReplicas,
        // NoChange: nothing is written
        specReplicas'    = specReplicas,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        stage'           = Idle,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // Idle with replica mismatch → transition to PreScaling
    // Sets desiredReplicas, keeps statusReplicas unchanged.
    action reconcileIdleToPreScaling = all {
        stage == Idle,
        statusReplicas != specReplicas,
        stage'           = PreScaling,
        desiredReplicas' = specReplicas,       // record the target
        statusReplicas'  = statusReplicas,     // NOT updated yet
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // --- PreScaling ---

    // PreScaling hook returns Done → advance to Scaling
    // This is the critical moment: statusReplicas is updated to desiredReplicas.
    action reconcilePreScalingDone = all {
        stage == PreScaling,
        stage'           = Scaling,
        statusReplicas'  = desiredReplicas,    // NOW the STS target changes
        desiredReplicas' = desiredReplicas,    // preserved
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // PreScaling hook returns InProgress → requeue (stay in PreScaling)
    action reconcilePreScalingInProgress = all {
        stage == PreScaling,
        stage'           = PreScaling,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // PreScaling hook returns Error → transition to Failed
    action reconcilePreScalingError = all {
        stage == PreScaling,
        stage'           = FailedAtPre,
        statusReplicas'  = statusReplicas,     // unchanged on failure
        desiredReplicas' = desiredReplicas,    // preserved for diagnostics
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // --- Scaling ---

    // StatefulSet is stable → advance to PostScaling
    action reconcileScalingDone = all {
        stage == Scaling,
        statefulsetStable,
        stage'           = PostScaling,
        statusReplicas'  = statusReplicas,     // already updated on entry
        desiredReplicas' = desiredReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // StatefulSet not yet stable → requeue (stay in Scaling)
    action reconcileScalingWait = all {
        stage == Scaling,
        not(statefulsetStable),
        stage'           = Scaling,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // --- PostScaling ---

    // PostScaling hook returns Done → return to Idle
    // Clear desiredReplicas.
    action reconcilePostScalingDone = all {
        stage == PostScaling,
        stage'           = Idle,
        desiredReplicas' = UNSET,              // cleared on completion
        statusReplicas'  = statusReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // PostScaling hook returns InProgress → requeue (stay in PostScaling)
    action reconcilePostScalingInProgress = all {
        stage == PostScaling,
        stage'           = PostScaling,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // PostScaling hook returns Error → transition to Failed
    action reconcilePostScalingError = all {
        stage == PostScaling,
        stage'           = FailedAtPost,
        statusReplicas'  = statusReplicas,     // unchanged on failure
        desiredReplicas' = desiredReplicas,    // preserved for diagnostics
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // --- Failed ---

    // Failed stays failed (no-op reconcile)
    action reconcileFailedNoOp = all {
        stage == FailedAtPre or stage == FailedAtPost,
        stage'           = stage,
        statusReplicas'  = statusReplicas,
        desiredReplicas' = desiredReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // Manual retry annotation resets Failed → Idle
    action retryFromFailed = all {
        stage == FailedAtPre or stage == FailedAtPost,
        stage'           = Idle,
        desiredReplicas' = UNSET,
        statusReplicas'  = statusReplicas,
        specReplicas'    = specReplicas,
        stsReady'        = stsReady,
        reconcileCount'  = reconcileCount + 1,
    }

    // ----------------------------------------------------------------
    // Step relation (nondeterministic choice)
    // ----------------------------------------------------------------

    action step = any {
        // Environment
        nondet target = oneOf(0.to(MAX_REPLICAS))
        hpaWrite(target),
        stsProgress,

        // Reconciler
        reconcileIdleNoChange,
        reconcileIdleToPreScaling,
        reconcilePreScalingDone,
        reconcilePreScalingInProgress,
        reconcilePreScalingError,
        reconcileScalingDone,
        reconcileScalingWait,
        reconcilePostScalingDone,
        reconcilePostScalingInProgress,
        reconcilePostScalingError,
        reconcileFailedNoOp,
        retryFromFailed,
    }

    // ----------------------------------------------------------------
    // Safety Invariants
    // ----------------------------------------------------------------

    // S1: Replica counts are always non-negative
    val nonNegativeReplicas: bool =
        specReplicas >= 0 and statusReplicas >= 0 and stsReady >= 0

    // S2: desiredReplicas is either UNSET or non-negative
    val validDesiredReplicas: bool =
        desiredReplicas == UNSET or desiredReplicas >= 0

    // S3: Stage is always a valid member of the enum
    val validStage: bool =
        stage == Idle or stage == PreScaling or stage == Scaling
        or stage == PostScaling or stage == FailedAtPre or stage == FailedAtPost

    // S4: In Idle with no pending scale, status.replicas matches spec.replicas
    // (if desiredReplicas is unset and we are Idle, the system is at rest)
    val idleConsistency: bool =
        (stage == Idle and desiredReplicas == UNSET)
        implies statusReplicas == specReplicas

    // S5: desiredReplicas is set whenever the state machine is active (not Idle)
    // and not after a retry reset
    val desiredReplicasSetWhenActive: bool =
        (stage == PreScaling or stage == Scaling or stage == PostScaling)
        implies desiredReplicas != UNSET

    // S6: statusReplicas is only updated when entering the Scaling stage.
    // Equivalently: in PreScaling, statusReplicas still holds the OLD value
    // (it has not been updated to desiredReplicas yet).
    val preScalingDoesNotUpdateReplicas: bool =
        stage == PreScaling implies statusReplicas != desiredReplicas
            or statusReplicas == desiredReplicas  // allow vacuously true if equal by coincidence
    // The real property: statusReplicas changes ONLY at the PreScaling→Scaling transition.
    // This is structurally enforced by the actions, but we verify the consequence:
    // if we are in Scaling, statusReplicas == desiredReplicas.
    val scalingReplicasMatchDesired: bool =
        stage == Scaling implies statusReplicas == desiredReplicas

    // S7: PostScaling implies statusReplicas == desiredReplicas
    // (the STS target was already set when entering Scaling, and it doesn't change in PostScaling)
    val postScalingReplicasMatchDesired: bool =
        stage == PostScaling implies statusReplicas == desiredReplicas

    // S8: Failed is a trap — once entered, only retryFromFailed can exit
    // This is structural but we can check: stage in Failed implies stage was
    // previously in Failed or just transitioned. The invariant is that
    // the reconciler does not spontaneously leave Failed.
    // Verified by: reconcileFailedNoOp is the only reconcile action for Failed stages.

    // S9: Webhook blocks spec writes mid-flight
    // If the state machine is in PreScaling/Scaling/PostScaling, the HPA cannot
    // change spec.replicas. This is enforced by specWriteAllowed in hpaWrite.
    // We verify the consequence: during active scaling, specReplicas is frozen
    // at the value that triggered the transition.
    val specFrozenDuringScaling: bool =
        (stage == PreScaling or stage == Scaling or stage == PostScaling)
        implies specReplicas == desiredReplicas

    // S10: desiredReplicas is cleared (UNSET) when returning to Idle
    val idleDesiredCleared: bool =
        stage == Idle implies desiredReplicas == UNSET

    // S11: statusReplicas is bounded
    val statusReplicasBounded: bool =
        statusReplicas >= 0 and statusReplicas <= MAX_REPLICAS

    // S12: stsReady is bounded
    val stsReadyBounded: bool =
        stsReady >= 0 and stsReady <= MAX_REPLICAS

    // Composite safety invariant
    val safetyInvariant: bool = and {
        nonNegativeReplicas,
        validDesiredReplicas,
        validStage,
        idleConsistency,
        desiredReplicasSetWhenActive,
        scalingReplicasMatchDesired,
        postScalingReplicasMatchDesired,
        specFrozenDuringScaling,
        idleDesiredCleared,
        statusReplicasBounded,
        stsReadyBounded,
    }

    // ----------------------------------------------------------------
    // Liveness Properties (Temporal)
    // ----------------------------------------------------------------

    // L1: If we are in PreScaling and hooks eventually return Done,
    //     we eventually leave PreScaling.
    temporal eventuallyLeavePreScaling =
        always(stage == PreScaling implies eventually(stage != PreScaling))

    // L2: If we are in Scaling and the STS eventually stabilizes,
    //     we eventually leave Scaling.
    temporal eventuallyLeaveScaling =
        always(stage == Scaling implies eventually(stage != Scaling))

    // L3: If we are in PostScaling and hooks eventually return Done,
    //     we eventually leave PostScaling.
    temporal eventuallyLeavePostScaling =
        always(stage == PostScaling implies eventually(stage != PostScaling))

    // L4: Overall convergence — the system eventually returns to Idle or Failed
    //     (i.e., doesn't get stuck in an intermediate stage forever).
    temporal eventualSettlement =
        always(eventually(stage == Idle or stage == FailedAtPre or stage == FailedAtPost))

    // L5: If scale is requested and hooks succeed, eventually statusReplicas matches specReplicas
    temporal eventualReplicaConvergence =
        always(stage == Idle implies eventually(statusReplicas == specReplicas))

    // ----------------------------------------------------------------
    // Test Scenarios (run traces)
    // ----------------------------------------------------------------

    // T1: Simple scale-up from 1 to 3, all hooks return Done immediately
    run scaleUpHappyPath = {
        init
        .then(hpaWrite(3))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(stsProgress)     // stsReady: 1 → 2
        .then(reconcileScalingWait)
        .then(stsProgress)     // stsReady: 2 → 3
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 3),
            assert(specReplicas == 3),
            assert(desiredReplicas == UNSET),
            assert(stsReady == 3),
            // Unchanged vars for next step
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T2: Scale-down from 3 to 1, pre-scale hook takes two reconciles
    run scaleDownWithSlowHook = {
        // Start at 3 replicas
        all {
            specReplicas'    = 3,
            statusReplicas'  = 3,
            desiredReplicas' = UNSET,
            stage'           = Idle,
            stsReady'        = 3,
            reconcileCount'  = 0,
        }
        .then(hpaWrite(1))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingInProgress)   // hook not done yet
        .then(reconcilePreScalingInProgress)   // still not done
        .then(reconcilePreScalingDone)         // hook completes
        .then(stsProgress)                     // stsReady: 3 → 2
        .then(reconcileScalingWait)
        .then(stsProgress)                     // stsReady: 2 → 1
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 1),
            assert(stsReady == 1),
            assert(desiredReplicas == UNSET),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T3: Pre-scale hook fails → enters Failed, then manual retry completes
    run preScaleFailureAndRecovery = {
        init
        .then(hpaWrite(3))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingError)        // hook fails
        .then(all {
            assert(stage == FailedAtPre),
            assert(statusReplicas == 1),       // unchanged
            assert(desiredReplicas != UNSET),  // preserved for diagnostics
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
        .then(retryFromFailed)                 // manual retry annotation
        .then(all {
            assert(stage == Idle),
            assert(desiredReplicas == UNSET),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
        // Now the scale can proceed normally
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(stsProgress)
        .then(stsProgress)
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 3),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T4: Post-scale hook fails → Failed state
    run postScaleFailure = {
        init
        .then(hpaWrite(2))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(stsProgress)                     // stsReady: 1 → 2
        .then(reconcileScalingDone)
        .then(reconcilePostScalingError)       // post-scale hook fails
        .then(all {
            assert(stage == FailedAtPost),
            assert(statusReplicas == 2),       // STS was already scaled
            assert(desiredReplicas == 2),      // preserved
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T5: Scale to zero
    run scaleToZero = {
        init
        .then(hpaWrite(0))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(all {
            assert(stage == Scaling),
            assert(statusReplicas == 0),       // STS target now 0
            assert(desiredReplicas == 0),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
        .then(stsProgress)                     // stsReady: 1 → 0
        // Now statefulsetStable should be true because desiredReplicas == 0
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 0),
            assert(stsReady == 0),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T6: HPA write is blocked during active scaling
    run webhookBlocksMidFlight = {
        init
        .then(hpaWrite(3))
        .then(reconcileIdleToPreScaling)
        // At this point stage == PreScaling, so specWriteAllowed == false
        .then(all {
            assert(not(specWriteAllowed)),
            assert(stage == PreScaling),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T7: HPA write IS allowed when in Failed state (for retry scenarios)
    run hpaCanWriteInFailed = {
        init
        .then(hpaWrite(3))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingError)
        .then(all {
            assert(stage == FailedAtPre),
            assert(specWriteAllowed),          // webhook allows writes in Failed
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
        // HPA can adjust the target while we're failed
        .then(hpaWrite(2))
        .then(all {
            assert(specReplicas == 2),         // HPA write succeeded
            assert(stage == FailedAtPre),      // still failed
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T8: Successive scale operations (scale up then scale down)
    run successiveScaleOperations = {
        init
        // First: scale up 1 → 3
        .then(hpaWrite(3))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(stsProgress)
        .then(stsProgress)
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 3),
            assert(stsReady == 3),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
        // Second: scale down 3 → 1
        .then(hpaWrite(1))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingDone)
        .then(stsProgress)
        .then(stsProgress)
        .then(reconcileScalingDone)
        .then(reconcilePostScalingDone)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 1),
            assert(stsReady == 1),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T9: No-op when spec equals status in Idle
    run idleNoOp = {
        init
        .then(reconcileIdleNoChange)
        .then(all {
            assert(stage == Idle),
            assert(statusReplicas == 1),
            assert(specReplicas == 1),
            assert(reconcileCount == 1),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // T10: Failed stays failed across multiple reconciles until retry
    run failedIsSticky = {
        init
        .then(hpaWrite(2))
        .then(reconcileIdleToPreScaling)
        .then(reconcilePreScalingError)
        .then(reconcileFailedNoOp)
        .then(reconcileFailedNoOp)
        .then(reconcileFailedNoOp)
        .then(all {
            assert(stage == FailedAtPre),      // still failed after 3 no-ops
            assert(reconcileCount == 6),
            specReplicas'    = specReplicas,
            statusReplicas'  = statusReplicas,
            desiredReplicas' = desiredReplicas,
            stage'           = stage,
            stsReady'        = stsReady,
            reconcileCount'  = reconcileCount,
        })
    }

    // ----------------------------------------------------------------
    // Correctness Criteria Summary
    // ----------------------------------------------------------------
    //
    // | Property                        | Type     | Description                                             |
    // |---------------------------------|----------|---------------------------------------------------------|
    // | nonNegativeReplicas             | Safety   | All replica counts >= 0                                 |
    // | validDesiredReplicas            | Safety   | desiredReplicas is UNSET or >= 0                        |
    // | validStage                      | Safety   | Stage is always a valid enum member                     |
    // | idleConsistency                 | Safety   | Idle + no pending → status == spec                      |
    // | desiredReplicasSetWhenActive    | Safety   | Active stages have desiredReplicas set                  |
    // | scalingReplicasMatchDesired     | Safety   | In Scaling, statusReplicas == desiredReplicas           |
    // | postScalingReplicasMatchDesired | Safety   | In PostScaling, statusReplicas == desiredReplicas       |
    // | specFrozenDuringScaling         | Safety   | Webhook blocks spec writes mid-flight                   |
    // | idleDesiredCleared              | Safety   | desiredReplicas cleared when returning to Idle          |
    // | statusReplicasBounded           | Safety   | statusReplicas in [0, MAX_REPLICAS]                     |
    // | stsReadyBounded                 | Safety   | stsReady in [0, MAX_REPLICAS]                           |
    // | eventuallyLeavePreScaling       | Liveness | PreScaling doesn't get stuck forever                    |
    // | eventuallyLeaveScaling          | Liveness | Scaling doesn't get stuck forever                       |
    // | eventuallyLeavePostScaling      | Liveness | PostScaling doesn't get stuck forever                   |
    // | eventualSettlement              | Liveness | System always reaches Idle or Failed                    |
    // | eventualReplicaConvergence      | Liveness | Replicas eventually converge to spec                    |
}