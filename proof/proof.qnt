# Formal Correctness Check: Kubernetes Resource Scaling in Quint

## 1. Overview

This specification models the state machine for a Kubernetes scale operation, tracking the lifecycle from request through completion.

## 2. State Variables
```quint
module KubernetesScaling {
    // Input values
    var targetReplicas: int       // Desired number of replicas
    var currentReplicas: int      // Actual running replicas

    // Internal state
    var scaleRequested: bool      // Scale operation has been requested
    var lastAcknowledgedTarget: int  // Last target the controller acknowledged
}
```

## 3. Derived State: Scale Operation Status
```quint
// Scale operation states
type ScaleStatus = Idle | Requested | Running | Finished

def scaleStatus: ScaleStatus = {
    if (not(scaleRequested) and targetReplicas == currentReplicas) {
        Idle
    } else if (scaleRequested and lastAcknowledgedTarget != targetReplicas) {
        Requested
    } else if (lastAcknowledgedTarget == targetReplicas and currentReplicas != targetReplicas) {
        Running
    } else if (currentReplicas == targetReplicas and lastAcknowledgedTarget == targetReplicas) {
        Finished
    } else {
        Idle  // Default fallback
    }
}
```

## 4. State Transitions

### 4.1 Valid Transitions
```
┌──────────┐   requestScale    ┌───────────┐   acknowledge   ┌─────────┐   replicasMatch   ┌──────────┐
│   Idle   │ ───────────────►  │ Requested │ ─────────────►  │ Running │ ────────────────► │ Finished │
└──────────┘                   └───────────┘                 └─────────┘                   └──────────┘
     ▲                                                                                          │
     └──────────────────────────────────────── reset ──────────────────────────────────────────┘
```

### 4.2 Actions
```quint
action init = all {
    targetReplicas' = 0,
    currentReplicas' = 0,
    scaleRequested' = false,
    lastAcknowledgedTarget' = 0,
}

action requestScale(newTarget: int) = all {
    newTarget >= 0,
    targetReplicas' = newTarget,
    scaleRequested' = true,
    // Other vars unchanged
    currentReplicas' = currentReplicas,
    lastAcknowledgedTarget' = lastAcknowledgedTarget,
}

action acknowledgeScale = all {
    scaleRequested,
    lastAcknowledgedTarget' = targetReplicas,
    // Other vars unchanged
    targetReplicas' = targetReplicas,
    currentReplicas' = currentReplicas,
    scaleRequested' = scaleRequested,
}

action progressScale = all {
    lastAcknowledgedTarget == targetReplicas,
    currentReplicas != targetReplicas,
    // Move one replica closer to target
    currentReplicas' = if (currentReplicas < targetReplicas)
                         currentReplicas + 1
                       else
                         currentReplicas - 1,
    // Other vars unchanged
    targetReplicas' = targetReplicas,
    scaleRequested' = scaleRequested,
    lastAcknowledgedTarget' = lastAcknowledgedTarget,
}

action completeScale = all {
    currentReplicas == targetReplicas,
    scaleRequested' = false,
    // Other vars unchanged
    targetReplicas' = targetReplicas,
    currentReplicas' = currentReplicas,
    lastAcknowledgedTarget' = lastAcknowledgedTarget,
}
```

## 5. Invariants to Verify

### 5.1 Safety Properties
```quint
// Replicas are always non-negative
val nonNegativeReplicas = currentReplicas >= 0 and targetReplicas >= 0

// Status is always well-defined
val validStatus = scaleStatus.in(Set(Idle, Requested, Running, Finished))

// If Finished, current matches target
val finishedImpliesMatch =
    scaleStatus == Finished implies currentReplicas == targetReplicas

// If Idle and no request, replicas match
val idleConsistency =
    (scaleStatus == Idle and not(scaleRequested)) implies
    currentReplicas == targetReplicas

// Running implies controller has acknowledged
val runningImpliesAcknowledged =
    scaleStatus == Running implies lastAcknowledgedTarget == targetReplicas
```

### 5.2 Liveness Properties (Temporal)
```quint
// Eventually, if a scale is requested, it will be acknowledged
temporal eventualAcknowledge =
    always(scaleStatus == Requested implies eventually(scaleStatus != Requested))

// Eventually, scaling completes
temporal eventualCompletion =
    always(scaleRequested implies eventually(currentReplicas == targetReplicas))

// No infinite scaling: replicas converge
temporal convergence =
    always(eventually(currentReplicas == targetReplicas))
```

## 6. Test Scenarios

### 6.1 Scale Up
```quint
run scaleUpTest = {
    init
    .then(requestScale(3))
    .then(acknowledgeScale)
    .then(progressScale)
    .then(progressScale)
    .then(progressScale)
    .then(completeScale)
    .then(all {
        assert(scaleStatus == Idle),
        assert(currentReplicas == 3),
    })
}
```

### 6.2 Scale Down
```quint
run scaleDownTest = {
    init
    .then(requestScale(5))
    .then(acknowledgeScale)
    .then(progressScale.repeated(5))
    .then(completeScale)
    .then(requestScale(2))
    .then(acknowledgeScale)
    .then(progressScale.repeated(3))
    .then(completeScale)
    .then(all {
        assert(currentReplicas == 2),
    })
}
```

### 6.3 Concurrent Request (New Scale During Running)
```quint
run concurrentRequestTest = {
    init
    .then(requestScale(5))
    .then(acknowledgeScale)
    .then(progressScale)  // currentReplicas = 1
    .then(requestScale(10))  // New request while scaling
    .then(all {
        assert(scaleStatus == Requested),  // Back to Requested
        assert(targetReplicas == 10),
        assert(lastAcknowledgedTarget == 5),
    })
}
```

## 7. Correctness Criteria Summary

| Property | Type | Description |
|----------|------|-------------|
| Non-negative replicas | Safety | `currentReplicas >= 0 ∧ targetReplicas >= 0` |
| Valid status derivation | Safety | Status always computable from state |
| Finished consistency | Safety | `Finished ⟹ current = target` |
| Running acknowledged | Safety | `Running ⟹ acknowledged = target` |
| Eventual acknowledgment | Liveness | Requests eventually acknowledged |
| Eventual completion | Liveness | Scaling eventually completes |
| Convergence | Liveness | Replicas converge to target |

## 8. Edge Cases to Consider

1. Scale to zero (valid in K8s)
2. Scale to same value (no-op)
3. Rapid successive scale requests
4. Controller restart (reset `lastAcknowledgedTarget`)
5. Pod failures during scaling (external perturbation of `currentReplicas`)