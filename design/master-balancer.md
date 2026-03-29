# Master Balancer

### Purpose
Computes per-round replica and leader balancing actions from cluster placement snapshots using bounded concurrency budgets and cooldown windows.

### Scope
**In scope:**
- Planner config/defaults and cooldown map.
- Round planning for under-replication, over-replication, and leader imbalance.
- Placement violation diagnostics.

**Out of scope:**
- Actual execution of emitted actions on tablet servers.

### Primary User Flow
1. Control loop provides current `ClusterState`.
2. Planner emits ordered `BalanceAction` list (`add_replica`, `remove_replica`, `move_leader`).
3. Caller executes actions and reports success/failure (`NotifyActionResult`).

### System Flow
1. Entry: `balancer.Planner.PlanBalanceRound(state)`.
2. Priority stages:
   - Under-replicated tablets -> choose add candidate.
   - Over-replicated tablets -> choose removal victim.
   - Optional leader balancing -> move leader from overloaded to underloaded replica.
3. Each emitted action updates optimistic node load and writes tablet cooldown timestamp.

### Data Model
- `NodeLoad`:
  - `NodeID`, `ReplicaCount`, `LeaderCount`, `IsLive`, `Placement`.
- `TabletPlacement`:
  - `TabletID`, `Replicas []ReplicaPlacement`, `RF`.
- `BalanceAction`:
  - `Type`, `TabletID`, `FromNode`, `ToNode`, `Reason`.
- `Planner.cooldown map[tabletID]time.Time` is in-memory only.

### Interfaces and Contracts
- `PlanBalanceRound(state)` returns best-effort actions up to configured caps.
- `NotifyActionResult(tabletID, success)` clears cooldown when action fails.
- `GetPlacementViolations(state)` reports unsatisfied RF constraints due to live node/zone shortages.

### Dependencies
**Internal modules:**
- None outside package.

**External services/libraries:**
- None; deterministic algorithm over provided state.

### Failure Modes and Edge Cases
- No suitable node for add/remove/leader move -> action skipped.
- Cooldown can suppress repeated actions for same tablet until window expires.
- If leader balancing disabled, only replica count actions are emitted.
- Violations report when distinct placement labels or live nodes are insufficient for RF.

### Observability and Debugging
- Debug decision points:
  - `balancer.go:PlanBalanceRound`
  - helper selectors `pickAddNode`, `pickRemoveNode`, `pickLeaderMove`.
- Coverage: `internal/master/balancer/balancer_test.go`.

### Risks and Notes
- Planner assumes caller supplies accurate `NodeLoad`/`TabletPlacement`; stale inputs can produce suboptimal actions.
- Leader movement is heuristic and does not consult raft term/lease details.

Changes:

- Redefine balancer objectives after Raft removal, including whether RF-based add/remove actions remain supported.
- Remove or replace leader-move planning with post-Raft shard ownership distribution rules.
