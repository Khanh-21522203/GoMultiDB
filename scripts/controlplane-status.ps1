$ErrorActionPreference = "Stop"

Write-Host "[controlplane-status] This scaffold reports where to inspect replication state."
Write-Host "- CDC status and lag: internal/replication/cdc Store.Status and Store.LagSnapshot"
Write-Host "- xCluster status: internal/replication/xcluster Loop.Status"
Write-Host "- Unified snapshot: internal/replication/controlplane Registry.Snapshot"
Write-Host "- Ownership/failover routing checks: internal/testing/invariant ownership_convergence + routing_consistency"
Write-Host "Use go test outputs and integration tests as validation source in this phase."
