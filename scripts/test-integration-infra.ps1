param(
  [switch]$KeepUp
)

$ErrorActionPreference = "Stop"

Write-Host "[integration] running real-infra integration tests"
go test ./tests/integration/infra -tags integration -v

if (-not $KeepUp) {
  Write-Host "[integration] auto teardown"
  ./scripts/compose-down.ps1 | Out-Null
}

Write-Host "[integration] completed"
