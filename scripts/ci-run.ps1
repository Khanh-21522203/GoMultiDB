param(
  [switch]$WithStress,
  [string]$StressProfile = "quick"
)

$ErrorActionPreference = "Stop"

Write-Host "[ci] stage=lint (placeholder)"
# Placeholder lint stage: rely on IDE lint + go test compile checks.

Write-Host "[ci] stage=unit-quick"
./scripts/test-quick.ps1

Write-Host "[ci] stage=unit-standard"
./scripts/test-standard.ps1

Write-Host "[ci] stage=integration-infra"
./scripts/test-integration-infra.ps1

if ($WithStress) {
  Write-Host "[ci] stage=stress profile=$StressProfile"
  ./scripts/stress-run.ps1 -Profile $StressProfile
  ./scripts/stress-report.ps1
}

Write-Host "[ci] all selected stages passed"
