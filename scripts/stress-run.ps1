param(
  [ValidateSet("quick","standard")]
  [string]$Profile = "quick"
)

$ErrorActionPreference = "Stop"

if ($Profile -eq "quick") {
  $env:STRESS_PROFILE = "quick"
} else {
  $env:STRESS_PROFILE = "standard"
}

Write-Host "[stress] profile=$($env:STRESS_PROFILE)"
go test ./tests/stress -tags stress -v
Write-Host "[stress] done"
