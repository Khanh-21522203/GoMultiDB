param(
  [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"

Write-Host "[preflight] phase7 preflight start"

if ($VerboseOutput) {
  ./scripts/test-quick.ps1 -VerboseOutput
  ./scripts/test-standard.ps1 -VerboseOutput
} else {
  ./scripts/test-quick.ps1
  ./scripts/test-standard.ps1
}

Write-Host "[preflight] success"
