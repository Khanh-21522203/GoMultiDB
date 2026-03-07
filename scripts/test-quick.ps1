param(
  [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"

$cmd = "go test ./internal/replication/cdc ./internal/replication/xcluster"
if ($VerboseOutput) {
  $cmd = "$cmd -v"
}

Write-Host "[quick] running: $cmd"
Invoke-Expression $cmd
Write-Host "[quick] success"
