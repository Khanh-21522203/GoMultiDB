param(
  [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"

$cmd = "go test ./internal/master/balancer ./internal/testing/invariant ./internal/replication/cdc ./internal/replication/xcluster ./internal/server"
if ($VerboseOutput) {
  $cmd = "$cmd -v"
}

Write-Host "[quick] running: $cmd"
Invoke-Expression $cmd
Write-Host "[quick] success"
