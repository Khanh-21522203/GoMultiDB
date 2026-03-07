param(
  [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"

$cmd = "go test ./..."
if ($VerboseOutput) {
  $cmd = "$cmd -v"
}

Write-Host "[standard] running: $cmd"
Invoke-Expression $cmd
Write-Host "[standard] success"
