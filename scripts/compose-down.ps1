param(
  [switch]$RemoveVolumes
)

$ErrorActionPreference = "Stop"

$volArg = ""
if ($RemoveVolumes) {
  $volArg = "-v"
}

$cmd = "docker compose down $volArg"
Write-Host "[compose-down] $cmd"
Invoke-Expression $cmd
Write-Host "[compose-down] cluster stopped"
