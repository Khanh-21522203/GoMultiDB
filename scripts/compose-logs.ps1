param(
  [string]$Service = "",
  [int]$Tail = 200,
  [switch]$Follow
)

$ErrorActionPreference = "Stop"

$svcArg = ""
if ($Service -ne "") {
  $svcArg = $Service
}
$followArg = ""
if ($Follow) {
  $followArg = "-f"
}

$cmd = "docker compose logs --tail $Tail $followArg $svcArg"
Write-Host "[compose-logs] $cmd"
Invoke-Expression $cmd
