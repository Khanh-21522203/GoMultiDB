param(
  [string]$Profile = "",
  [switch]$Build
)

$ErrorActionPreference = "Stop"

$profileArg = ""
if ($Profile -ne "") {
  $profileArg = "--profile $Profile"
}
$buildArg = ""
if ($Build) {
  $buildArg = "--build"
}

$cmd = "docker compose $profileArg up -d $buildArg"
Write-Host "[compose-up] $cmd"
Invoke-Expression $cmd
Write-Host "[compose-up] cluster started"
