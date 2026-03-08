param(
  [ValidateSet("quick","standard")]
  [string]$Profile = "quick",
  [ValidateSet("local","compose")]
  [string]$Mode = "local",
  [switch]$KeepUp
)

$ErrorActionPreference = "Stop"

if ($Profile -eq "quick") {
  $env:STRESS_PROFILE = "quick"
} else {
  $env:STRESS_PROFILE = "standard"
}
$env:STRESS_MODE = $Mode

Write-Host "[stress] profile=$($env:STRESS_PROFILE) mode=$($env:STRESS_MODE)"

function Test-DockerReady {
  try {
    docker info *> $null
    return $true
  } catch {
    return $false
  }
}

if ($Mode -eq "compose") {
  if (-not (Test-DockerReady)) {
    throw "Docker daemon is not available. Start Docker Desktop and retry."
  }

  ./scripts/compose-up.ps1 -Profile stress -Build
  try {
    ./scripts/compose-smoke.ps1
    go test ./tests/stress -tags stress -v

    if (Test-Path "tests/stress/artifacts") {
      New-Item -ItemType Directory -Force -Path "tests/stress/artifacts" | Out-Null
    }
    docker compose logs --no-color | Out-File -FilePath "tests/stress/artifacts/compose-logs.txt" -Encoding utf8
  } finally {
    if (-not $KeepUp) {
      ./scripts/compose-down.ps1
    }
  }
} else {
  go test ./tests/stress -tags stress -v
}

Write-Host "[stress] done"
