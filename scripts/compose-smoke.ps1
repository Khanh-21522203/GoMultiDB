$ErrorActionPreference = "Stop"

$services = @("multidb-master", "multidb-tserver-1", "multidb-tserver-2", "multidb-tserver-3")
$maxAttempts = 30
$intervalSec = 2

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
  $allHealthy = $true
  foreach ($svc in $services) {
    $health = docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' $svc 2>$null
    if ($health -ne "healthy") {
      $allHealthy = $false
      break
    }
  }
  if ($allHealthy) {
    Write-Host "[compose-smoke] all services healthy"
    exit 0
  }
  Start-Sleep -Seconds $intervalSec
}

Write-Error "[compose-smoke] services did not become healthy in time"
exit 1
