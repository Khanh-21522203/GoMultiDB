$ErrorActionPreference = "Stop"

Write-Host "[compose-status] docker compose ps"
docker compose ps

Write-Host ""
Write-Host "[compose-status] health summary"
$services = @("multidb-master", "multidb-tserver-1", "multidb-tserver-2", "multidb-tserver-3")
foreach ($svc in $services) {
  $health = docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' $svc 2>$null
  if (-not $health) { $health = "not-found" }
  Write-Host ("- {0}: {1}" -f $svc, $health)
}
