$ErrorActionPreference = "Stop"

Write-Host "[compose-reset] stopping cluster and removing volumes"
docker compose down -v

Write-Host "[compose-reset] pruning dangling resources"
docker system prune -f

Write-Host "[compose-reset] done"
