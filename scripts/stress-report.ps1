$ErrorActionPreference = "Stop"

$summaryPath = "tests/stress/artifacts/summary.json"
if (!(Test-Path $summaryPath)) {
  Write-Host "[stress-report] summary not found at $summaryPath"
  exit 1
}

$summary = Get-Content $summaryPath -Raw | ConvertFrom-Json
Write-Host "[stress-report] generated_at: $($summary.generated_at)"
Write-Host "[stress-report] seed: $($summary.seed)"
Write-Host "[stress-report] passed: $($summary.passed)"

Write-Host "[stress-report] scenario results:"
foreach ($r in $summary.results) {
  Write-Host ("- {0}: passed={1}, throughput={2}, retry_ratio={3}, lag_upper={4}, checkpoint_staleness={5}" -f $r.scenario, $r.passed, $r.throughput, $r.retry_ratio, $r.lag_upper, $r.checkpoint_staleness)
}

if ($summary.top_offenders.Count -gt 0) {
  Write-Host "[stress-report] top offenders: $($summary.top_offenders -join ', ')"
} else {
  Write-Host "[stress-report] no offenders"
}
