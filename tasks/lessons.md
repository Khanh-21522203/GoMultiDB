# Lessons Learned

## 2026-03-01 — Verification source must be concrete

- Mistake pattern: assuming `go test ./...` result from local tool execution when sandbox constraints can invalidate that run.
- Rule: when execution environment is constrained, only mark verification complete using explicit terminal evidence from the project terminal output.
- Prevention: before reporting test/lint status, read latest terminal output and cite the observed exit code/result.
