// Package integration provides Harness, a docker-compose-based integration
// test driver: it brings the compose environment up and down, restarts
// individual services, waits for container health, retries flaky
// operations, and captures compose logs and a run Summary as artifacts
// under tests/integration/infra/artifacts. It is consumed by the v2
// integration test suites exercising a multi-service, containerized
// deployment of GoMultiDB v2.
// This is scaffold-only; behavior is unimplemented.
package integration
