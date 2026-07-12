package integration

import (
	"context"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// Harness drives a docker-compose-based integration test environment:
// bringing services up and down, restarting individual services, waiting
// for container health, retrying flaky operations, and capturing compose
// logs and a run Summary as artifacts.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Harness struct {
	RepoRoot     string
	ArtifactsDir string
	Timeout      time.Duration
	Retries      int
}

// Summary reports the outcome of an integration test run: overall
// pass/fail, per-scenario results, and free-form diagnostic details.
type Summary struct {
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Passed     bool              `json:"passed"`
	Scenarios  map[string]string `json:"scenarios"`
	Details    []string          `json:"details"`
}

// NewHarness returns a Harness rooted at repoRoot, with its artifacts
// directory under tests/integration/infra/artifacts and default timeout
// and retry settings.
func NewHarness(repoRoot string) *Harness {
	return &Harness{}
}

// EnsureArtifactsDir creates the harness's artifacts directory if it does
// not already exist.
//
// Not yet implemented in this scaffold.
func (h *Harness) EnsureArtifactsDir() error {
	return errors.ErrNotImplemented
}

// ComposeUp brings up the docker-compose environment, building images as
// needed.
//
// Not yet implemented in this scaffold.
func (h *Harness) ComposeUp(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// ComposeDown tears down the docker-compose environment and removes its
// volumes.
//
// Not yet implemented in this scaffold.
func (h *Harness) ComposeDown(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// RestartService restarts a single named docker-compose service.
//
// Not yet implemented in this scaffold.
func (h *Harness) RestartService(ctx context.Context, service string) error {
	return errors.ErrNotImplemented
}

// WaitHealthy blocks until every named container reports a healthy status,
// or returns an error once h.Timeout elapses.
//
// Not yet implemented in this scaffold.
func (h *Harness) WaitHealthy(ctx context.Context, containers ...string) error {
	return errors.ErrNotImplemented
}

// Retry calls fn up to h.Retries times with a backing-off delay between
// attempts, returning the last error if every attempt fails.
//
// Not yet implemented in this scaffold.
func (h *Harness) Retry(ctx context.Context, fn func() error) error {
	return errors.ErrNotImplemented
}

// CaptureLogs writes the combined docker-compose logs to the artifacts
// directory and returns the path written.
//
// Not yet implemented in this scaffold.
func (h *Harness) CaptureLogs(ctx context.Context) (string, error) {
	return "", errors.ErrNotImplemented
}

// WriteSummary writes summary as JSON to the artifacts directory and
// returns the path written.
//
// Not yet implemented in this scaffold.
func (h *Harness) WriteSummary(summary Summary) (string, error) {
	return "", errors.ErrNotImplemented
}
