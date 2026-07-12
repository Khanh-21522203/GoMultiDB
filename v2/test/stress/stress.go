package stress

import (
	"math/rand"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// ScenarioName identifies one of the fixed stress-test scenarios.
type ScenarioName string

// ScenarioName values enumerating the stress-test scenario suite.
const (
	// ScenarioS1Steady drives a steady-state workload with no faults.
	ScenarioS1Steady ScenarioName = "S1_steady_state"
	// ScenarioS2Bursty drives a bursty workload with periodic fault injection.
	ScenarioS2Bursty ScenarioName = "S2_bursty_with_faults"
	// ScenarioS3Scale drives a workload while scaling out the cluster.
	ScenarioS3Scale ScenarioName = "S3_scale_out"
	// ScenarioS4Soak drives a long-running, low-rate soak workload.
	ScenarioS4Soak ScenarioName = "S4_soak"
)

// Workload describes the shape of load a scenario drives: iteration
// counts, stream fan-out, event volume per stream, fault cadence, and soak
// duration.
type Workload struct {
	Iterations          int `json:"iterations"`
	Streams             int `json:"streams"`
	EventsPerStream     int `json:"events_per_stream"`
	FaultEveryN         int `json:"fault_every_n"`
	SoakDurationSeconds int `json:"soak_duration_seconds"`
}

// Thresholds are the pass/fail bounds a scenario's ScenarioResult is
// graded against.
type Thresholds struct {
	Throughput      float64 `json:"throughput"`
	RetryRatio      float64 `json:"retry_ratio"`
	LagUpper        uint64  `json:"lag_upper"`
	CheckpointStale uint64  `json:"checkpoint_staleness"`
}

// ScenarioConfig pairs a scenario's Workload shape with its Thresholds.
type ScenarioConfig struct {
	Workload   Workload   `json:"workload"`
	Thresholds Thresholds `json:"thresholds"`
}

// Config is the top-level stress-test configuration: a random seed and the
// ScenarioConfig for every ScenarioName in the suite.
type Config struct {
	Seed      int64                           `json:"seed"`
	Scenarios map[ScenarioName]ScenarioConfig `json:"scenarios"`
}

// ScenarioResult captures the measured outcome of running one scenario.
type ScenarioResult struct {
	Scenario            ScenarioName `json:"scenario"`
	Passed              bool         `json:"passed"`
	DurationMillis      int64        `json:"duration_ms"`
	Throughput          float64      `json:"throughput"`
	RetryRatio          float64      `json:"retry_ratio"`
	LagUpper            uint64       `json:"lag_upper"`
	CheckpointStaleness uint64       `json:"checkpoint_staleness"`
	Details             []string     `json:"details"`
}

// Summary reports the outcome of a full stress-test run across all
// scenarios, plus aggregate run-duration percentiles.
type Summary struct {
	GeneratedAt  time.Time        `json:"generated_at"`
	Seed         int64            `json:"seed"`
	Mode         string           `json:"mode"`
	Results      []ScenarioResult `json:"results"`
	Passed       bool             `json:"passed"`
	TopOffenders []string         `json:"top_offenders"`
	RunDurations []int64          `json:"run_durations_ms,omitempty"`
	P50Millis    int64            `json:"p50_ms,omitempty"`
	P95Millis    int64            `json:"p95_ms,omitempty"`
}

// Harness drives a stress-test run: it holds the resolved Config, a
// seeded random source shared across scenario generators, and the
// artifacts directory results are written to.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
type Harness struct {
	Config       Config
	Rand         *rand.Rand
	ArtifactsDir string
}

// DefaultQuickConfig returns a Config sized for a fast, low-volume run of
// every scenario.
func DefaultQuickConfig() Config {
	return Config{
		Seed: 20260307,
		Scenarios: map[ScenarioName]ScenarioConfig{
			ScenarioS1Steady: {
				Workload:   Workload{Iterations: 100, Streams: 4, EventsPerStream: 200, FaultEveryN: 0, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 100, RetryRatio: 0.05, LagUpper: 0, CheckpointStale: 10},
			},
			ScenarioS2Bursty: {
				Workload:   Workload{Iterations: 100, Streams: 2, EventsPerStream: 200, FaultEveryN: 25, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 80, RetryRatio: 0.20, LagUpper: 20, CheckpointStale: 50},
			},
			ScenarioS3Scale: {
				Workload:   Workload{Iterations: 100, Streams: 8, EventsPerStream: 100, FaultEveryN: 0, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 90, RetryRatio: 0.05, LagUpper: 25, CheckpointStale: 60},
			},
			ScenarioS4Soak: {
				Workload:   Workload{Iterations: 0, Streams: 1, EventsPerStream: 0, FaultEveryN: 0, SoakDurationSeconds: 10},
				Thresholds: Thresholds{Throughput: 20, RetryRatio: 0.05, LagUpper: 5, CheckpointStale: 10},
			},
		},
	}
}

// DefaultStandardConfig returns DefaultQuickConfig scaled up to a
// standard-volume run of every scenario.
func DefaultStandardConfig() Config {
	cfg := DefaultQuickConfig()
	cfg.Scenarios[ScenarioS1Steady] = ScenarioConfig{
		Workload:   Workload{Iterations: 400, Streams: 12, EventsPerStream: 1000, FaultEveryN: 0, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 200, RetryRatio: 0.05, LagUpper: 0, CheckpointStale: 20},
	}
	cfg.Scenarios[ScenarioS2Bursty] = ScenarioConfig{
		Workload:   Workload{Iterations: 400, Streams: 6, EventsPerStream: 1000, FaultEveryN: 25, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 150, RetryRatio: 0.25, LagUpper: 30, CheckpointStale: 100},
	}
	cfg.Scenarios[ScenarioS3Scale] = ScenarioConfig{
		Workload:   Workload{Iterations: 400, Streams: 24, EventsPerStream: 400, FaultEveryN: 0, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 160, RetryRatio: 0.05, LagUpper: 50, CheckpointStale: 120},
	}
	cfg.Scenarios[ScenarioS4Soak] = ScenarioConfig{
		Workload:   Workload{Iterations: 0, Streams: 1, EventsPerStream: 0, FaultEveryN: 0, SoakDurationSeconds: 30},
		Thresholds: Thresholds{Throughput: 30, RetryRatio: 0.05, LagUpper: 10, CheckpointStale: 20},
	}
	return cfg
}

// NewHarness returns a Harness for config, rooted at repoRoot, with its
// random source seeded from config.Seed (or the current time if unset)
// and its artifacts directory under tests/stress/artifacts.
func NewHarness(config Config, repoRoot string) *Harness {
	return &Harness{}
}

// Scenario returns the ScenarioConfig registered under name, or the zero
// ScenarioConfig if none is registered.
//
// Not yet implemented in this scaffold.
func (h *Harness) Scenario(name ScenarioName) ScenarioConfig {
	panic("not implemented")
}

// EnsureArtifacts creates the harness's artifacts directory if it does not
// already exist.
//
// Not yet implemented in this scaffold.
func (h *Harness) EnsureArtifacts() error {
	return errors.ErrNotImplemented
}

// WriteSummary writes sum as JSON to the artifacts directory and returns
// the path written.
//
// Not yet implemented in this scaffold.
func (h *Harness) WriteSummary(sum Summary) (string, error) {
	return "", errors.ErrNotImplemented
}

// WriteConfig writes h.Config as JSON to the artifacts directory and
// returns the path written.
//
// Not yet implemented in this scaffold.
func (h *Harness) WriteConfig() (string, error) {
	return "", errors.ErrNotImplemented
}

// EvaluateThresholds grades r against sc.Thresholds, setting r.Passed and
// appending a diagnostic detail for each threshold r fails.
//
// Not yet implemented in this scaffold.
func EvaluateThresholds(sc ScenarioConfig, r ScenarioResult) ScenarioResult {
	panic("not implemented")
}

// BuildSummary assembles a Summary from a run's per-scenario results,
// recording which scenarios failed as TopOffenders.
//
// Not yet implemented in this scaffold.
func BuildSummary(seed int64, mode string, results []ScenarioResult) Summary {
	panic("not implemented")
}

// WithRunStats attaches sorted run durations and their p50/p95 to sum,
// returning sum unchanged if runDurations is empty.
//
// Not yet implemented in this scaffold.
func WithRunStats(sum Summary, runDurations []int64) Summary {
	panic("not implemented")
}
