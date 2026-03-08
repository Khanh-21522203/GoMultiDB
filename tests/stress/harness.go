package stress

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type ScenarioName string

const (
	ScenarioS1Steady ScenarioName = "S1_steady_state"
	ScenarioS2Bursty ScenarioName = "S2_bursty_with_faults"
	ScenarioS3Scale  ScenarioName = "S3_scale_out"
	ScenarioS4Soak   ScenarioName = "S4_soak"
)

type Workload struct {
	Iterations          int `json:"iterations"`
	Streams             int `json:"streams"`
	EventsPerStream     int `json:"events_per_stream"`
	FaultEveryN         int `json:"fault_every_n"`
	SoakDurationSeconds int `json:"soak_duration_seconds"`
}

type Thresholds struct {
	Throughput        float64 `json:"throughput"`
	RetryRatio        float64 `json:"retry_ratio"`
	LagUpper          uint64  `json:"lag_upper"`
	CheckpointStale   uint64  `json:"checkpoint_staleness"`
}

type ScenarioConfig struct {
	Workload   Workload   `json:"workload"`
	Thresholds Thresholds `json:"thresholds"`
}

type Config struct {
	Seed      int64                      `json:"seed"`
	Scenarios map[ScenarioName]ScenarioConfig `json:"scenarios"`
}

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

type Summary struct {
	GeneratedAt   time.Time        `json:"generated_at"`
	Seed          int64            `json:"seed"`
	Mode          string           `json:"mode"`
	Results       []ScenarioResult `json:"results"`
	Passed        bool             `json:"passed"`
	TopOffenders  []string         `json:"top_offenders"`
	RunDurations  []int64          `json:"run_durations_ms,omitempty"`
	P50Millis     int64            `json:"p50_ms,omitempty"`
	P95Millis     int64            `json:"p95_ms,omitempty"`
}

type Harness struct {
	Config       Config
	Rand         *rand.Rand
	ArtifactsDir string
}

func DefaultQuickConfig() Config {
	return Config{
		Seed: 20260307,
		Scenarios: map[ScenarioName]ScenarioConfig{
			ScenarioS1Steady: {
				Workload: Workload{Iterations: 100, Streams: 4, EventsPerStream: 200, FaultEveryN: 0, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 100, RetryRatio: 0.05, LagUpper: 0, CheckpointStale: 10},
			},
			ScenarioS2Bursty: {
				Workload: Workload{Iterations: 100, Streams: 2, EventsPerStream: 200, FaultEveryN: 25, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 80, RetryRatio: 0.20, LagUpper: 20, CheckpointStale: 50},
			},
			ScenarioS3Scale: {
				Workload: Workload{Iterations: 100, Streams: 8, EventsPerStream: 100, FaultEveryN: 0, SoakDurationSeconds: 0},
				Thresholds: Thresholds{Throughput: 90, RetryRatio: 0.05, LagUpper: 25, CheckpointStale: 60},
			},
			ScenarioS4Soak: {
				Workload: Workload{Iterations: 0, Streams: 1, EventsPerStream: 0, FaultEveryN: 0, SoakDurationSeconds: 10},
				Thresholds: Thresholds{Throughput: 20, RetryRatio: 0.05, LagUpper: 5, CheckpointStale: 10},
			},
		},
	}
}

func DefaultStandardConfig() Config {
	cfg := DefaultQuickConfig()
	cfg.Scenarios[ScenarioS1Steady] = ScenarioConfig{
		Workload: Workload{Iterations: 400, Streams: 12, EventsPerStream: 1000, FaultEveryN: 0, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 200, RetryRatio: 0.05, LagUpper: 0, CheckpointStale: 20},
	}
	cfg.Scenarios[ScenarioS2Bursty] = ScenarioConfig{
		Workload: Workload{Iterations: 400, Streams: 6, EventsPerStream: 1000, FaultEveryN: 25, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 150, RetryRatio: 0.25, LagUpper: 30, CheckpointStale: 100},
	}
	cfg.Scenarios[ScenarioS3Scale] = ScenarioConfig{
		Workload: Workload{Iterations: 400, Streams: 24, EventsPerStream: 400, FaultEveryN: 0, SoakDurationSeconds: 0},
		Thresholds: Thresholds{Throughput: 160, RetryRatio: 0.05, LagUpper: 50, CheckpointStale: 120},
	}
	cfg.Scenarios[ScenarioS4Soak] = ScenarioConfig{
		Workload: Workload{Iterations: 0, Streams: 1, EventsPerStream: 0, FaultEveryN: 0, SoakDurationSeconds: 30},
		Thresholds: Thresholds{Throughput: 30, RetryRatio: 0.05, LagUpper: 10, CheckpointStale: 20},
	}
	return cfg
}

func NewHarness(config Config, repoRoot string) *Harness {
	if config.Seed == 0 {
		config.Seed = time.Now().UnixNano()
	}
	return &Harness{
		Config:       config,
		Rand:         rand.New(rand.NewSource(config.Seed)),
		ArtifactsDir: filepath.Join(repoRoot, "tests", "stress", "artifacts"),
	}
}

func (h *Harness) Scenario(name ScenarioName) ScenarioConfig {
	if sc, ok := h.Config.Scenarios[name]; ok {
		return sc
	}
	return ScenarioConfig{}
}

func (h *Harness) EnsureArtifacts() error {
	return os.MkdirAll(h.ArtifactsDir, 0o755)
}

func (h *Harness) WriteSummary(sum Summary) (string, error) {
	if err := h.EnsureArtifacts(); err != nil {
		return "", err
	}
	path := filepath.Join(h.ArtifactsDir, "summary.json")
	b, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func (h *Harness) WriteConfig() (string, error) {
	if err := h.EnsureArtifacts(); err != nil {
		return "", err
	}
	path := filepath.Join(h.ArtifactsDir, "config.json")
	b, err := json.MarshalIndent(h.Config, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func EvaluateThresholds(sc ScenarioConfig, r ScenarioResult) ScenarioResult {
	r.Passed = true
	if r.Throughput < sc.Thresholds.Throughput {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("throughput below threshold: got %.2f need >= %.2f", r.Throughput, sc.Thresholds.Throughput))
	}
	if r.RetryRatio > sc.Thresholds.RetryRatio {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("retry ratio above threshold: got %.4f need <= %.4f", r.RetryRatio, sc.Thresholds.RetryRatio))
	}
	if r.LagUpper > sc.Thresholds.LagUpper {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("lag upper above threshold: got %d need <= %d", r.LagUpper, sc.Thresholds.LagUpper))
	}
	if r.CheckpointStaleness > sc.Thresholds.CheckpointStale {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("checkpoint staleness above threshold: got %d need <= %d", r.CheckpointStaleness, sc.Thresholds.CheckpointStale))
	}
	return r
}

func BuildSummary(seed int64, mode string, results []ScenarioResult) Summary {
	s := Summary{GeneratedAt: time.Now().UTC(), Seed: seed, Mode: mode, Results: results, Passed: true}
	offenders := make([]string, 0)
	for _, r := range results {
		if !r.Passed {
			s.Passed = false
			offenders = append(offenders, string(r.Scenario))
		}
	}
	sort.Strings(offenders)
	s.TopOffenders = offenders
	return s
}

func WithRunStats(sum Summary, runDurations []int64) Summary {
	if len(runDurations) == 0 {
		return sum
	}
	d := append([]int64(nil), runDurations...)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	sum.RunDurations = d
	sum.P50Millis = percentile(d, 50)
	sum.P95Millis = percentile(d, 95)
	return sum
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (p*(len(sorted)-1) + 50) / 100
	return sorted[idx]
}
