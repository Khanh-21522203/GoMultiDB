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

type Config struct {
	Seed                 int64         `json:"seed"`
	Scenario             ScenarioName  `json:"scenario"`
	Iterations           int           `json:"iterations"`
	Streams              int           `json:"streams"`
	EventsPerStream      int           `json:"events_per_stream"`
	FaultEveryN          int           `json:"fault_every_n"`
	SoakDurationSeconds  int           `json:"soak_duration_seconds"`
	ThresholdThroughput  float64       `json:"threshold_throughput"`
	ThresholdRetryRatio  float64       `json:"threshold_retry_ratio"`
	ThresholdLagUpper    uint64        `json:"threshold_lag_upper"`
	ThresholdCheckpointStale uint64    `json:"threshold_checkpoint_stale"`
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
	GeneratedAt time.Time        `json:"generated_at"`
	Seed        int64            `json:"seed"`
	Results     []ScenarioResult `json:"results"`
	Passed      bool             `json:"passed"`
	TopOffenders []string        `json:"top_offenders"`
}

type Harness struct {
	Config      Config
	Rand        *rand.Rand
	ArtifactsDir string
}

func DefaultQuickConfig() Config {
	return Config{
		Seed:                    20260307,
		Scenario:                ScenarioS1Steady,
		Iterations:              100,
		Streams:                 4,
		EventsPerStream:         200,
		FaultEveryN:             25,
		SoakDurationSeconds:     10,
		ThresholdThroughput:     100,
		ThresholdRetryRatio:     0.20,
		ThresholdLagUpper:       10,
		ThresholdCheckpointStale: 50,
	}
}

func DefaultStandardConfig() Config {
	cfg := DefaultQuickConfig()
	cfg.Iterations = 400
	cfg.Streams = 12
	cfg.EventsPerStream = 1000
	cfg.SoakDurationSeconds = 30
	cfg.ThresholdThroughput = 200
	cfg.ThresholdRetryRatio = 0.25
	cfg.ThresholdLagUpper = 20
	cfg.ThresholdCheckpointStale = 100
	return cfg
}

func NewHarness(config Config, repoRoot string) *Harness {
	if config.Seed == 0 {
		config.Seed = time.Now().UnixNano()
	}
	return &Harness{
		Config:      config,
		Rand:        rand.New(rand.NewSource(config.Seed)),
		ArtifactsDir: filepath.Join(repoRoot, "tests", "stress", "artifacts"),
	}
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

func EvaluateThresholds(cfg Config, r ScenarioResult) ScenarioResult {
	r.Passed = true
	if r.Throughput < cfg.ThresholdThroughput {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("throughput below threshold: got %.2f need >= %.2f", r.Throughput, cfg.ThresholdThroughput))
	}
	if r.RetryRatio > cfg.ThresholdRetryRatio {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("retry ratio above threshold: got %.4f need <= %.4f", r.RetryRatio, cfg.ThresholdRetryRatio))
	}
	if r.LagUpper > cfg.ThresholdLagUpper {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("lag upper above threshold: got %d need <= %d", r.LagUpper, cfg.ThresholdLagUpper))
	}
	if r.CheckpointStaleness > cfg.ThresholdCheckpointStale {
		r.Passed = false
		r.Details = append(r.Details, fmt.Sprintf("checkpoint staleness above threshold: got %d need <= %d", r.CheckpointStaleness, cfg.ThresholdCheckpointStale))
	}
	return r
}

func BuildSummary(seed int64, results []ScenarioResult) Summary {
	s := Summary{GeneratedAt: time.Now().UTC(), Seed: seed, Results: results, Passed: true}
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
