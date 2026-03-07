package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Harness struct {
	RepoRoot     string
	ArtifactsDir string
	Timeout      time.Duration
	Retries      int
}

type Summary struct {
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Passed     bool              `json:"passed"`
	Scenarios  map[string]string `json:"scenarios"`
	Details    []string          `json:"details"`
}

func NewHarness(repoRoot string) *Harness {
	return &Harness{
		RepoRoot:     repoRoot,
		ArtifactsDir: filepath.Join(repoRoot, "tests", "integration", "infra", "artifacts"),
		Timeout:      3 * time.Minute,
		Retries:      3,
	}
}

func (h *Harness) EnsureArtifactsDir() error {
	return os.MkdirAll(h.ArtifactsDir, 0o755)
}

func (h *Harness) ComposeUp(ctx context.Context) error {
	return h.runCompose(ctx, "up", "-d", "--build")
}

func (h *Harness) ComposeDown(ctx context.Context) error {
	return h.runCompose(ctx, "down", "-v")
}

func (h *Harness) RestartService(ctx context.Context, service string) error {
	if strings.TrimSpace(service) == "" {
		return fmt.Errorf("service is required")
	}
	return h.runCompose(ctx, "restart", service)
}

func (h *Harness) WaitHealthy(ctx context.Context, containers ...string) error {
	deadline := time.Now().Add(h.Timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("health wait timeout reached")
		}
		allHealthy := true
		for _, c := range containers {
			health, err := h.containerHealth(ctx, c)
			if err != nil {
				allHealthy = false
				break
			}
			if health != "healthy" {
				allHealthy = false
				break
			}
		}
		if allHealthy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (h *Harness) Retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for i := 0; i < h.Retries; i++ {
		if err := fn(); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(i+1) * time.Second):
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (h *Harness) CaptureLogs(ctx context.Context) (string, error) {
	if err := h.EnsureArtifactsDir(); err != nil {
		return "", err
	}
	outPath := filepath.Join(h.ArtifactsDir, "compose-logs.txt")
	cmd := exec.CommandContext(ctx, "docker", "compose", "logs", "--no-color")
	cmd.Dir = h.RepoRoot
	b, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.WriteFile(outPath, b, 0o644)
		return outPath, fmt.Errorf("capture compose logs: %w", err)
	}
	if err := os.WriteFile(outPath, b, 0o644); err != nil {
		return "", err
	}
	return outPath, nil
}

func (h *Harness) WriteSummary(summary Summary) (string, error) {
	if err := h.EnsureArtifactsDir(); err != nil {
		return "", err
	}
	outPath := filepath.Join(h.ArtifactsDir, "summary.json")
	b, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(outPath, b, 0o644); err != nil {
		return "", err
	}
	return outPath, nil
}

func (h *Harness) runCompose(ctx context.Context, args ...string) error {
	fullArgs := append([]string{"compose"}, args...)
	cmd := exec.CommandContext(ctx, "docker", fullArgs...)
	cmd.Dir = h.RepoRoot
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(fullArgs, " "), err, string(b))
	}
	return nil
}

func (h *Harness) containerHealth(ctx context.Context, container string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", "--format={{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}", container)
	cmd.Dir = h.RepoRoot
	b, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
