package sql

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

// PGProcess manages a PostgreSQL (or compatible) subprocess.
type PGProcess struct {
	mu           sync.Mutex
	cmd          *exec.Cmd
	cfg          PGProcessConfig
	started      bool
	proc         *os.Process
	cancel       context.CancelFunc
}

// PGProcessConfig configures the PostgreSQL process.
type PGProcessConfig struct {
	DataDir      string
	BindAddress  string
	Port         int
	BinPath      string // path to postgres binary; empty uses system PATH
	InitDBPath   string // path to initdb binary
	HBAConfig    string
	ExtraConf    map[string]string
	StartTimeout time.Duration // how long to wait for postmaster to accept connections
	StopTimeout  time.Duration // how long to wait for clean shutdown
	// HBAConfigContent is the pg_hba.conf content; if empty, uses default trust all.
	HBAConfigContent string
}

// NewPGProcess creates a new PGProcess manager.
func NewPGProcess(cfg PGProcessConfig) (*PGProcess, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir is required")
	}
	if cfg.Port == 0 {
		cfg.Port = 5433
	}
	if cfg.BindAddress == "" {
		cfg.BindAddress = "127.0.0.1"
	}
	if cfg.StartTimeout == 0 {
		cfg.StartTimeout = 30 * time.Second
	}
	if cfg.StopTimeout == 0 {
		cfg.StopTimeout = 10 * time.Second
	}
	return &PGProcess{
		cfg: cfg,
	}, nil
}

// InitDB creates a new PostgreSQL data directory if it doesn't exist.
func (p *PGProcess) InitDB(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if data dir already exists (has PG_VERSION).
	versionFile := filepath.Join(p.cfg.DataDir, "PG_VERSION")
	if _, err := os.Stat(versionFile); err == nil {
		return nil // already initialized
	}

	// Run initdb.
	initDBPath := p.initDBPath()
	args := []string{"-D", p.cfg.DataDir, "-A", "trust", "-U", "postgres", "--no-locale", "--encoding=UTF8"}
	cmd := exec.CommandContext(ctx, initDBPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("initdb: %w", err)
	}

	// Write postgresql.conf and pg_hba.conf.
	if err := p.writeConfigFiles(); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// Start launches the PostgreSQL postmaster process.
func (p *PGProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	// Ensure data dir is initialized (takes its own lock).
	if err := p.InitDB(ctx); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil
	}

	// Start postmaster.
	pgPath := p.pgPath()
	args := []string{
		"-D", p.cfg.DataDir,
		"-p", strconv.Itoa(p.cfg.Port),
		"-h", p.cfg.BindAddress,
		"-i", // enable TCP/IP
	}

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.cmd = exec.CommandContext(ctx, pgPath, args...)
	p.cmd.Stdout = os.Stdout
	p.cmd.Stderr = os.Stderr

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}

	p.proc = p.cmd.Process

	// Wait for postmaster to accept connections.
	deadline := time.Now().Add(p.cfg.StartTimeout)
	for time.Now().Before(deadline) {
		if p.ping() {
			p.started = true
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Timeout: kill process.
	_ = p.proc.Kill()
	return dberrors.New(dberrors.ErrTimeout, "postgres did not start within timeout", false, nil)
}

// Stop gracefully shuts down PostgreSQL.
func (p *PGProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	// Send SIGTERM for graceful shutdown.
	if err := p.proc.Signal(os.Interrupt); err != nil {
		return fmt.Errorf("signal postgres: %w", err)
	}

	// Wait for process exit with timeout.
	done := make(chan error, 1)
	go func() {
		_, err := p.cmd.Process.Wait()
		done <- err
	}()

	select {
	case <-done:
		p.started = false
		if p.cancel != nil {
			p.cancel()
		}
		return nil
	case <-time.After(p.cfg.StopTimeout):
		// Timeout: force kill.
		_ = p.proc.Kill()
		p.started = false
		return dberrors.New(dberrors.ErrTimeout, "postgres did not stop within timeout", false, nil)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Health checks if the postmaster is running and accepting connections.
func (p *PGProcess) Health(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started || p.proc == nil {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "postgres not started", true, nil)
	}

	// Check if process is still alive.
	if err := p.proc.Signal(os.Signal(nil)); err != nil {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "postgres process died", true, nil)
	}

	if !p.ping() {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "postgres not accepting connections", true, nil)
	}

	return nil
}

// GetProcess returns the underlying os.Process for test introspection.
func (p *PGProcess) GetProcess() *os.Process {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.proc
}

// pgPath returns the path to the postgres binary.
func (p *PGProcess) pgPath() string {
	if p.cfg.BinPath != "" {
		return p.cfg.BinPath
	}
	return "postgres"
}

// initDBPath returns the path to the initdb binary.
func (p *PGProcess) initDBPath() string {
	if p.cfg.InitDBPath != "" {
		return p.cfg.InitDBPath
	}
	return "initdb"
}

// ping checks if postgres is accepting TCP connections.
func (p *PGProcess) ping() bool {
	addr := fmt.Sprintf("%s:%d", p.cfg.BindAddress, p.cfg.Port)
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// writeConfigFiles writes postgresql.conf and pg_hba.conf.
func (p *PGProcess) writeConfigFiles() error {
	// postgresql.conf
	confPath := filepath.Join(p.cfg.DataDir, "postgresql.conf")
	conf := &strings.Builder{}
	conf.WriteString("# Auto-generated by GoMultiDB\n")
	conf.WriteString(fmt.Sprintf("listen_addresses = '%s'\n", p.cfg.BindAddress))
	conf.WriteString(fmt.Sprintf("port = %d\n", p.cfg.Port))
	conf.WriteString("max_connections = 200\n")
	conf.WriteString("shared_buffers = 128MB\n")
	conf.WriteString("logging_collector = off\n")
	conf.WriteString("log_destination = 'stderr'\n")
	conf.WriteString("client_min_messages = warning\n")

	// Extra config.
	for k, v := range p.cfg.ExtraConf {
		conf.WriteString(fmt.Sprintf("%s = '%s'\n", k, v))
	}

	if err := os.WriteFile(confPath, []byte(conf.String()), 0644); err != nil {
		return err
	}

	// pg_hba.conf
	hbaPath := filepath.Join(p.cfg.DataDir, "pg_hba.conf")
	hba := p.cfg.HBAConfigContent
	if hba == "" {
		hba = "host all all all trust\n"
	}
	if err := os.WriteFile(hbaPath, []byte(hba), 0644); err != nil {
		return err
	}

	return nil
}

// CatalogVersion tracks the current catalog version for cache invalidation.
type CatalogVersion struct {
	mu     sync.RWMutex
	version uint64
}

// NewCatalogVersion creates a new CatalogVersion tracker.
func NewCatalogVersion() *CatalogVersion {
	return &CatalogVersion{version: 1}
}

// Get returns the current version.
func (c *CatalogVersion) Get() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// Increment bumps the version and returns the new value.
func (c *CatalogVersion) Increment() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	return c.version
}

// Set updates the version to a specific value.
func (c *CatalogVersion) Set(v uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version = v
}
