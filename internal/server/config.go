package server

import (
	"fmt"
	"net"
	"strings"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type TLSConfig struct {
	NodeToNode      bool
	ClientToNode    bool
	CertsDir        string
	RequireClientCA bool
}

type Config struct {
	NodeID               string
	RPCBindAddress       string
	HTTPBindAddress      string
	DataDirs             []string
	WALDirs              []string
	MemoryHardLimitBytes int64
	MaxClockSkew         time.Duration
	TLS                  TLSConfig
	StrictContractCheck  bool
	EnableYSQL           bool
	YSQLBindAddress      string
	YSQLMaxConnections   int
	EnableYCQL           bool
	YCQLBindAddress      string
	YCQLMaxConnections   int
	EnableSnapshotCoord  bool
	MaxConcurrentSnaps   int
}

// ValidateConfig validates all Config fields and returns ErrInvalidConfig on the
// first invalid field encountered.
func ValidateConfig(cfg Config) error {
	invalid := func(field, reason string) error {
		return dberrors.New(dberrors.ErrInvalidConfig,
			fmt.Sprintf("field %s: %s", field, reason), false, nil)
	}

	if cfg.NodeID == "" {
		return invalid("NodeID", "must not be empty")
	}
	if err := validateBindAddress("RPCBindAddress", cfg.RPCBindAddress); err != nil {
		return err
	}
	if len(cfg.DataDirs) == 0 {
		return invalid("DataDirs", "must not be empty")
	}
	if hasOverlap(cfg.DataDirs, cfg.WALDirs) {
		return invalid("WALDirs", "must not overlap with DataDirs")
	}
	if cfg.MemoryHardLimitBytes <= 0 {
		return invalid("MemoryHardLimitBytes", "must be > 0")
	}
	if cfg.MaxClockSkew < time.Millisecond || cfg.MaxClockSkew > 10*time.Second {
		return invalid("MaxClockSkew", "must be in [1ms, 10s]")
	}
	return nil
}

func validateBindAddress(field, addr string) error {
	invalid := func(reason string) error {
		return dberrors.New(dberrors.ErrInvalidConfig,
			fmt.Sprintf("field %s: %s", field, reason), false, nil)
	}
	if addr == "" {
		return invalid("must not be empty")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return invalid(fmt.Sprintf("not a valid host:port: %v", err))
	}
	_ = host
	if port == "" {
		return invalid("port is empty")
	}
	return nil
}

func hasOverlap(a, b []string) bool {
	set := make(map[string]struct{}, len(a))
	for _, v := range a {
		set[strings.TrimRight(v, "/")] = struct{}{}
	}
	for _, v := range b {
		if _, ok := set[strings.TrimRight(v, "/")]; ok {
			return true
		}
	}
	return false
}

func DefaultConfig() Config {
	return Config{
		RPCBindAddress:      "0.0.0.0:9100",
		HTTPBindAddress:     "0.0.0.0:9000",
		MaxClockSkew:        500 * time.Millisecond,
		StrictContractCheck: true,
		EnableYSQL:          false,
		YSQLBindAddress:     "127.0.0.1:5433",
		YSQLMaxConnections:  300,
		EnableYCQL:          true,
		YCQLBindAddress:     "127.0.0.1:9042",
		YCQLMaxConnections:  1000,
		EnableSnapshotCoord: true,
		MaxConcurrentSnaps:  2,
	}
}
