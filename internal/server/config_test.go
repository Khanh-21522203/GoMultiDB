package server

import (
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

func validConfig() Config {
	return Config{
		NodeID:               "node-1",
		RPCBindAddress:       "0.0.0.0:9100",
		DataDirs:             []string{"/data1"},
		MemoryHardLimitBytes: 1 << 30,
		MaxClockSkew:         500 * time.Millisecond,
	}
}

func assertInvalidConfig(t *testing.T, cfg Config, expectField string) {
	t.Helper()
	err := ValidateConfig(cfg)
	if err == nil {
		t.Fatalf("expected ErrInvalidConfig for field %s, got nil", expectField)
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInvalidConfig {
		t.Fatalf("expected ErrInvalidConfig, got %s: %v", n.Code, err)
	}
}

func TestValidateConfigValid(t *testing.T) {
	if err := ValidateConfig(validConfig()); err != nil {
		t.Fatalf("valid config should pass validation: %v", err)
	}
}

func TestValidateConfigEmptyNodeID(t *testing.T) {
	cfg := validConfig()
	cfg.NodeID = ""
	assertInvalidConfig(t, cfg, "NodeID")
}

func TestValidateConfigEmptyRPCBindAddress(t *testing.T) {
	cfg := validConfig()
	cfg.RPCBindAddress = ""
	assertInvalidConfig(t, cfg, "RPCBindAddress")
}

func TestValidateConfigBadRPCBindAddress(t *testing.T) {
	cfg := validConfig()
	cfg.RPCBindAddress = "not-a-valid-address"
	assertInvalidConfig(t, cfg, "RPCBindAddress")
}

func TestValidateConfigEmptyDataDirs(t *testing.T) {
	cfg := validConfig()
	cfg.DataDirs = nil
	assertInvalidConfig(t, cfg, "DataDirs")
}

func TestValidateConfigOverlappingWALDirs(t *testing.T) {
	cfg := validConfig()
	cfg.WALDirs = []string{"/data1"} // overlaps DataDirs
	assertInvalidConfig(t, cfg, "WALDirs")
}

func TestValidateConfigZeroMemoryLimit(t *testing.T) {
	cfg := validConfig()
	cfg.MemoryHardLimitBytes = 0
	assertInvalidConfig(t, cfg, "MemoryHardLimitBytes")
}

func TestValidateConfigNegativeMemoryLimit(t *testing.T) {
	cfg := validConfig()
	cfg.MemoryHardLimitBytes = -1
	assertInvalidConfig(t, cfg, "MemoryHardLimitBytes")
}

func TestValidateConfigClockSkewTooLow(t *testing.T) {
	cfg := validConfig()
	cfg.MaxClockSkew = 500 * time.Microsecond // below 1ms
	assertInvalidConfig(t, cfg, "MaxClockSkew")
}

func TestValidateConfigClockSkewTooHigh(t *testing.T) {
	cfg := validConfig()
	cfg.MaxClockSkew = 11 * time.Second // above 10s
	assertInvalidConfig(t, cfg, "MaxClockSkew")
}

func TestValidateConfigClockSkewBoundaryLow(t *testing.T) {
	cfg := validConfig()
	cfg.MaxClockSkew = time.Millisecond
	if err := ValidateConfig(cfg); err != nil {
		t.Fatalf("1ms clock skew should be valid: %v", err)
	}
}

func TestValidateConfigClockSkewBoundaryHigh(t *testing.T) {
	cfg := validConfig()
	cfg.MaxClockSkew = 10 * time.Second
	if err := ValidateConfig(cfg); err != nil {
		t.Fatalf("10s clock skew should be valid: %v", err)
	}
}
