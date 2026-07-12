package server

import (
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// TLSConfig controls TLS for node-to-node and client-to-node connections.
type TLSConfig struct {
	NodeToNode      bool
	ClientToNode    bool
	CertsDir        string
	RequireClientCA bool
}

// Config describes a single node's identity, network bindings, storage
// directories, and query-gateway options.
type Config struct {
	NodeID                        string
	RPCBindAddress                string
	HTTPBindAddress               string
	DataDirs                      []string
	WALDirs                       []string
	MemoryHardLimitBytes          int64
	MaxClockSkew                  time.Duration
	TLS                           TLSConfig
	StrictContractCheck           bool
	EnableSQL                     bool
	EnableSQLProcess              bool
	SQLBindAddress                string
	SQLMaxConnections             int
	SQLDataDir                    string
	SQLProcessBinPath             string
	SQLProcessInitDBPath          string
	SQLProcessStartTimeout        time.Duration
	SQLProcessStopTimeout         time.Duration
	SQLAllowFallbackToCoordinator bool
	EnableCQL                     bool
	CQLBindAddress                string
	CQLMaxConnections             int
	EnableSnapshotCoord           bool
	MaxConcurrentSnaps            int
}

// ValidateConfig validates all Config fields and returns ErrInvalidConfig
// on the first invalid field encountered: NodeID and RPCBindAddress must
// be set, RPCBindAddress must be a valid host:port, DataDirs must be
// non-empty and must not overlap WALDirs, MemoryHardLimitBytes must be
// positive, and MaxClockSkew must fall within [1ms, 10s].
//
// Not yet implemented in this scaffold.
func ValidateConfig(cfg Config) error {
	return errors.ErrNotImplemented
}

// DefaultConfig returns a Config populated with the runtime's default
// bind addresses, timeouts, and query-gateway settings.
func DefaultConfig() Config {
	return Config{
		RPCBindAddress:                "0.0.0.0:9100",
		HTTPBindAddress:               "0.0.0.0:9000",
		MaxClockSkew:                  500 * time.Millisecond,
		StrictContractCheck:           true,
		EnableSQL:                     false,
		EnableSQLProcess:              false,
		SQLBindAddress:                "127.0.0.1:5433",
		SQLMaxConnections:             300,
		SQLDataDir:                    "/tmp/gomultidb/sql",
		SQLAllowFallbackToCoordinator: true,
		EnableCQL:                     true,
		CQLBindAddress:                "127.0.0.1:9042",
		CQLMaxConnections:             1000,
		EnableSnapshotCoord:           true,
		MaxConcurrentSnaps:            2,
	}
}
