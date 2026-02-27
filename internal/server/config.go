package server

import "time"

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
}

func DefaultConfig() Config {
	return Config{
		RPCBindAddress:      "0.0.0.0:9100",
		HTTPBindAddress:     "0.0.0.0:9000",
		MaxClockSkew:        500 * time.Millisecond,
		StrictContractCheck: true,
	}
}
