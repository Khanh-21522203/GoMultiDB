package cql

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	dberrors "GoMultiDB/internal/common/errors"
)

// Listener handles incoming CQL client connections.
type Listener struct {
	mu       sync.RWMutex
	cfg      Config
	server   *LocalServer
	listener net.Listener
	started  atomic.Bool
	stopped  atomic.Bool
	acceptWg sync.WaitGroup
}

// NewListener creates a CQL protocol listener.
func NewListener(server *LocalServer) *Listener {
	return &Listener{server: server}
}

// Start begins accepting CQL connections on the configured address.
func (l *Listener) Start(ctx context.Context, cfg Config) error {
	if !cfg.Enabled {
		return nil
	}

	if cfg.BindAddress == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "bind address is required", false, nil)
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1000
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started.Load() {
		return nil
	}

	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		return fmt.Errorf("listen cql: %w", err)
	}

	l.listener = ln
	l.cfg = cfg
	l.started.Store(true)

	l.acceptWg.Add(1)
	go l.acceptLoop(ctx)

	return nil
}

// Stop closes the listener and waits for all connections to drain.
func (l *Listener) Stop(ctx context.Context) error {
	if !l.started.Swap(false) {
		return nil
	}
	l.stopped.Store(true)

	l.mu.Lock()
	if l.listener != nil {
		_ = l.listener.Close()
	}
	l.mu.Unlock()

	// Wait for accept loop to exit.
	done := make(chan struct{})
	go func() {
		l.acceptWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acceptLoop accepts incoming connections and spawns handlers.
func (l *Listener) acceptLoop(ctx context.Context) {
	defer l.acceptWg.Done()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if l.stopped.Load() {
				return
			}
			continue
		}

		l.acceptWg.Add(1)
		go l.handleConn(ctx, conn)
	}
}

// handleConn processes CQL frames from a single connection.
func (l *Listener) handleConn(ctx context.Context, conn net.Conn) {
	defer l.acceptWg.Done()
	defer conn.Close()

	cqlConn := NewConnection(conn)
	connID := conn.RemoteAddr().String()

	// Open session.
	if err := l.server.OpenConnection(ctx, connID); err != nil {
		return
	}
	defer l.server.CloseConnection(ctx, connID)

	codec := NewCodec()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frame, err := cqlConn.ReadFrame()
		if err != nil {
			return
		}

		respFrame := l.handleFrame(ctx, codec, connID, frame)
		if respFrame != nil {
			if err := cqlConn.WriteFrame(respFrame); err != nil {
				return
			}
		}
	}
}

// handleFrame processes a single CQL request frame.
func (l *Listener) handleFrame(ctx context.Context, codec *Codec, connID string, frame *Frame) *Frame {
	// Response frame uses same stream ID, version with response bit set.
	resp := &Frame{
		Version: frame.Version | 0x80, // set response bit
		Flags:   0x00,
		Stream:  frame.Stream,
		Opcode:  OpcodeResult,
	}

	var body []byte
	var errCode int
	var errMsg string

	switch frame.Opcode {
	case OpcodeStartup:
		// Parse startup request.
		startup := &StartupRequest{}
		if err := startup.Unmarshal(codec, frame.Body); err != nil {
			errCode = 0x1000 // protocol error
			errMsg = err.Error()
			break
		}
		// Send READY.
		resp.Opcode = OpcodeReady
		body = []byte{}
		resp.Body = body
		resp.Length = len(body)
		return resp

	case OpcodeOptions:
		// Send SUPPORTED.
		resp.Opcode = OpcodeSupported
		buf := make([]byte, 0, 64)
		// Write multi-map (simplified: just options).
		// Count.
		buf = append(buf, 0, 1) // 1 entry
		// Key "CQL_VERSION"
		key := "CQL_VERSION"
		buf = append(buf, byte(len(key)>>8), byte(len(key)))
		buf = append(buf, []byte(key)...)
		// Value list ["4.0.0"]
		val := "4.0.0"
		buf = append(buf, 0, 1) // 1 value
		buf = append(buf, byte(len(val)>>8), byte(len(val)))
		buf = append(buf, []byte(val)...)
		resp.Body = buf
		resp.Length = len(buf)
		return resp

	case OpcodeQuery:
		// Parse and execute query.
		queryReq := &QueryRequest{}
		if err := queryReq.Unmarshal(codec, frame.Body); err != nil {
			errCode = 0x1000
			errMsg = err.Error()
			break
		}

		// Route to server.
		cqlReq := Request{
			ConnID: connID,
			Query:  queryReq.Query,
		}
		cqlResp, err := l.server.Route(ctx, cqlReq)
		if err != nil {
			// Map error to CQL error code.
			n := dberrors.NormalizeError(err)
			switch n.Code {
			case dberrors.ErrInvalidArgument:
				errCode = 0x0000 // invalid
			default:
				errCode = 0x1000 // server error
			}
			errMsg = n.Message
			break
		}

		// Build VOID or ROWS result.
		result := &ResultResponse{}
		if cqlResp.Rows > 0 {
			result.Kind = ResultKindRows
			result.Rows = &RowsResult{
				Metadata: &RowsMetadata{Columns: []*Column{}},
				Rows:     make([]*Row, cqlResp.Rows),
			}
		} else {
			result.Kind = ResultKindVoid
		}

		// Marshal result.
		buf := &bytes.Buffer{}
		if err := result.Marshal(codec, buf); err != nil {
			errCode = 0x1000
			errMsg = err.Error()
			break
		}
		resp.Body = buf.Bytes()
		resp.Length = len(resp.Body)
		return resp

	case OpcodePrepare:
		// Prepare statement.
		prep := &PrepareRequest{}
		if err := prep.Unmarshal(codec, frame.Body); err != nil {
			errCode = 0x1000
			errMsg = err.Error()
			break
		}

		stmt, err := l.server.Prepare(ctx, connID, prep.Query)
		if err != nil {
			n := dberrors.NormalizeError(err)
			errCode = 0x1000
			errMsg = n.Message
			break
		}

		// Return PREPARED result with prepared ID.
		result := &ResultResponse{Kind: ResultKindPrepared}
		buf := &bytes.Buffer{}
		_ = codec.WriteInt(buf, int32(result.Kind))
		_ = codec.WriteBytes(buf, []byte(stmt.ID)) // prepared ID
		// Result metadata (empty for now).
		_ = codec.WriteInt(buf, 0) // flags
		_ = codec.WriteInt(buf, 0) // column count
		_ = codec.WriteInt(buf, 0) // pk count
		resp.Body = buf.Bytes()
		resp.Length = len(resp.Body)
		return resp

	case OpcodeExecute:
		// Execute prepared statement.
		exec := &ExecuteRequest{}
		if err := exec.Unmarshal(codec, frame.Body); err != nil {
			errCode = 0x1000
			errMsg = err.Error()
			break
		}

		// Convert bytes to prepared ID.
		prepID := string(exec.QueryID)

		// Convert values.
		vars := make([]any, len(exec.QueryParams.Values))
		for i, v := range exec.QueryParams.Values {
			vars[i] = v
		}

		cqlReq := Request{
			ConnID:     connID,
			PreparedID: prepID,
			Vars:       vars,
		}
		_, err := l.server.Route(ctx, cqlReq)
		if err != nil {
			n := dberrors.NormalizeError(err)
			errCode = 0x1000
			errMsg = n.Message
			break
		}

		// Return VOID result.
		result := &ResultResponse{Kind: ResultKindVoid}
		buf := &bytes.Buffer{}
		_ = result.Marshal(codec, buf)
		resp.Body = buf.Bytes()
		resp.Length = len(resp.Body)
		return resp

	case OpcodeBatch:
		// Parse batch.
		// For simplicity, just acknowledge.
		result := &ResultResponse{Kind: ResultKindVoid}
		buf := &bytes.Buffer{}
		_ = result.Marshal(codec, buf)
		resp.Body = buf.Bytes()
		resp.Length = len(resp.Body)
		return resp

	case OpcodeRegister:
		// Register for events (stub: no-op).
		resp.Opcode = OpcodeReady
		resp.Body = []byte{}
		resp.Length = 0
		return resp

	default:
		errCode = 0x1000
		errMsg = fmt.Sprintf("unsupported opcode: %d", frame.Opcode)
	}

	// Build ERROR response.
	resp.Opcode = OpcodeError
	buf := &bytes.Buffer{}
	_ = codec.WriteInt(buf, int32(errCode))
	_ = codec.WriteString(buf, errMsg)
	resp.Body = buf.Bytes()
	resp.Length = len(resp.Body)
	return resp
}

// GetStatus returns the current listener status.
func (l *Listener) GetStatus() (Status, error) {
	ctx := context.Background()
	return l.server.Status(ctx)
}
