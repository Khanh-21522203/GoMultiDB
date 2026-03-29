package cql

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	orig := &Frame{
		Version: 0x04,
		Flags:   0x00,
		Stream:  42,
		Opcode:  OpcodeQuery,
		Body:    []byte{0x00, 0x00, 0x00},
	}
	data, err := orig.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed := &Frame{}
	if err := parsed.UnmarshalBinary(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Version != orig.Version {
		t.Fatalf("version: want %d, got %d", orig.Version, parsed.Version)
	}
	if parsed.Stream != orig.Stream {
		t.Fatalf("stream: want %d, got %d", orig.Stream, parsed.Stream)
	}
	if parsed.Opcode != OpcodeQuery {
		t.Fatalf("opcode: want %d, got %d", OpcodeQuery, parsed.Opcode)
	}
	if len(parsed.Body) != len(orig.Body) {
		t.Fatalf("body length: want %d, got %d", len(orig.Body), len(parsed.Body))
	}
}

func TestCodecString(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	s := "hello world"
	if err := codec.WriteString(buf, s); err != nil {
		t.Fatalf("write string: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadString(rd)
	if err != nil {
		t.Fatalf("read string: %v", err)
	}
	if out != s {
		t.Fatalf("string: want %q, got %q", s, out)
	}
}

func TestCodecLongString(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	s := "a longer string with more content"
	if err := codec.WriteLongString(buf, s); err != nil {
		t.Fatalf("write long string: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadLongString(rd)
	if err != nil {
		t.Fatalf("read long string: %v", err)
	}
	if out != s {
		t.Fatalf("long string: want %q, got %q", s, out)
	}
}

func TestCodecBytes(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	data := []byte{1, 2, 3, 4, 5}
	if err := codec.WriteBytes(buf, data); err != nil {
		t.Fatalf("write bytes: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadBytes(rd)
	if err != nil {
		t.Fatalf("read bytes: %v", err)
	}
	if len(out) != len(data) {
		t.Fatalf("bytes length: want %d, got %d", len(data), len(out))
	}
}

func TestCodecBytesNull(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	if err := codec.WriteBytes(buf, nil); err != nil {
		t.Fatalf("write null bytes: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadBytes(rd)
	if err != nil {
		t.Fatalf("read null bytes: %v", err)
	}
	if out != nil {
		t.Fatalf("null bytes: want nil, got %v", out)
	}
}

func TestCodecStringList(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	items := []string{"a", "b", "c"}
	if err := codec.WriteStringList(buf, items); err != nil {
		t.Fatalf("write string list: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadStringList(rd)
	if err != nil {
		t.Fatalf("read string list: %v", err)
	}
	if len(out) != len(items) {
		t.Fatalf("string list length: want %d, got %d", len(items), len(out))
	}
}

func TestCodecStringMap(t *testing.T) {
	codec := NewCodec()
	buf := &bytes.Buffer{}

	m := map[string]string{"k1": "v1", "k2": "v2"}
	if err := codec.WriteStringMap(buf, m); err != nil {
		t.Fatalf("write string map: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	out, err := codec.ReadStringMap(rd)
	if err != nil {
		t.Fatalf("read string map: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("string map length: want 2, got %d", len(out))
	}
	if out["k1"] != "v1" {
		t.Fatalf("k1: want v1, got %s", out["k1"])
	}
}

func TestQueryRequestMarshal(t *testing.T) {
	codec := NewCodec()
	req := &QueryRequest{
		Query:       "SELECT * FROM t",
		Consistency: ConsistencyOne,
		Flags:       0x00,
	}

	buf := &bytes.Buffer{}
	if err := req.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Unmarshal.
	parsed := &QueryRequest{}
	if err := parsed.Unmarshal(codec, buf.Bytes()); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Query != req.Query {
		t.Fatalf("query: want %q, got %q", req.Query, parsed.Query)
	}
	if parsed.Consistency != ConsistencyOne {
		t.Fatalf("consistency: want ONE, got %d", parsed.Consistency)
	}
}

func TestPrepareRequestMarshal(t *testing.T) {
	codec := NewCodec()
	req := &PrepareRequest{
		Query: "INSERT INTO t (k, v) VALUES (?, ?)",
	}

	buf := &bytes.Buffer{}
	if err := req.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed := &PrepareRequest{}
	if err := parsed.Unmarshal(codec, buf.Bytes()); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Query != req.Query {
		t.Fatalf("query: want %q, got %q", req.Query, parsed.Query)
	}
}

func TestExecuteRequestMarshal(t *testing.T) {
	codec := NewCodec()
	req := &ExecuteRequest{
		QueryID:     []byte{1, 2, 3, 4},
		Consistency: ConsistencyQuorum,
		Flags:       0x00,
		QueryParams: &QueryParams{
			Consistency: ConsistencyQuorum,
			Values:      [][]byte{{1}, {2}},
		},
	}

	buf := &bytes.Buffer{}
	if err := req.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed := &ExecuteRequest{}
	if err := parsed.Unmarshal(codec, buf.Bytes()); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed.QueryID) != 4 {
		t.Fatalf("query id length: want 4, got %d", len(parsed.QueryID))
	}
	if parsed.Consistency != ConsistencyQuorum {
		t.Fatalf("consistency: want QUORUM, got %d", parsed.Consistency)
	}
}

func TestRegisterRequestMarshal(t *testing.T) {
	codec := NewCodec()
	req := &RegisterRequest{
		Events: []string{"TOPOLOGY_CHANGE", "STATUS_CHANGE"},
	}

	buf := &bytes.Buffer{}
	if err := req.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed := &RegisterRequest{}
	if err := parsed.Unmarshal(codec, buf.Bytes()); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed.Events) != 2 {
		t.Fatalf("events: want 2, got %d", len(parsed.Events))
	}
}

func TestBatchRequestMarshal(t *testing.T) {
	codec := NewCodec()
	req := &BatchRequest{
		Type:        BatchTypeLogged,
		Consistency: ConsistencyQuorum,
		Queries: []BatchQuery{
			{
				Kind:        0,
				QueryString: "INSERT INTO t (k, v) VALUES (?, ?)",
				Values:      [][]byte{[]byte("1"), []byte("a")},
			},
			{
				Kind:    1,
				QueryID: []byte("prepared-1"),
				Values:  [][]byte{[]byte("2")},
			},
		},
		SerialConsistency: ConsistencyLocalQuorum,
		Timestamp:         123456789,
	}

	buf := &bytes.Buffer{}
	if err := req.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed := &BatchRequest{}
	if err := parsed.Unmarshal(codec, buf.Bytes()); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Type != BatchTypeLogged {
		t.Fatalf("type: want LOGGED, got %d", parsed.Type)
	}
	if len(parsed.Queries) != 2 {
		t.Fatalf("queries length: want 2, got %d", len(parsed.Queries))
	}
	if parsed.Queries[0].QueryString == "" {
		t.Fatalf("expected first query string to be present")
	}
	if string(parsed.Queries[1].QueryID) != "prepared-1" {
		t.Fatalf("expected prepared query id to round-trip")
	}
	if parsed.Consistency != ConsistencyQuorum {
		t.Fatalf("consistency: want QUORUM, got %d", parsed.Consistency)
	}
	if parsed.SerialConsistency != ConsistencyLocalQuorum {
		t.Fatalf("serial consistency mismatch: got %d", parsed.SerialConsistency)
	}
	if parsed.Timestamp != 123456789 {
		t.Fatalf("timestamp mismatch: got %d", parsed.Timestamp)
	}
}
