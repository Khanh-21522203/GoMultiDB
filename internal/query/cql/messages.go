package cql

import (
	"bytes"
	"fmt"
	"io"
)

// Request types (client -> server).

// StartupRequest initiates a connection.
type StartupRequest struct {
	Options map[string]string
}

// QueryRequest executes a CQL query.
type QueryRequest struct {
	Query        string
	Consistency  ConsistencyLevel
	Flags        byte
	QueryParams  *QueryParams
	ResultPageID []byte // for paging
}

// QueryParams contains bound parameters for a query.
type QueryParams struct {
	Consistency       ConsistencyLevel
	SkipMetadata      bool
	Values            [][]byte
	PageSize          int
	PagingState       []byte
	SerialConsistency ConsistencyLevel
	Timestamp         int64
	Keyspace          string
}

// PrepareRequest prepares a statement for execution.
type PrepareRequest struct {
	Query string
}

// ExecuteRequest executes a prepared statement.
type ExecuteRequest struct {
	QueryID      []byte
	Consistency  ConsistencyLevel
	Flags        byte
	QueryParams  *QueryParams
	ResultPageID []byte
}

// BatchRequest executes a batch of statements.
type BatchRequest struct {
	Type              BatchType
	Queries           []BatchQuery
	Consistency       ConsistencyLevel
	SerialConsistency ConsistencyLevel
	Timestamp         int64
}

// BatchQuery is a single query in a batch.
type BatchQuery struct {
	Kind        byte // 0=query string, 1=prepared statement ID
	QueryString string
	QueryID     []byte
	Values      [][]byte
}

// RegisterRequest registers for events.
type RegisterRequest struct {
	Events []string // e.g., "TOPOLOGY_CHANGE", "STATUS_CHANGE"
}

// Response types (server -> client).

// ResultResponse contains query results.
type ResultResponse struct {
	Kind         ResultKind
	Rows         *RowsResult
	SchemaChange *SchemaChangeResult
}

// ResultKind identifies the result type.
type ResultKind byte

const (
	ResultKindVoid         ResultKind = 0x01
	ResultKindRows         ResultKind = 0x02
	ResultKindSetKeyspace  ResultKind = 0x03
	ResultKindPrepared     ResultKind = 0x04
	ResultKindSchemaChange ResultKind = 0x05
)

// RowsResult contains row data.
type RowsResult struct {
	Metadata    *RowsMetadata
	Rows        []*Row
	PagingState []byte
}

// RowsMetadata describes result columns.
type RowsMetadata struct {
	Flags    int
	Columns  []*Column
	RowCount int
}

// Column describes a single column.
type Column struct {
	Keyspace string
	Table    string
	Name     string
	Type     DataType
}

// Row is a single result row.
type Row []*Value

// Value is a cell value (typed bytes).
type Value struct {
	Type  DataType
	Value []byte
}

// DataType identifies CQL data types.
type DataType byte

const (
	TypeCustom    DataType = 0x00
	TypeAscii     DataType = 0x01
	TypeBigInt    DataType = 0x02
	TypeBlob      DataType = 0x03
	TypeBoolean   DataType = 0x04
	TypeCounter   DataType = 0x05
	TypeDecimal   DataType = 0x06
	TypeDouble    DataType = 0x07
	TypeFloat     DataType = 0x08
	TypeInt       DataType = 0x09
	TypeTimestamp DataType = 0x0B
	TypeUUID      DataType = 0x0C
	TypeVarchar   DataType = 0x0D
	TypeVarint    DataType = 0x0E
	TypeTimeUUID  DataType = 0x0F
	TypeInet      DataType = 0x10
	TypeDate      DataType = 0x11
	TypeTime      DataType = 0x12
	TypeSmallInt  DataType = 0x13
	TypeTinyInt   DataType = 0x14
	TypeDuration  DataType = 0x15
	TypeList      DataType = 0x20
	TypeMap       DataType = 0x21
	TypeSet       DataType = 0x22
	TypeUDT       DataType = 0x30
	TypeTuple     DataType = 0x31
)

// SchemaChangeResult describes a DDL operation.
type SchemaChangeResult struct {
	Change   string // "CREATED", "UPDATED", "DROPPED"
	Target   string // "KEYSPACE", "TABLE", "TYPE", "FUNCTION", "AGGREGATE"
	Keyspace string
	Table    string
}

// PreparedResult contains prepared statement metadata.
type PreparedResult struct {
	QueryID        []byte
	Metadata       *RowsMetadata
	ResultMetadata *RowsMetadata
}

// ErrorResponse contains error information.
type ErrorResponse struct {
	Code    int
	Message string
}

// ConsistencyLevel identifies CQL consistency levels.
type ConsistencyLevel byte

const (
	ConsistencyAny         ConsistencyLevel = 0x00
	ConsistencyOne         ConsistencyLevel = 0x01
	ConsistencyTwo         ConsistencyLevel = 0x02
	ConsistencyThree       ConsistencyLevel = 0x03
	ConsistencyQuorum      ConsistencyLevel = 0x04
	ConsistencyAll         ConsistencyLevel = 0x05
	ConsistencyLocalQuorum ConsistencyLevel = 0x06
	ConsistencyEachQuorum  ConsistencyLevel = 0x07
	ConsistencyLocalOne    ConsistencyLevel = 0x0A
)

// BatchType identifies batch operation types.
type BatchType byte

const (
	BatchTypeLogged   BatchType = 0
	BatchTypeUnlogged BatchType = 1
	BatchTypeCounter  BatchType = 2
)

// Marshal StartupRequest.
func (r *StartupRequest) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteStringMap(w, r.Options); err != nil {
		return err
	}
	return nil
}

// Unmarshal StartupRequest.
func (r *StartupRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)
	m, err := codec.ReadStringMap(rd)
	if err != nil {
		return err
	}
	r.Options = m
	return nil
}

// Marshal QueryRequest.
func (r *QueryRequest) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteLongString(w, r.Query); err != nil {
		return err
	}
	if err := codec.WriteByte(w, byte(r.Consistency)); err != nil {
		return err
	}
	if err := codec.WriteByte(w, r.Flags); err != nil {
		return err
	}
	if r.QueryParams != nil {
		if err := r.QueryParams.Marshal(codec, w); err != nil {
			return err
		}
	}
	if (r.Flags&0x08) != 0 && r.ResultPageID != nil {
		if err := codec.WriteBytes(w, r.ResultPageID); err != nil {
			return err
		}
	}
	return nil
}

// Unmarshal QueryRequest.
func (r *QueryRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)
	query, err := codec.ReadLongString(rd)
	if err != nil {
		return err
	}
	r.Query = query

	cons, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	r.Consistency = ConsistencyLevel(cons)

	flags, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	r.Flags = flags

	if (flags & 0x01) != 0 {
		// Query params present.
		qp := &QueryParams{}
		if err := qp.Unmarshal(codec, rd); err != nil {
			return err
		}
		r.QueryParams = qp
	}

	if (flags & 0x08) != 0 {
		pid, err := codec.ReadBytes(rd)
		if err != nil {
			return err
		}
		r.ResultPageID = pid
	}

	return nil
}

// Marshal QueryParams.
func (p *QueryParams) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteByte(w, byte(p.Consistency)); err != nil {
		return err
	}
	var flags byte
	if p.SkipMetadata {
		flags |= 0x01
	}
	if len(p.Values) > 0 {
		flags |= 0x02
	}
	if p.PageSize > 0 {
		flags |= 0x04
	}
	if p.PagingState != nil {
		flags |= 0x08
	}
	if p.SerialConsistency != 0 {
		flags |= 0x10
	}
	if p.Timestamp != 0 {
		flags |= 0x20
	}
	if p.Keyspace != "" {
		flags |= 0x40
	}
	if err := codec.WriteByte(w, flags); err != nil {
		return err
	}

	if (flags & 0x02) != 0 {
		if err := codec.WriteShort(w, int16(len(p.Values))); err != nil {
			return err
		}
		for _, v := range p.Values {
			if err := codec.WriteBytes(w, v); err != nil {
				return err
			}
		}
	}

	if (flags & 0x04) != 0 {
		if err := codec.WriteInt(w, int32(p.PageSize)); err != nil {
			return err
		}
	}
	if (flags & 0x08) != 0 {
		if err := codec.WriteBytes(w, p.PagingState); err != nil {
			return err
		}
	}
	if (flags & 0x10) != 0 {
		if err := codec.WriteByte(w, byte(p.SerialConsistency)); err != nil {
			return err
		}
	}
	if (flags & 0x20) != 0 {
		if err := codec.WriteLong(w, p.Timestamp); err != nil {
			return err
		}
	}
	if (flags & 0x40) != 0 {
		if err := codec.WriteString(w, p.Keyspace); err != nil {
			return err
		}
	}

	return nil
}

// Unmarshal QueryParams.
func (p *QueryParams) Unmarshal(codec *Codec, rd *bytes.Reader) error {
	cons, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	p.Consistency = ConsistencyLevel(cons)

	flags, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}

	p.SkipMetadata = (flags & 0x01) != 0

	if (flags & 0x02) != 0 {
		// Values list.
		count, err := codec.ReadShort(rd)
		if err != nil {
			return err
		}
		p.Values = make([][]byte, count)
		for i := range p.Values {
			v, err := codec.ReadBytes(rd)
			if err != nil {
				return err
			}
			p.Values[i] = v
		}
	}

	if (flags & 0x04) != 0 {
		ps, err := codec.ReadInt(rd)
		if err != nil {
			return err
		}
		p.PageSize = int(ps)
	}

	if (flags & 0x08) != 0 {
		pg, err := codec.ReadBytes(rd)
		if err != nil {
			return err
		}
		p.PagingState = pg
	}

	if (flags & 0x10) != 0 {
		sc, err := codec.ReadByte(rd)
		if err != nil {
			return err
		}
		p.SerialConsistency = ConsistencyLevel(sc)
	}

	if (flags & 0x20) != 0 {
		ts, err := codec.ReadLong(rd)
		if err != nil {
			return err
		}
		p.Timestamp = ts
	}

	if (flags & 0x40) != 0 {
		ks, err := codec.ReadString(rd)
		if err != nil {
			return err
		}
		p.Keyspace = ks
	}

	return nil
}

// Marshal PrepareRequest.
func (r *PrepareRequest) Marshal(codec *Codec, w io.Writer) error {
	return codec.WriteLongString(w, r.Query)
}

// Unmarshal PrepareRequest.
func (r *PrepareRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)
	q, err := codec.ReadLongString(rd)
	if err != nil {
		return err
	}
	r.Query = q
	return nil
}

// Marshal ExecuteRequest.
func (r *ExecuteRequest) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteBytes(w, r.QueryID); err != nil {
		return err
	}
	if err := codec.WriteByte(w, byte(r.Consistency)); err != nil {
		return err
	}
	if err := codec.WriteByte(w, r.Flags); err != nil {
		return err
	}
	if r.QueryParams != nil {
		if err := r.QueryParams.Marshal(codec, w); err != nil {
			return err
		}
	}
	if (r.Flags&0x08) != 0 && r.ResultPageID != nil {
		return codec.WriteBytes(w, r.ResultPageID)
	}
	return nil
}

// Unmarshal ExecuteRequest.
func (r *ExecuteRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)
	qid, err := codec.ReadBytes(rd)
	if err != nil {
		return err
	}
	r.QueryID = qid

	cons, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	r.Consistency = ConsistencyLevel(cons)

	flags, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	r.Flags = flags

	if (flags & 0x01) != 0 {
		qp := &QueryParams{}
		if err := qp.Unmarshal(codec, rd); err != nil {
			return err
		}
		r.QueryParams = qp
	}

	if (flags & 0x08) != 0 {
		pid, err := codec.ReadBytes(rd)
		if err != nil {
			return err
		}
		r.ResultPageID = pid
	}

	return nil
}

// Marshal BatchRequest.
func (r *BatchRequest) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteByte(w, byte(r.Type)); err != nil {
		return err
	}
	if err := codec.WriteShort(w, int16(len(r.Queries))); err != nil {
		return err
	}
	for _, q := range r.Queries {
		if err := codec.WriteByte(w, q.Kind); err != nil {
			return err
		}
		switch q.Kind {
		case 0:
			if err := codec.WriteLongString(w, q.QueryString); err != nil {
				return err
			}
		case 1:
			if err := writeShortBytes(codec, w, q.QueryID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported batch query kind: %d", q.Kind)
		}
		if err := codec.WriteShort(w, int16(len(q.Values))); err != nil {
			return err
		}
		for _, v := range q.Values {
			if err := codec.WriteBytes(w, v); err != nil {
				return err
			}
		}
	}
	if err := codec.WriteShort(w, int16(r.Consistency)); err != nil {
		return err
	}

	var flags byte
	if r.SerialConsistency != 0 {
		flags |= 0x10
	}
	if r.Timestamp != 0 {
		flags |= 0x20
	}
	if err := codec.WriteByte(w, flags); err != nil {
		return err
	}
	if (flags & 0x10) != 0 {
		if err := codec.WriteShort(w, int16(r.SerialConsistency)); err != nil {
			return err
		}
	}
	if (flags & 0x20) != 0 {
		if err := codec.WriteLong(w, r.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

// Unmarshal BatchRequest.
func (r *BatchRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)

	batchType, err := codec.ReadByte(rd)
	if err != nil {
		return err
	}
	r.Type = BatchType(batchType)

	count, err := codec.ReadShort(rd)
	if err != nil {
		return err
	}
	if count < 0 {
		return fmt.Errorf("invalid batch query count: %d", count)
	}
	r.Queries = make([]BatchQuery, int(count))
	for i := 0; i < int(count); i++ {
		kind, readErr := codec.ReadByte(rd)
		if readErr != nil {
			return readErr
		}
		r.Queries[i].Kind = kind
		switch kind {
		case 0:
			query, qErr := codec.ReadLongString(rd)
			if qErr != nil {
				return qErr
			}
			r.Queries[i].QueryString = query
		case 1:
			id, idErr := readShortBytes(codec, rd)
			if idErr != nil {
				return idErr
			}
			r.Queries[i].QueryID = id
		default:
			return fmt.Errorf("unsupported batch query kind: %d", kind)
		}

		valueCount, vErr := codec.ReadShort(rd)
		if vErr != nil {
			return vErr
		}
		if valueCount < 0 {
			return fmt.Errorf("invalid batch value count: %d", valueCount)
		}
		if valueCount > 0 {
			r.Queries[i].Values = make([][]byte, int(valueCount))
			for j := 0; j < int(valueCount); j++ {
				val, valErr := codec.ReadBytes(rd)
				if valErr != nil {
					return valErr
				}
				r.Queries[i].Values[j] = val
			}
		}
	}

	consistency, err := codec.ReadShort(rd)
	if err != nil {
		return err
	}
	r.Consistency = ConsistencyLevel(consistency)

	if rd.Len() > 0 {
		flags, flagErr := codec.ReadByte(rd)
		if flagErr != nil {
			return flagErr
		}
		if (flags & 0x10) != 0 {
			sc, scErr := codec.ReadShort(rd)
			if scErr != nil {
				return scErr
			}
			r.SerialConsistency = ConsistencyLevel(sc)
		}
		if (flags & 0x20) != 0 {
			ts, tsErr := codec.ReadLong(rd)
			if tsErr != nil {
				return tsErr
			}
			r.Timestamp = ts
		}
	}

	return nil
}

// Marshal RegisterRequest.
func (r *RegisterRequest) Marshal(codec *Codec, w io.Writer) error {
	return codec.WriteStringList(w, r.Events)
}

// Unmarshal RegisterRequest.
func (r *RegisterRequest) Unmarshal(codec *Codec, body []byte) error {
	rd := bytes.NewReader(body)
	events, err := codec.ReadStringList(rd)
	if err != nil {
		return err
	}
	r.Events = events
	return nil
}

// Marshal ErrorResponse.
func (r *ErrorResponse) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteInt(w, int32(r.Code)); err != nil {
		return err
	}
	return codec.WriteString(w, r.Message)
}

// Marshal ResultResponse.
func (r *ResultResponse) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteInt(w, int32(r.Kind)); err != nil {
		return err
	}
	switch r.Kind {
	case ResultKindVoid:
		// No additional data.
	case ResultKindRows:
		return r.Rows.Marshal(codec, w)
	case ResultKindPrepared:
		return codec.WriteBytes(w, []byte{}) // stub: empty query ID
	case ResultKindSchemaChange:
		return r.SchemaChange.Marshal(codec, w)
	default:
		return fmt.Errorf("unimplemented result kind: %d", r.Kind)
	}
	return nil
}

// Marshal RowsResult.
func (r *RowsResult) Marshal(codec *Codec, w io.Writer) error {
	// Flags.
	var flags int32
	if r.Metadata != nil && len(r.Metadata.Columns) > 0 {
		flags |= 0x01 // global tablespace
	}
	if err := codec.WriteInt(w, flags); err != nil {
		return err
	}

	// Column count.
	colCount := 0
	if r.Metadata != nil {
		colCount = len(r.Metadata.Columns)
	}
	if err := codec.WriteInt(w, int32(colCount)); err != nil {
		return err
	}

	// Column metadata.
	if (flags&0x01) != 0 && r.Metadata != nil {
		for _, col := range r.Metadata.Columns {
			if err := codec.WriteString(w, col.Keyspace); err != nil {
				return err
			}
			if err := codec.WriteString(w, col.Table); err != nil {
				return err
			}
			if err := codec.WriteString(w, col.Name); err != nil {
				return err
			}
			if err := codec.WriteShort(w, int16(col.Type)); err != nil {
				return err
			}
		}
	}

	// Row count.
	if err := codec.WriteInt(w, int32(len(r.Rows))); err != nil {
		return err
	}

	// Rows.
	for _, row := range r.Rows {
		if row == nil {
			continue
		}
		for _, val := range *row {
			if val == nil {
				// Null value.
				if err := codec.WriteInt(w, int32(-1)); err != nil {
					return err
				}
				continue
			}
			if err := codec.WriteBytes(w, val.Value); err != nil {
				return err
			}
		}
	}

	// Paging state.
	if r.PagingState != nil {
		if err := codec.WriteBytes(w, r.PagingState); err != nil {
			return err
		}
	}

	return nil
}

// Marshal SchemaChangeResult.
func (r *SchemaChangeResult) Marshal(codec *Codec, w io.Writer) error {
	if err := codec.WriteString(w, r.Change); err != nil {
		return err
	}
	if err := codec.WriteString(w, r.Target); err != nil {
		return err
	}
	if r.Target == "KEYSPACE" {
		return codec.WriteString(w, r.Keyspace)
	}
	if r.Keyspace != "" {
		if err := codec.WriteString(w, r.Keyspace); err != nil {
			return err
		}
	}
	if r.Table != "" {
		return codec.WriteString(w, r.Table)
	}
	return nil
}

func writeShortBytes(codec *Codec, w io.Writer, data []byte) error {
	if data == nil {
		return codec.WriteShort(w, -1)
	}
	if err := codec.WriteShort(w, int16(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readShortBytes(codec *Codec, rd *bytes.Reader) ([]byte, error) {
	length, err := codec.ReadShort(rd)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	data := make([]byte, int(length))
	if _, err := io.ReadFull(rd, data); err != nil {
		return nil, err
	}
	return data, nil
}
