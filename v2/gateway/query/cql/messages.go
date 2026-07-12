package cql

import (
	"bytes"
	"io"

	errors "GoMultiDB/v2/contracts/errors"
)

// StartupRequest is the CQL STARTUP request that initiates a connection.
type StartupRequest struct {
	Options map[string]string
}

// Marshal encodes the StartupRequest body.
//
// Not yet implemented in this scaffold.
func (r *StartupRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes a StartupRequest body.
//
// Not yet implemented in this scaffold.
func (r *StartupRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// QueryRequest is a CQL QUERY request executing a single statement.
type QueryRequest struct {
	Query        string
	Consistency  ConsistencyLevel
	Flags        byte
	QueryParams  *QueryParams
	ResultPageID []byte
}

// Marshal encodes the QueryRequest body.
//
// Not yet implemented in this scaffold.
func (r *QueryRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes a QueryRequest body.
//
// Not yet implemented in this scaffold.
func (r *QueryRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// QueryParams carries the bound parameters and paging/consistency
// modifiers attached to a QUERY or EXECUTE request.
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

// Marshal encodes the QueryParams body.
//
// Not yet implemented in this scaffold.
func (p *QueryParams) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes the QueryParams body from rd.
//
// Not yet implemented in this scaffold.
func (p *QueryParams) Unmarshal(codec *Codec, rd *bytes.Reader) error {
	return errors.ErrNotImplemented
}

// PrepareRequest is a CQL PREPARE request that prepares a statement for
// later execution.
type PrepareRequest struct {
	Query string
}

// Marshal encodes the PrepareRequest body.
//
// Not yet implemented in this scaffold.
func (r *PrepareRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes a PrepareRequest body.
//
// Not yet implemented in this scaffold.
func (r *PrepareRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// ExecuteRequest is a CQL EXECUTE request that runs a previously prepared
// statement.
type ExecuteRequest struct {
	QueryID      []byte
	Consistency  ConsistencyLevel
	Flags        byte
	QueryParams  *QueryParams
	ResultPageID []byte
}

// Marshal encodes the ExecuteRequest body.
//
// Not yet implemented in this scaffold.
func (r *ExecuteRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes an ExecuteRequest body.
//
// Not yet implemented in this scaffold.
func (r *ExecuteRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// BatchRequest is a CQL BATCH request executing a set of statements
// together.
type BatchRequest struct {
	Type              BatchType
	Queries           []BatchQuery
	Consistency       ConsistencyLevel
	SerialConsistency ConsistencyLevel
	Timestamp         int64
}

// Marshal encodes the BatchRequest body.
//
// Not yet implemented in this scaffold.
func (r *BatchRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes a BatchRequest body.
//
// Not yet implemented in this scaffold.
func (r *BatchRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// BatchQuery is a single statement within a BatchRequest: either a plain
// query string or a reference to a prepared statement ID, plus its bound
// values.
type BatchQuery struct {
	Kind        byte
	QueryString string
	QueryID     []byte
	Values      [][]byte
}

// RegisterRequest is a CQL REGISTER request subscribing the connection to
// server-pushed events.
type RegisterRequest struct {
	Events []string
}

// Marshal encodes the RegisterRequest body.
//
// Not yet implemented in this scaffold.
func (r *RegisterRequest) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// Unmarshal decodes a RegisterRequest body.
//
// Not yet implemented in this scaffold.
func (r *RegisterRequest) Unmarshal(codec *Codec, body []byte) error {
	return errors.ErrNotImplemented
}

// ResultResponse is the CQL RESULT response returned for QUERY, EXECUTE,
// PREPARE, and BATCH requests, shaped according to Kind.
type ResultResponse struct {
	Kind         ResultKind
	Rows         *RowsResult
	SchemaChange *SchemaChangeResult
}

// ResultKind identifies which shape of RESULT body follows the kind field.
type ResultKind byte

// CQL RESULT kinds.
const (
	// ResultKindVoid carries no further data.
	ResultKindVoid ResultKind = 0x01
	// ResultKindRows carries a RowsResult.
	ResultKindRows ResultKind = 0x02
	// ResultKindSetKeyspace carries the new keyspace name.
	ResultKindSetKeyspace ResultKind = 0x03
	// ResultKindPrepared carries a prepared statement ID and its metadata.
	ResultKindPrepared ResultKind = 0x04
	// ResultKindSchemaChange carries a SchemaChangeResult.
	ResultKindSchemaChange ResultKind = 0x05
)

// Marshal encodes the ResultResponse body according to its Kind.
//
// Not yet implemented in this scaffold.
func (r *ResultResponse) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// RowsResult carries the column metadata, row data, and paging state for a
// ResultKindRows response.
type RowsResult struct {
	Metadata    *RowsMetadata
	Rows        []*Row
	PagingState []byte
}

// Marshal encodes the RowsResult body.
//
// Not yet implemented in this scaffold.
func (r *RowsResult) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// RowsMetadata describes the columns of a RowsResult.
type RowsMetadata struct {
	Flags    int
	Columns  []*Column
	RowCount int
}

// Column describes a single result column's keyspace, table, name, and
// CQL type.
type Column struct {
	Keyspace string
	Table    string
	Name     string
	Type     DataType
}

// Row is a single result row: one Value per column.
type Row []*Value

// Value is a single cell's typed, wire-encoded contents.
type Value struct {
	Type  DataType
	Value []byte
}

// DataType identifies a CQL column or bound-parameter type.
type DataType byte

// CQL data type codes, as defined by the CQL binary protocol v4 spec.
const (
	// TypeCustom is a custom (Java-class-named) type.
	TypeCustom DataType = 0x00
	// TypeAscii is the ascii type.
	TypeAscii DataType = 0x01
	// TypeBigInt is the bigint type.
	TypeBigInt DataType = 0x02
	// TypeBlob is the blob type.
	TypeBlob DataType = 0x03
	// TypeBoolean is the boolean type.
	TypeBoolean DataType = 0x04
	// TypeCounter is the counter type.
	TypeCounter DataType = 0x05
	// TypeDecimal is the decimal type.
	TypeDecimal DataType = 0x06
	// TypeDouble is the double type.
	TypeDouble DataType = 0x07
	// TypeFloat is the float type.
	TypeFloat DataType = 0x08
	// TypeInt is the int type.
	TypeInt DataType = 0x09
	// TypeTimestamp is the timestamp type.
	TypeTimestamp DataType = 0x0B
	// TypeUUID is the uuid type.
	TypeUUID DataType = 0x0C
	// TypeVarchar is the varchar type.
	TypeVarchar DataType = 0x0D
	// TypeVarint is the varint type.
	TypeVarint DataType = 0x0E
	// TypeTimeUUID is the timeuuid type.
	TypeTimeUUID DataType = 0x0F
	// TypeInet is the inet type.
	TypeInet DataType = 0x10
	// TypeDate is the date type.
	TypeDate DataType = 0x11
	// TypeTime is the time type.
	TypeTime DataType = 0x12
	// TypeSmallInt is the smallint type.
	TypeSmallInt DataType = 0x13
	// TypeTinyInt is the tinyint type.
	TypeTinyInt DataType = 0x14
	// TypeDuration is the duration type.
	TypeDuration DataType = 0x15
	// TypeList is the list<T> collection type.
	TypeList DataType = 0x20
	// TypeMap is the map<K,V> collection type.
	TypeMap DataType = 0x21
	// TypeSet is the set<T> collection type.
	TypeSet DataType = 0x22
	// TypeUDT is a user-defined type.
	TypeUDT DataType = 0x30
	// TypeTuple is the tuple<...> type.
	TypeTuple DataType = 0x31
)

// SchemaChangeResult describes a DDL operation for a ResultKindSchemaChange
// response.
type SchemaChangeResult struct {
	Change   string
	Target   string
	Keyspace string
	Table    string
}

// Marshal encodes the SchemaChangeResult body.
//
// Not yet implemented in this scaffold.
func (r *SchemaChangeResult) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// PreparedResult carries a prepared statement's ID and its bind/result
// column metadata.
type PreparedResult struct {
	QueryID        []byte
	Metadata       *RowsMetadata
	ResultMetadata *RowsMetadata
}

// ErrorResponse is the CQL ERROR response body: a numeric error code and a
// human-readable message.
type ErrorResponse struct {
	Code    int
	Message string
}

// Marshal encodes the ErrorResponse body.
//
// Not yet implemented in this scaffold.
func (r *ErrorResponse) Marshal(codec *Codec, w io.Writer) error {
	return errors.ErrNotImplemented
}

// ConsistencyLevel identifies a CQL request's required consistency level.
type ConsistencyLevel byte

// CQL consistency levels.
const (
	// ConsistencyAny requires at least one replica, including hinted handoff.
	ConsistencyAny ConsistencyLevel = 0x00
	// ConsistencyOne requires exactly one replica to respond.
	ConsistencyOne ConsistencyLevel = 0x01
	// ConsistencyTwo requires two replicas to respond.
	ConsistencyTwo ConsistencyLevel = 0x02
	// ConsistencyThree requires three replicas to respond.
	ConsistencyThree ConsistencyLevel = 0x03
	// ConsistencyQuorum requires a quorum of all replicas across all
	// datacenters.
	ConsistencyQuorum ConsistencyLevel = 0x04
	// ConsistencyAll requires every replica to respond.
	ConsistencyAll ConsistencyLevel = 0x05
	// ConsistencyLocalQuorum requires a quorum of replicas in the local
	// datacenter only.
	ConsistencyLocalQuorum ConsistencyLevel = 0x06
	// ConsistencyEachQuorum requires a quorum of replicas in each
	// datacenter.
	ConsistencyEachQuorum ConsistencyLevel = 0x07
	// ConsistencyLocalOne requires exactly one replica in the local
	// datacenter.
	ConsistencyLocalOne ConsistencyLevel = 0x0A
)

// BatchType identifies the semantics of a BatchRequest.
type BatchType byte

// CQL batch types.
const (
	// BatchTypeLogged applies the batch atomically via a batchlog.
	BatchTypeLogged BatchType = 0
	// BatchTypeUnlogged applies the batch without atomicity guarantees.
	BatchTypeUnlogged BatchType = 1
	// BatchTypeCounter batches counter updates.
	BatchTypeCounter BatchType = 2
)
