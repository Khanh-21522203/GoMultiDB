package cql

import (
	"io"
	"net"

	errors "GoMultiDB/v2/contracts/errors"
)

// protocolVersion is the CQL binary protocol version this gateway speaks.
const protocolVersion = 0x04

// cqlHeaderSize is the fixed size, in bytes, of a CQL frame header.
const cqlHeaderSize = 9

// Opcode identifies a CQL protocol frame's operation.
type Opcode byte

// CQL protocol opcodes, as defined by the CQL binary protocol v4 spec.
const (
	// OpcodeError marks a frame carrying an ERROR response.
	OpcodeError Opcode = 0x00
	// OpcodeStartup marks a client STARTUP request.
	OpcodeStartup Opcode = 0x01
	// OpcodeReady marks a server READY response.
	OpcodeReady Opcode = 0x02
	// OpcodeAuthenticate marks a server AUTHENTICATE challenge.
	OpcodeAuthenticate Opcode = 0x03
	// OpcodeOptions marks a client OPTIONS request.
	OpcodeOptions Opcode = 0x05
	// OpcodeSupported marks a server SUPPORTED response.
	OpcodeSupported Opcode = 0x06
	// OpcodeQuery marks a client QUERY request.
	OpcodeQuery Opcode = 0x07
	// OpcodeResult marks a server RESULT response.
	OpcodeResult Opcode = 0x08
	// OpcodePrepare marks a client PREPARE request.
	OpcodePrepare Opcode = 0x09
	// OpcodeExecute marks a client EXECUTE request.
	OpcodeExecute Opcode = 0x0A
	// OpcodeRegister marks a client REGISTER request for event notifications.
	OpcodeRegister Opcode = 0x0B
	// OpcodeEvent marks a server EVENT push notification.
	OpcodeEvent Opcode = 0x0C
	// OpcodeBatch marks a client BATCH request.
	OpcodeBatch Opcode = 0x0D
	// OpcodeAuthChallenge marks a server AUTH_CHALLENGE response.
	OpcodeAuthChallenge Opcode = 0x0E
	// OpcodeAuthResponse marks a client AUTH_RESPONSE request.
	OpcodeAuthResponse Opcode = 0x0F
	// OpcodeAuthSuccess marks a server AUTH_SUCCESS response.
	OpcodeAuthSuccess Opcode = 0x10
)

// Frame represents a single CQL binary protocol frame: a fixed header
// followed by an opcode-specific body.
type Frame struct {
	Version byte
	Flags   byte
	Stream  int16
	Opcode  Opcode
	Length  int
	Body    []byte
}

// MarshalBinary encodes the frame to CQL wire format.
//
// Not yet implemented in this scaffold.
func (f *Frame) MarshalBinary() ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// UnmarshalBinary decodes a frame from CQL wire format.
//
// Not yet implemented in this scaffold.
func (f *Frame) UnmarshalBinary(data []byte) error {
	return errors.ErrNotImplemented
}

// Codec encodes and decodes the CQL protocol's primitive wire types
// ([string], [bytes], [int], [short], [long], [string list], [string map],
// ...).
type Codec struct{}

// NewCodec returns a Codec ready for use.
func NewCodec() *Codec {
	return &Codec{}
}

// WriteString writes a CQL [string] (ushort length + UTF-8 bytes).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteString(w io.Writer, s string) error {
	return errors.ErrNotImplemented
}

// WriteLongString writes a CQL [long string] (int length + UTF-8 bytes).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteLongString(w io.Writer, s string) error {
	return errors.ErrNotImplemented
}

// WriteBytes writes a CQL [bytes] value (int length + raw bytes, or -1 for
// null).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteBytes(w io.Writer, data []byte) error {
	return errors.ErrNotImplemented
}

// WriteShort writes a CQL [short] (big-endian int16).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteShort(w io.Writer, v int16) error {
	return errors.ErrNotImplemented
}

// WriteInt writes a CQL [int] (big-endian int32).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteInt(w io.Writer, v int32) error {
	return errors.ErrNotImplemented
}

// WriteLong writes a CQL [long] (big-endian int64).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteLong(w io.Writer, v int64) error {
	return errors.ErrNotImplemented
}

// WriteRawByte writes a single raw byte.
//
// Not yet implemented in this scaffold.
//
// Named WriteRawByte rather than WriteByte: the latter's (io.Writer, byte)
// signature collides with go vet's stdmethods check against
// io.ByteWriter's WriteByte(byte) error.
func (c *Codec) WriteRawByte(w io.Writer, v byte) error {
	return errors.ErrNotImplemented
}

// ReadRawByte reads a single raw byte.
//
// Not yet implemented in this scaffold.
//
// Named ReadRawByte rather than ReadByte: the latter's (io.Reader) (byte,
// error) signature collides with go vet's stdmethods check against
// io.ByteReader's ReadByte() (byte, error).
func (c *Codec) ReadRawByte(r io.Reader) (byte, error) {
	return 0, errors.ErrNotImplemented
}

// WriteStringList writes a CQL [string list] (ushort count + [string]s).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteStringList(w io.Writer, items []string) error {
	return errors.ErrNotImplemented
}

// WriteStringMap writes a CQL [string map] (ushort count + key/value
// [string] pairs).
//
// Not yet implemented in this scaffold.
func (c *Codec) WriteStringMap(w io.Writer, m map[string]string) error {
	return errors.ErrNotImplemented
}

// ReadString reads a CQL [string].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadString(r io.Reader) (string, error) {
	return "", errors.ErrNotImplemented
}

// ReadLongString reads a CQL [long string].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadLongString(r io.Reader) (string, error) {
	return "", errors.ErrNotImplemented
}

// ReadBytes reads a CQL [bytes] value.
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadBytes(r io.Reader) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// ReadShort reads a CQL [short].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadShort(r io.Reader) (int16, error) {
	return 0, errors.ErrNotImplemented
}

// ReadInt reads a CQL [int].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadInt(r io.Reader) (int32, error) {
	return 0, errors.ErrNotImplemented
}

// ReadLong reads a CQL [long].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadLong(r io.Reader) (int64, error) {
	return 0, errors.ErrNotImplemented
}

// ReadStringList reads a CQL [string list].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadStringList(r io.Reader) ([]string, error) {
	return nil, errors.ErrNotImplemented
}

// ReadStringMap reads a CQL [string map].
//
// Not yet implemented in this scaffold.
func (c *Codec) ReadStringMap(r io.Reader) (map[string]string, error) {
	return nil, errors.ErrNotImplemented
}

// Connection wraps a net.Conn with CQL frame reading and writing.
type Connection struct {
	conn  net.Conn
	codec *Codec
}

// NewConnection wraps conn for CQL frame I/O.
func NewConnection(conn net.Conn) *Connection {
	return &Connection{}
}

// ReadFrame reads one complete CQL frame from the connection.
//
// Not yet implemented in this scaffold.
func (c *Connection) ReadFrame() (*Frame, error) {
	return nil, errors.ErrNotImplemented
}

// WriteFrame writes one CQL frame to the connection.
//
// Not yet implemented in this scaffold.
func (c *Connection) WriteFrame(frame *Frame) error {
	return errors.ErrNotImplemented
}

// Close closes the underlying connection.
//
// Not yet implemented in this scaffold.
func (c *Connection) Close() error {
	return errors.ErrNotImplemented
}
