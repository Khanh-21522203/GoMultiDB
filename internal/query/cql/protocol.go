package cql

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	protocolVersion = 0x04
	cqlHeaderSize   = 9
)

// Opcode identifies CQL protocol opcodes.
type Opcode byte

const (
	OpcodeError       Opcode = 0x00
	OpcodeStartup     Opcode = 0x01
	OpcodeReady       Opcode = 0x02
	OpcodeAuthenticate Opcode = 0x03
	OpcodeOptions     Opcode = 0x05
	OpcodeSupported   Opcode = 0x06
	OpcodeQuery       Opcode = 0x07
	OpcodeResult      Opcode = 0x08
	OpcodePrepare     Opcode = 0x09
	OpcodeExecute     Opcode = 0x0A
	OpcodeRegister    Opcode = 0x0B
	OpcodeEvent       Opcode = 0x0C
	OpcodeBatch       Opcode = 0x0D
	OpcodeAuthChallenge Opcode = 0x0E
	OpcodeAuthResponse  Opcode = 0x0F
	OpcodeAuthSuccess    Opcode = 0x10
)

// Frame represents a CQL binary protocol frame.
type Frame struct {
	Version   byte
	Flags     byte
	Stream    int16
	Opcode    Opcode
	Length    int
	Body      []byte
}

// MarshalBinary encodes the frame to wire format.
func (f *Frame) MarshalBinary() ([]byte, error) {
	if len(f.Body) > 0xffffff {
		return nil, fmt.Errorf("frame body too large: %d", len(f.Body))
	}
	buf := make([]byte, cqlHeaderSize+len(f.Body))
	buf[0] = f.Version
	buf[1] = f.Flags
	binary.BigEndian.PutUint16(buf[2:4], uint16(f.Stream))
	buf[4] = byte(f.Opcode)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(f.Body)))
	copy(buf[9:], f.Body)
	return buf, nil
}

// UnmarshalBinary decodes a frame from wire format.
func (f *Frame) UnmarshalBinary(data []byte) error {
	if len(data) < cqlHeaderSize {
		return fmt.Errorf("frame too short: %d < %d", len(data), cqlHeaderSize)
	}
	f.Version = data[0]
	f.Flags = data[1]
	f.Stream = int16(binary.BigEndian.Uint16(data[2:4]))
	f.Opcode = Opcode(data[4])
	f.Length = int(binary.BigEndian.Uint32(data[5:9]))
	if len(data) < cqlHeaderSize+f.Length {
		return fmt.Errorf("incomplete frame: expected %d bytes, got %d", cqlHeaderSize+f.Length, len(data))
	}
	f.Body = data[9 : 9+f.Length]
	return nil
}

// Codec handles CQL type encoding/decoding.
type Codec struct{}

// NewCodec creates a new CQL codec.
func NewCodec() *Codec {
	return &Codec{}
}

// WriteString writes a [string] (ushort length + bytes).
func (c *Codec) WriteString(w io.Writer, s string) error {
	buf := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(s)))
	copy(buf[2:], s)
	_, err := w.Write(buf)
	return err
}

// WriteLongString writes a [long string] (int length + bytes).
func (c *Codec) WriteLongString(w io.Writer, s string) error {
	buf := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(s)))
	copy(buf[4:], s)
	_, err := w.Write(buf)
	return err
}

// WriteBytes writes [bytes] (int length + bytes, or -1 for null).
func (c *Codec) WriteBytes(w io.Writer, data []byte) error {
	if data == nil {
		return binary.Write(w, binary.BigEndian, int32(-1))
	}
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	_, err := w.Write(buf)
	return err
}

// WriteShort writes a [short] (int16).
func (c *Codec) WriteShort(w io.Writer, v int16) error {
	return binary.Write(w, binary.BigEndian, v)
}

// WriteInt writes an [int] (int32).
func (c *Codec) WriteInt(w io.Writer, v int32) error {
	return binary.Write(w, binary.BigEndian, v)
}

// WriteLong writes a [long] (int64).
func (c *Codec) WriteLong(w io.Writer, v int64) error {
	return binary.Write(w, binary.BigEndian, v)
}

// WriteByte writes a single byte.
func (c *Codec) WriteByte(w io.Writer, v byte) error {
	return binary.Write(w, binary.BigEndian, v)
}

// ReadByte reads a single byte.
func (c *Codec) ReadByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

// WriteStringList writes a [string list] (ushort count + strings).
func (c *Codec) WriteStringList(w io.Writer, items []string) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(items))); err != nil {
		return err
	}
	for _, item := range items {
		if err := c.WriteString(w, item); err != nil {
			return err
		}
	}
	return nil
}

// WriteStringMap writes a [string map] (ushort count + key-value pairs).
func (c *Codec) WriteStringMap(w io.Writer, m map[string]string) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := c.WriteString(w, k); err != nil {
			return err
		}
		if err := c.WriteString(w, v); err != nil {
			return err
		}
	}
	return nil
}

// ReadString reads a [string].
func (c *Codec) ReadString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadLongString reads a [long string].
func (c *Codec) ReadLongString(r io.Reader) (string, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadBytes reads [bytes].
func (c *Codec) ReadBytes(r io.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadShort reads a [short].
func (c *Codec) ReadShort(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ReadInt reads an [int].
func (c *Codec) ReadInt(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ReadLong reads a [long].
func (c *Codec) ReadLong(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ReadStringList reads a [string list].
func (c *Codec) ReadStringList(r io.Reader) ([]string, error) {
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	items := make([]string, count)
	for i := range items {
		s, err := c.ReadString(r)
		if err != nil {
			return nil, err
		}
		items[i] = s
	}
	return items, nil
}

// ReadStringMap reads a [string map].
func (c *Codec) ReadStringMap(r io.Reader) (map[string]string, error) {
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	m := make(map[string]string, count)
	for i := uint16(0); i < count; i++ {
		k, err := c.ReadString(r)
		if err != nil {
			return nil, err
		}
		v, err := c.ReadString(r)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// Connection wraps a net.Conn with CQL framing.
type Connection struct {
	conn net.Conn
	codec *Codec
}

// NewConnection creates a new CQL connection.
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:  conn,
		codec: NewCodec(),
	}
}

// ReadFrame reads a complete CQL frame from the connection.
func (c *Connection) ReadFrame() (*Frame, error) {
	header := make([]byte, cqlHeaderSize)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(header[5:9]))
	if length > 256*1024*1024 { // 256MB max frame size
		return nil, fmt.Errorf("frame too large: %d", length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}
	frame := &Frame{
		Version: header[0],
		Flags:   header[1],
		Stream:  int16(binary.BigEndian.Uint16(header[2:4])),
		Opcode:  Opcode(header[4]),
		Length:  length,
		Body:    body,
	}
	return frame, nil
}

// WriteFrame writes a CQL frame to the connection.
func (c *Connection) WriteFrame(frame *Frame) error {
	data, err := frame.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = c.conn.Write(data)
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.conn.Close()
}
