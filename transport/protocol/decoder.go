package protocol

import (
	"encoding/binary"
	"io"
)

// A Decoder reads length-prefixed messages from a stream. It is not
// safe for concurrent use.
type Decoder struct {
	r io.Reader
}

// NewDecoder returns a Decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r,
	}
}

// Decode reads the next message. It returns ErrMessageTooLarge when a
// frame header announces more than MaxMessageSize bytes.
func (d *Decoder) Decode() (Message, error) {
	var header [4]byte

	if _, err := io.ReadFull(d.r, header[:]); err != nil {
		return Message{}, err
	}

	size := binary.BigEndian.Uint32(header[:])

	if size > MaxMessageSize {
		return Message{}, ErrMessageTooLarge
	}

	payload := make([]byte, size)

	if _, err := io.ReadFull(d.r, payload); err != nil {
		return Message{}, err
	}

	return unmarshal(payload)
}
