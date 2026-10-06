package protocol

import (
	"encoding/binary"
	"io"
)

// An Encoder writes length-prefixed messages to a stream. It is not
// safe for concurrent use.
type Encoder struct {
	w io.Writer
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w,
	}
}

// Encode writes msg as one frame. It returns ErrMessageTooLarge when
// the encoded message is longer than MaxMessageSize.
func (e *Encoder) Encode(msg Message) error {
	payload, err := marshal(msg)
	if err != nil {
		return err
	}

	if len(payload) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	var header [4]byte

	binary.BigEndian.PutUint32(
		header[:],
		uint32(len(payload)),
	)

	if _, err := e.w.Write(header[:]); err != nil {
		return err
	}

	_, err = e.w.Write(payload)
	return err
}
