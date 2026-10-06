package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// A MessageType says what one message on a stream means.
type MessageType uint8

// The message types of one invocation stream.
const (
	MessageStart MessageType = iota + 1
	MessageInput
	MessageOutput
	MessageAck
	MessageHeartbeat
	MessageCancel
	MessageComplete
	MessageError
)

// A Message is one framed message on a stream. Payload carries the
// bytes the type defines, and is empty for a type that has none.
type Message struct {
	Type       MessageType
	Sequence   uint64
	Invocation string
	Payload    []byte
}

// The payload layout is one type byte, an 8-byte big-endian sequence, a
// 2-byte invocation length with its bytes, then the payload bytes.

var (
	errTruncated         = errors.New("protocol: truncated message")
	errInvocationTooLong = errors.New("protocol: invocation field too long")
)

func marshal(msg Message) ([]byte, error) {
	if len(msg.Invocation) > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: %d bytes", errInvocationTooLong, len(msg.Invocation))
	}

	buf := make([]byte, 11+len(msg.Invocation)+len(msg.Payload))
	buf[0] = byte(msg.Type)
	binary.BigEndian.PutUint64(buf[1:9], msg.Sequence)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(msg.Invocation)))
	copy(buf[11:], msg.Invocation)
	copy(buf[11+len(msg.Invocation):], msg.Payload)
	return buf, nil
}

func unmarshal(payload []byte) (Message, error) {
	if len(payload) < 11 {
		return Message{}, errTruncated
	}

	var msg Message
	msg.Type = MessageType(payload[0])
	msg.Sequence = binary.BigEndian.Uint64(payload[1:9])

	n := int(binary.BigEndian.Uint16(payload[9:11]))
	if len(payload) < 11+n {
		return Message{}, errTruncated
	}
	msg.Invocation = string(payload[11 : 11+n])
	msg.Payload = payload[11+n:]
	return msg, nil
}
