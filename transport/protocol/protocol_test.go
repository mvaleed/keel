package protocol_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/keel/keel/transport/protocol"
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  protocol.Message
	}{
		{"start", protocol.Message{Type: protocol.MessageStart, Invocation: "abc"}},
		{"no payload", protocol.Message{Type: protocol.MessageAck, Sequence: 7, Invocation: "a/b/c"}},
		{"payload", protocol.Message{
			Type:       protocol.MessageOutput,
			Sequence:   1 << 40,
			Invocation: "01HZX",
			Payload:    []byte(`{"ok":true}`),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			if err := protocol.NewEncoder(&buf).Encode(tt.msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}

			got, err := protocol.NewDecoder(&buf).Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Type != tt.msg.Type || got.Sequence != tt.msg.Sequence || got.Invocation != tt.msg.Invocation {
				t.Fatalf("message = %+v, want %+v", got, tt.msg)
			}
			if !bytes.Equal(got.Payload, tt.msg.Payload) {
				t.Fatalf("payload = %q, want %q", got.Payload, tt.msg.Payload)
			}
		})
	}
}

func TestEncodeTooLarge(t *testing.T) {
	t.Parallel()

	msg := protocol.Message{Payload: make([]byte, protocol.MaxMessageSize)}
	if err := protocol.NewEncoder(io.Discard).Encode(msg); !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Fatalf("Encode err = %v, want ErrMessageTooLarge", err)
	}
}

func TestDecodeTooLarge(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff})
	if _, err := protocol.NewDecoder(&buf).Decode(); !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Fatalf("Decode err = %v, want ErrMessageTooLarge", err)
	}
}

func TestDecodeTruncated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		len  uint32
		body []byte
	}{
		{"short payload", 5, []byte{1, 0, 0, 0, 0}},
		{"invocation cut", 12, []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 'a'}},
		{"body cut", 100, bytes.Repeat([]byte{0}, 10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			header := []byte{byte(tt.len >> 24), byte(tt.len >> 16), byte(tt.len >> 8), byte(tt.len)}
			buf.Write(header)
			buf.Write(tt.body)

			_, err := protocol.NewDecoder(&buf).Decode()
			if err == nil {
				t.Fatal("Decode err = nil, want an error")
			}
			if errors.Is(err, protocol.ErrMessageTooLarge) {
				t.Fatalf("Decode err = %v, want a truncation error", err)
			}
		})
	}
}
