package transport

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/keel/keel/transport/protocol"
)

// A Stream is one ordered, bidirectional sequence of messages between
// the engine and a worker. Implementations must be safe for concurrent
// use.
type Stream interface {
	// Recv reads the next message from the stream.
	Recv(ctx context.Context) (protocol.Message, error)

	// Send writes one message to the stream.
	Send(ctx context.Context, msg protocol.Message) error

	// Close ends the stream.
	Close() error
}

type stream struct {
	body    io.ReadCloser
	writer  http.ResponseWriter
	flusher *http.ResponseController

	decoder *protocol.Decoder
	encoder *protocol.Encoder

	writeMu sync.Mutex
}

// NewStream turns one HTTP request and its response writer into a
// Stream. It writes the response header, so a caller must not do it.
func NewStream(
	w http.ResponseWriter,
	r *http.Request,
) (Stream, error) {
	w.Header().Set(
		"Content-Type",
		"application/x-invocation-stream",
	)

	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)

	if err := rc.Flush(); err != nil {
		return nil, err
	}

	return &stream{
		body:    r.Body,
		writer:  w,
		flusher: rc,
		decoder: protocol.NewDecoder(r.Body),
		encoder: protocol.NewEncoder(w),
	}, nil
}

func (s *stream) Recv(
	ctx context.Context,
) (protocol.Message, error) {
	return s.decoder.Decode()
}

func (s *stream) Send(
	ctx context.Context,
	msg protocol.Message,
) error {
	if err := s.encoder.Encode(msg); err != nil {
		return err
	}

	return s.flusher.Flush()
}

func (s *stream) Close() error {
	return nil
}
