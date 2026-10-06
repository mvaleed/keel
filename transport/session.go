package transport

import (
	"context"

	"github.com/keel/keel/transport/protocol"
	"golang.org/x/sync/errgroup"
)

// A Session pumps one stream in both directions until the stream fails
// or ctx ends. It is safe for concurrent use.
type Session struct {
	inbound  chan protocol.Message
	outbound chan protocol.Message

	lastSent uint64
	lastAck  uint64
}

// Run reads and writes the stream until either side fails.
func (s *Session) Run(
	ctx context.Context,
	stream Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.readLoop(ctx, stream)
	})

	g.Go(func() error {
		return s.writeLoop(ctx, stream)
	})

	return g.Wait()
}

func (s *Session) readLoop(
	ctx context.Context,
	stream Stream,
) error {
	for {
		msg, err := stream.Recv(ctx)
		if err != nil {
			return err
		}

		select {
		case s.inbound <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Session) writeLoop(
	ctx context.Context,
	stream Stream,
) error {
	for {
		select {
		case msg := <-s.outbound:
			if err := stream.Send(ctx, msg); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
