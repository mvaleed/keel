package protocol

import "errors"

// MaxMessageSize bounds the encoded payload of one frame, so one peer
// cannot exhaust the other with a single write.
const MaxMessageSize = 4 << 20

// ErrMessageTooLarge is returned when a message exceeds MaxMessageSize.
var ErrMessageTooLarge = errors.New("protocol: message exceeds MaxMessageSize")
