package invocation_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/keel/keel/invocation"
)

func inv(service, handler, id string) invocation.Invocation {
	return invocation.Invocation{Service: service, Handler: handler, ID: invocation.ID(id)}
}

func TestKey(t *testing.T) {
	t.Parallel()

	got := inv("billing", "Charge", "order-1").Key()
	if got != "billing/Charge/order-1" {
		t.Fatalf("Key() = %q, want billing/Charge/order-1", got)
	}
}

func TestKeyIsUnambiguous(t *testing.T) {
	t.Parallel()

	// The old key joined the parts with a dash, so these two addressed
	// one journal. Two workflows must never share a journal.
	a := inv("a-b", "h", "c").Key()
	b := inv("a", "h", "b-c").Key()
	if a == b {
		t.Fatalf("%q and %q are the same key", a, b)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      invocation.Invocation
		wantErr bool
	}{
		{"plain", inv("billing", "Charge", "order-1"), false},
		{"separators inside", inv("bill_ing", "Charge.v2", "order:1.a-b"), false},
		{"digits", inv("s1", "h2", "3"), false},
		{"max length", inv("s", "h", strings.Repeat("a", 128)), false},

		{"empty service", inv("", "h", "i"), true},
		{"empty handler", inv("s", "", "i"), true},
		{"empty id", inv("s", "h", ""), true},
		{"too long", inv("s", "h", strings.Repeat("a", 129)), true},

		// The id is client-supplied, so it must not reach out of its
		// own prefix or address another tenant.
		{"slash in id", inv("s", "h", "a/b"), true},
		{"traversal in id", inv("s", "h", "../../other"), true},
		{"dot id", inv("s", "h", "."), true},
		{"dotdot id", inv("s", "h", ".."), true},
		{"slash in service", inv("a/b", "h", "i"), true},
		{"traversal in handler", inv("s", "..", "i"), true},
		{"leading dot", inv("s", "h", ".hidden"), true},
		{"leading dash", inv("s", "h", "-x"), true},
		{"backslash", inv("s", "h", `a\b`), true},
		{"space", inv("s", "h", "a b"), true},
		{"null byte", inv("s", "h", "a\x00b"), true},
		{"newline", inv("s", "h", "a\nb"), true},
		{"percent encoded slash", inv("s", "h", "a%2Fb"), true},
		{"unicode", inv("s", "h", "café"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.in.Validate()
			if tt.wantErr && !errors.Is(err, invocation.ErrInvalid) {
				t.Fatalf("Validate() = %v, want ErrInvalid", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestValidateNamesTheField(t *testing.T) {
	t.Parallel()

	err := inv("s", "h", "a/b").Validate()
	if err == nil || !strings.Contains(err.Error(), "id") {
		t.Fatalf("err = %v, want it to name the id field", err)
	}
}

// A rejected name must never become a key that leaves its own prefix.
func TestValidateRejectsEveryEscapingKey(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"a/b", "../x", "..", ".", "/abs", "a/../../b"} {
		in := inv("s", "h", bad)
		if err := in.Validate(); err == nil {
			t.Fatalf("Validate() accepted id %q, key would be %q", bad, in.Key())
		}
	}
}

func TestHashInput(t *testing.T) {
	t.Parallel()

	same := invocation.HashInput([]byte(`{"a":1}`))
	if same != invocation.HashInput([]byte(`{"a":1}`)) {
		t.Fatal("the same input gave two hashes")
	}
	if same == invocation.HashInput([]byte(`{"a":2}`)) {
		t.Fatal("a different input gave the same hash")
	}
	if invocation.HashInput(nil) == "" {
		t.Fatal("HashInput(nil) is empty, want the hash of no bytes")
	}
}

func TestNewIDIsValid(t *testing.T) {
	t.Parallel()

	// The engine mints an id only when it needs one, and it must pass
	// the same rules a client id does.
	id, err := invocation.NewID()
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	if err := inv("s", "h", string(id)).Validate(); err != nil {
		t.Fatalf("NewID gave %q, which Validate rejects: %v", id, err)
	}
}

func TestCompact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"already compact", `{"a":1}`, `{"a":1}`},
		{"spaced", "{ \"a\" :\n 1 }", `{"a":1}`},
		{"nested", `{ "a": [ 1, 2 ] }`, `{"a":[1,2]}`},
		{"scalar", ` 5 `, `5`},
		{"empty", ``, ``},
		{"whitespace only", "  \n", ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := invocation.Compact(json.RawMessage(tt.in))
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("Compact(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCompactRejectsBadJSON(t *testing.T) {
	t.Parallel()

	if _, err := invocation.Compact(json.RawMessage(`{`)); !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

// Formatting alone must not make one registration look like a conflict
// with itself.
func TestCompactMakesEqualInputsHashEqual(t *testing.T) {
	t.Parallel()

	a, _ := invocation.Compact(json.RawMessage(`{"a":1,"b":[2]}`))
	b, _ := invocation.Compact(json.RawMessage("{ \"a\" : 1 ,\n\"b\" : [ 2 ] }"))
	if invocation.HashInput(a) != invocation.HashInput(b) {
		t.Fatalf("%q and %q hash differently", a, b)
	}
}
