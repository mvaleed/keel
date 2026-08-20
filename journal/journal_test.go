package journal_test

import (
	"encoding/json"
	"errors"
	"iter"
	"strings"
	"testing"

	"github.com/keel/keel/journal"
)

// seq returns a sequence of entries that ends with err, if err is not nil.
func seq(entries []journal.Entry, err error) iter.Seq2[journal.Entry, error] {
	return func(yield func(journal.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
		if err != nil {
			yield(journal.Entry{}, err)
		}
	}
}

func entries(names ...string) []journal.Entry {
	out := make([]journal.Entry, len(names))
	for i, name := range names {
		out[i] = journal.Entry{Step: i, Name: name}
	}
	return out
}

func TestCollect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []journal.Entry
		want int
	}{
		{"empty", nil, 0},
		{"one", entries("a"), 1},
		{"many", entries("a", "b", "c"), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := journal.Collect(seq(tt.in, nil))
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if len(got) != tt.want {
				t.Fatalf("got %d entries, want %d", len(got), tt.want)
			}
			for i := range got {
				if got[i].Step != tt.in[i].Step || got[i].Name != tt.in[i].Name {
					t.Fatalf("entry %d = %+v, want %+v", i, got[i], tt.in[i])
				}
			}
		})
	}
}

func TestCollectKeepsTheEntriesBeforeAnError(t *testing.T) {
	t.Parallel()

	want := errors.New("read failed")
	got, err := journal.Collect(seq(entries("a", "b"), want))

	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	// The caller sees how far the read got, which an operator needs.
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
}

func TestCollectStopsAtTheFirstError(t *testing.T) {
	t.Parallel()

	want := errors.New("read failed")
	pulled := 0
	s := func(yield func(journal.Entry, error) bool) {
		pulled++
		if !yield(journal.Entry{Step: 0, Name: "a"}, nil) {
			return
		}
		pulled++
		if !yield(journal.Entry{}, want) {
			return
		}
		pulled++
		yield(journal.Entry{Step: 1, Name: "never"}, nil)
	}

	if _, err := journal.Collect(s); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	// Collect must stop the sequence, not drain it.
	if pulled != 2 {
		t.Fatalf("yielded %d times, want 2", pulled)
	}
}

func TestCollectPreservesOutput(t *testing.T) {
	t.Parallel()

	want := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"x"}`)}
	got, err := journal.Collect(seq([]journal.Entry{want}, nil))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if string(got[0].Output) != string(want.Output) {
		t.Fatalf("output = %s, want %s", got[0].Output, want.Output)
	}
}

func TestVerifyReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		recorded []journal.Entry
		replayed []journal.Entry
		wantErr  bool
	}{
		{"both empty", nil, nil, false},
		{"identical", entries("a", "b"), entries("a", "b"), false},
		// A replay that is still running has fewer steps than the journal.
		{"replay is shorter", entries("a", "b", "c"), entries("a", "b"), false},
		// A handler that grew at the end has more.
		{"replay is longer", entries("a"), entries("a", "b"), false},
		{"nothing recorded yet", nil, entries("a"), false},
		{"mismatch at the first step", entries("a"), entries("z"), true},
		{"mismatch at a later step", entries("a", "b", "c"), entries("a", "z", "c"), true},
		// The shared prefix is all that is compared.
		{"mismatch past the overlap", entries("a"), entries("a", "z"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := journal.VerifyReplay(tt.recorded, tt.replayed)
			if tt.wantErr && !errors.Is(err, journal.ErrNonDeterministic) {
				t.Fatalf("err = %v, want ErrNonDeterministic", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}

func TestVerifyReplayNamesTheStep(t *testing.T) {
	t.Parallel()

	recorded := []journal.Entry{{Step: 4, Name: "charge"}}
	replayed := []journal.Entry{{Step: 4, Name: "refund"}}

	err := journal.VerifyReplay(recorded, replayed)
	if err == nil {
		t.Fatal("VerifyReplay returned nil, want an error")
	}
	// The message must let an operator find the step that moved.
	for _, want := range []string{"step 4", `"charge"`, `"refund"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err %q does not contain %q", err, want)
		}
	}
}
