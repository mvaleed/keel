package invocation

import "fmt"

// maxNameLen bounds one part of a key. A longer part is a defect in the
// caller, and it must not build an unbounded storage key.
const maxNameLen = 128

// Validate reports whether the invocation can be addressed and stored.
//
// A caller supplies the id, so every part of the key is untrusted input
// that becomes a storage path. The rules below keep a part from holding
// a separator, a traversal, or an unbounded name.
func (i Invocation) Validate() error {
	for _, part := range []struct{ field, value string }{
		{"service", i.Service},
		{"handler", i.Handler},
		{"id", string(i.ID)},
	} {
		if err := validName(part.value); err != nil {
			return fmt.Errorf("%s: %w", part.field, err)
		}
	}
	return nil
}

// validName reports whether s is safe as one part of a key. It permits
// letters, digits, and the separators that read well in a path.
func validName(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("%w: empty", ErrInvalid)
	case len(s) > maxNameLen:
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalid, maxNameLen)
	case s == "." || s == "..":
		return fmt.Errorf("%w: %q traverses the key", ErrInvalid, s)
	}

	for idx, r := range s {
		if isAlnum(r) {
			continue
		}
		// A separator is allowed inside the name, never at its start,
		// so a name cannot look like a hidden or an absolute path.
		if idx > 0 && (r == '-' || r == '_' || r == '.' || r == ':') {
			continue
		}
		return fmt.Errorf("%w: %q holds %q at %d", ErrInvalid, s, r, idx)
	}
	return nil
}

func isAlnum(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}
