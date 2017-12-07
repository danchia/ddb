package storage

// Iterator is an iterator over keys and values.
// Iterators MUST be closed when done.
type Iterator interface {
	// Key returns the current key.
	Key() string

	// Value returns the current value. Callers should not modify the returned value.
	// Returned value is only valid until the next call to Next.
	Value() []byte

	// Next advances the iterator. Returns whether there is another key/value.
	Next() bool

	// Close closes the iterator.
	Close()
}
