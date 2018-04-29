package server

// Iter is an iterator over rows.
type Iter interface {
	// Next advances the iterator. Returns true if there is a next value.
	Next() (bool, error)

	// Key returns the key of the current row.
	Key() string

	// Timestamp returns the timestamp of the current row.
	Timestamp() int64

	// Value returns the value of the current row.
	Value() []byte
}

type KTV struct {
	Key       string
	Timestamp int64
	Value     []byte
}

type memIter struct {
	rows  []KTV
	index int
}

// NewIterFromRows returns an Iter represents rows. rows must not be modified after.
func NewIterFromRows(rows []KTV) Iter {
	return &memIter{rows: rows, index: -1}
}

func (i *memIter) Next() (bool, error) {
	if i.index+1 >= len(i.rows) {
		return false, nil
	}

	i.index++
	return true, nil
}

func (i *memIter) Key() string {
	return i.rows[i.index].Key
}

func (i *memIter) Timestamp() int64 {
	return i.rows[i.index].Timestamp
}

func (i *memIter) Value() []byte {
	return i.rows[i.index].Value
}
