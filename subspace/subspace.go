package subspace

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
)

type Subspace struct {
	RawPrefix []byte
}

func New(tuple tuple.Tuple) Subspace {
	return Subspace{tuple.Pack()}
}

func (space Subspace) WithPrefix(prefix string) Subspace {
	buf := concat([]byte(prefix), space.RawPrefix)
	return Subspace{buf}
}

func (space Subspace) AsFoundationDbKey() fdb.Key {
	return fdb.Key(space.RawPrefix)
}

func (space Subspace) Item(tuple tuple.Tuple) Subspace {
	return Subspace{concat(space.RawPrefix, tuple.Pack())}
}

func concat(old1, old2 []byte) []byte {
	newslice := make([]byte, len(old1)+len(old2))
	copy(newslice, old1)
	copy(newslice[len(old1):], old2)
	return newslice
}

func (space Subspace) Range(tuple tuple.Tuple) fdb.KeyRange {

	rang := tuple.Range() // KeyRange{Key,Key}

	begin := concat(space.RawPrefix, []byte(rang.BeginKey()))
	end := concat(space.RawPrefix, []byte(rang.EndKey()))

	return fdb.KeyRange{fdb.Key(begin), fdb.Key(end)}
}

func (space Subspace) Unpack(key fdb.Key) (tuple.Tuple, error) {

	l := len(space.RawPrefix)

	return tuple.Unpack(key[l:])
}
func (space Subspace) Pack(tuple tuple.Tuple) []byte {
	return concat(space.RawPrefix, tuple.Pack())

}
