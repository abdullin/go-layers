package subspace

import (
	"github.com/happypancake/fdb-go/fdb"
	"github.com/happypancake/fdb-go/fdb/tuple"
)

type Subspace struct {
	RawPrefix []byte
}

func New(rawPrefix string) {
	return Subspace{rawPrefix}
}
func (space Subspace) WithTuple(tuple tuple.Tuple) Subspace {
	return Subspace{space.RawPrefix + tuple.Pack()}
}

func (space Subspace) AsFoundationDbKey() fdb.Key {
	return fdb.Key(space.RawPrefix)
}
