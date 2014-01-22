package queue

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"github.com/happypancake/go-layers/subspace"
)

type Queue struct {
	Subspace       subspace.Subspace
	HighContention bool
	conflictedPop  subspace.Subspace
	conflictedItem subspace.Subspace
	queueItem      subspace.Subspace
}

func New(sub subspace.Subspace, highContention bool) Queue {

	conflict := sub.Item(tuple.Tuple{"conflict"})
	pop := sub.Item(tuple.Tuple{"pop"})
	item := sub.Item(tuple.Tuple{"item"})

	return Queue{sub, highContention, pop, conflict, item}
}

func (queue *Queue) Clear(tr fdb.Transaction) {
	tr.ClearRange(queue.Subspace.FullRange())
}

func (queue *Queue) Peek(tr fdb.Transaction) (value []byte, ok bool) {
	if val, ok := queue.getFirstItem(tr); ok {
		return decodeValue(val.Value), true
	}
	return

}

func decodeValue(val []byte) []byte {
	if t, err := tuple.Unpack(val); err != nil {
		panic(err)
	} else {
		return t[0].([]byte)
	}

}
func encodeValue(value []byte) []byte {
	return tuple.Tuple{value}.Pack()
}

type KeyReader interface {
	GetKey(key fdb.Selectable) fdb.FutureKey
}

// to make private
func (queue *Queue) GetNextIndex(tr KeyReader, sub subspace.Subspace) int64 {

	r := sub.Range(tuple.Tuple{})

	key := tr.GetKey(fdb.LastLessThan(r.End)).GetOrPanic()

	if i := bytes.Compare(key, []byte(r.BeginKey())); i < 0 {
		return 0
	}

	if t, err := sub.Unpack(key); err != nil {
		panic("Failed to unpack key")
	} else {
		return t[0].(int64) + 1
	}
}

func (queue *Queue) GetNextQueueIndex(tr fdb.Transaction) int64 {
	return queue.GetNextIndex(tr.Snapshot(), queue.queueItem)
}
func (queue *Queue) Push(tr fdb.Transaction, value []byte) {
	snap := tr.Snapshot()
	index := queue.GetNextIndex(snap, queue.queueItem)
	queue.pushAt(tr, value, index)
}
func (queue *Queue) pushAt(tr fdb.Transaction, value []byte, index int64) {
	key := queue.queueItem.Pack(tuple.Tuple{index, nextRandom()})
	val := encodeValue(value)

	tr.Set(fdb.Key(key), val)
}

// popSimple does not attempt to avoid conflicts
// if many clients are trying to pop simultaneously, only one will be able to succeed at a time.
func (queue *Queue) popSimple(tr fdb.Transaction) (value []byte, ok bool) {
	if kv, ok := queue.getFirstItem(tr); ok {
		tr.Clear(kv.Key)
		return kv.Value, true
	}
	return
}

func nextRandom() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err == nil {
		return b
	} else {
		fmt.Println("Panic", err)
		panic(err)
	}
}

func (queue *Queue) Empty(tr fdb.Transaction) bool {
	_, ok := queue.getFirstItem(tr)
	return ok == false
}

func (queue *Queue) getFirstItem(tr fdb.Transaction) (kv fdb.KeyValue, ok bool) {
	r := queue.queueItem.FullRange()
	opt := fdb.RangeOptions{Limit: 1}

	if kvs := tr.GetRange(r, opt).GetSliceOrPanic(); len(kvs) == 1 {
		return kvs[0], true
	}
	return
}
