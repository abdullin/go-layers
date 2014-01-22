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
	tr.Clear(queue.Subspace.AsFoundationDbKey())
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
	tr.Set(fdb.Key(key), value)
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
	return !ok
}

func (queue *Queue) getFirstItem(tr fdb.Transaction) (fdb.KeyValue, bool) {
	r := queue.queueItem.FullRange()
	res := tr.GetRange(r, fdb.RangeOptions{Limit: 1}).GetSliceOrPanic()

	if len(res) == 0 {
		return fdb.KeyValue{}, false
	}

	return res[0], true
}
