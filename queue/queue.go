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

// Pop the next item from the queue. Cannot be composed with other functions in a single transaction.
func (queue *Queue) Pop(db fdb.Database) (value []byte, ok bool) {
	ret, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return queue.popInner(tr)
	})

	if err == nil {
		return ret.([]byte), true
	}
	return

}

func (queue *Queue) popInner(tr fdb.Transaction) (value []byte, err error) {
	if queue.HighContention {
		if result, ok := queue.popHighContention(tr); ok {
			return decodeValue(result), nil
		}
	} else {
		if result, ok := queue.popSimple(tr); ok {
			return decodeValue(result), nil
		}
	}
	return
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

func (queue *Queue) addConflictedPop(tr fdb.Transaction, forced bool) (val []byte, ok bool) {
	index := queue.GetNextIndex(tr.Snapshot(), queue.conflictedPop)

	if index == 0 && !forced {
		return nil, false
	}
	key := queue.conflictedPop.Pack(tuple.Tuple{index, nextRandom()})
	//read := tr.Get(key)
	tr.Set(fdb.Key(key), []byte(""))
	return key, true
}

func (queue *Queue) popSimpleOrRegisterWaitKey(tr fdb.Transaction) (value []byte, waitKey []byte) {

	// TODO: deal with FDB error in defer

	// Check if there are other people waiting to be popped. If so, we
	// cannot pop before them.
	if key, ok := queue.addConflictedPop(tr, false); ok {
		tr.Commit().BlockUntilReady()
		return nil, key
	} else {
		// No one else was waiting to be popped
		value, ok = queue.popSimple(tr)
		tr.Commit().BlockUntilReady()
		return value, nil
	}
}

// popHighContention attempts to avoid collisions by registering
// itself in a semi-ordered set of poppers if it doesn't initially succeed.
// It then enters a polling loop where it attempts to fulfill outstanding pops
// and then checks to see if it has been fulfilled.
func (queue *Queue) popHighContention(tr fdb.Transaction) (value []byte, ok bool) {
	//panic("Not implemented")
	//backoff := 0.01

	var waitKey []byte

	if value, waitKey := queue.popSimpleOrRegisterWaitKey(tr); value == nil {
		return value, true
	}

	randId := queue.conflictedPop.MustUnpack(waitKey)[1].([]byte)
	// The result of the pop will be stored at this key once it has been fulfilled
	resultKey := queue.conflictedItem.Item(tuple.Tuple{randId})

	tr.Reset()

	return nil, false
}

func (queue *Queue) getWaitingPops(tr fdb.Transaction, numPops int) fdb.RangeResult {
	r := queue.conflictedPop.FullRange()
	return tr.GetRange(r, fdb.RangeOptions{Limit: numPops})
}

func (queue *Queue) getItems(tr fdb.Transaction, numPops int) fdb.RangeResult {
	r := queue.queueItem.FullRange()
	return tr.GetRange(r, fdb.RangeOptions{Limit: numPops})
}

func minLength(a, b []fdb.KeyValue) int {
	if al, bl := len(a), len(b); al < bl {
		return al
	} else {
		return bl
	}

}

func (queue *Queue) conflictedItemKey(subkey []byte) []byte {
	return queue.conflictedItem.Pack(tuple.Tuple{subkey})
}

func (queue *Queue) fulfilConflictedPops(db fdb.Database) bool {
	numPops := 100

	v, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		pops := queue.getWaitingPops(tr, numPops).GetSliceOrPanic()
		items := queue.getItems(tr, numPops).GetSliceOrPanic()

		min := minLength(pops, items)

		for i := 0; i < min; i++ {
			pop, k, v := pops[i], items[i].Key, items[i].Value

			key := queue.conflictedPop.MustUnpack(pop.Key)
			storageKey := queue.conflictedItemKey(key[0].([]byte))
			tr.Set(fdb.Key(storageKey), v)
			_ = tr.Get(k)
			_ = tr.Get(pop.Key)
			tr.Clear(pop.Key)
			tr.Clear(k)
		}

		for _, pop := range pops[min:] {
			_ = tr.Get(pop.Key)
			tr.Clear(pop.Key)
		}

		return len(pops) < numPops, nil

	})
	return v.(bool)
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
