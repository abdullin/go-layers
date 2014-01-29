/*
Package queue provides a high-contention queue class. It is a part of
FoundationDb layer.

Queue has two operating modes. The high contention mode (default) is
designed for environments where many clients will be popping the queue
simultaneously. Pop operations in this mode are slower when performed in
isolation, but their performance scales much better with the number of
popping clients.

If high contention mode is off, then no attempt will be made to avoid
transaction conflicts in pop operations. This mode performs well with
only one popping client, but will not scale well to many popping clients.

This code is a port from official python layer
*/

package queue

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"time"
)

type Queue struct {
	Subspace       subspace.Subspace
	HighContention bool
	conflictedPop  subspace.Subspace // stores int64 index, randId []byte
	conflictedItem subspace.Subspace
	queueItem      subspace.Subspace
}

// New queue is created within a given subspace
func New(sub subspace.Subspace, highContention bool) Queue {

	conflict := sub.Sub("conflict")
	pop := sub.Sub("pop")
	item := sub.Sub("item")

	return Queue{sub, highContention, pop, conflict, item}
}

// Clear all items from the queue
func (queue *Queue) Clear(tr fdb.Transaction) {
	tr.ClearRange(queue.Subspace)
}

// Peek at value of the next item without popping it
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

	start, end := sub.FDBRangeKeys()

	key := tr.GetKey(fdb.LastLessThan(end)).GetOrPanic()

	if i := bytes.Compare(key, []byte(start.FDBKey())); i < 0 {
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

// Push a single item onto the queue
func (queue *Queue) Push(tr fdb.Transaction, value []byte) {
	snap := tr.Snapshot()
	index := queue.GetNextIndex(snap, queue.queueItem)
	queue.pushAt(tr, value, index)
}

// Pop the next item from the queue. Cannot be composed with other functions
// in a single transaction.
func (queue *Queue) Pop(db fdb.Database) (value []byte, ok bool) {

	if queue.HighContention {
		if result, ok := queue.popHighContention(db); ok {
			return decodeValue(result), true
		}
	} else {

		val, _ := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			if result, ok := queue.popSimple(tr); ok {
				return decodeValue(result), nil
			}
			return nil, nil

		})
		if val != nil {
			return val.([]byte), true
		}

	}
	return
}

// pushAt inserts item in the queue at (index, randomId) position. Items
// pushed at the same time will have the same index, and so their ordering
// will be random
// This makes pushes fast and usually conflict free (unless the queue becomes)
// empty during the push
func (queue *Queue) pushAt(tr fdb.Transaction, value []byte, index int64) {
	key := queue.queueItem.Pack(tuple.Tuple{index, nextRandom()})
	val := encodeValue(value)

	tr.Set(fdb.Key(key), val)
}

// popSimple gets the message without trying to avoid conflicts
// if many clients are trying to pop simultaneously, only one will be able to
// succeed at a time.
func (queue *Queue) popSimple(tr fdb.Transaction) (value []byte, ok bool) {
	if kv, ok := queue.getFirstItem(tr); ok {
		tr.Clear(kv.Key)
		return kv.Value, true
	}

	return
}

func (queue *Queue) addConflictedPop(tr fdb.Transaction, forced bool) (val []byte) {
	index := queue.GetNextIndex(tr.Snapshot(), queue.conflictedPop)

	if (index == 0) && (!forced) {
		return nil
	}
	key := queue.conflictedPop.Pack(tuple.Tuple{index, nextRandom()})
	// why do we read no
	_ = tr.Get(fdb.Key(key))
	tr.Set(fdb.Key(key), []byte(""))
	return key
}

func errIsCommitFailure(e error) bool {
	if nil != e {
		if f, ok := e.(fdb.Error); ok {
			if f.Code == 1020 {
				return true
			}
		}
	}
	return false
}

// popHighContention attempts to avoid collisions by registering
// itself in a semi-ordered set of poppers if it doesn't initially succeed.
// It then enters a polling loop where it attempts to fulfill outstanding pops
// and then checks to see if it has been fulfilled.
func (queue *Queue) popHighContention(db fdb.Database) (value []byte, ok bool) {
	//panic("Not implemented")
	backoff := 0.01

	tr, err := db.CreateTransaction()

	if err != nil {
		panic(err)
	}

	// Check if there are other people waiting to be popped. If so, we
	// cannot pop before them.

	waitKey := queue.addConflictedPop(tr, false)
	if waitKey == nil {
		value, ok := queue.popSimple(tr)

		// if we managed to commit without collisions

		if err := tr.Commit().GetWithError(); err == nil {
			return value, ok
		} else {
			if !errIsCommitFailure(err) {
				panic(err)
			}
		}

	}

	if err := tr.Commit().GetWithError(); err != nil {
		fmt.Println("Panic in #", err)
	}

	if waitKey == nil {
		waitKey = queue.addConflictedPop(tr, true)
	}

	t, err := queue.conflictedPop.Unpack(fdb.Key(waitKey))
	if err != nil {
		panic(err)
	}

	randId := t[1].([]byte)
	// The result of the pop will be stored at this key once it has been fulfilled
	resultKey := queue.conflictedItemKey(randId)

	tr.Reset()

	for {
		for done := queue.fulfilConflictedPops(db); !done; {

		}

		tr.Reset()
		value := tr.Get(fdb.Key(waitKey))
		result := tr.Get(fdb.Key(resultKey))

		// If waitKey is present, then we have not been fulfilled
		if value.IsReady() {
			time.Sleep(time.Duration(backoff) * time.Second)
			backoff = backoff * 2
			if backoff > 1 {
				backoff = 1
			}
			continue
		}
		if !result.IsReady() {
			return nil, false
		}
		tr.Clear(fdb.Key(resultKey))
		tr.Commit().BlockUntilReady()

		return result.GetOrPanic(), true

	}

	return nil, false
}

func (queue *Queue) getWaitingPops(tr fdb.Transaction, numPops int) fdb.RangeResult {
	return tr.GetRange(queue.conflictedPop, fdb.RangeOptions{Limit: numPops})
}

func (queue *Queue) getItems(tr fdb.Transaction, numPops int) fdb.RangeResult {
	return tr.GetRange(queue.queueItem, fdb.RangeOptions{Limit: numPops})
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

			tuple, err := queue.conflictedPop.Unpack(pop.Key)
			if err != nil {
				panic(err)
			}

			storageKey := queue.conflictedItemKey(tuple[1].([]byte))
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

	if err != nil {
		panic(err)
	}

	return v.(bool)
}

func nextRandom() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err == nil {
		return b
	} else {

		panic(err)
	}
}

// Empty returns true is queue does not have any messages
func (queue *Queue) Empty(tr fdb.Transaction) bool {
	_, ok := queue.getFirstItem(tr)
	return ok == false
}

func (queue *Queue) getFirstItem(tr fdb.Transaction) (kv fdb.KeyValue, ok bool) {
	r := queue.queueItem
	opt := fdb.RangeOptions{Limit: 1}

	if kvs := tr.GetRange(r, opt).GetSliceOrPanic(); len(kvs) == 1 {
		return kvs[0], true
	}
	return
}
