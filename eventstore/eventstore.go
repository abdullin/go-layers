package eventstore

import (
	_ "bytes"
	"crypto/rand"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	_ "sync"
	"time"
)

func nextRandom() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err == nil {
		return b
	} else {

		panic(err)
	}
}

type EventRecord struct {
	contract string
	Data     []byte
	Meta     []byte
}

type EventStore struct {
	space subspace.Subspace
}

func (es *EventStore) Clear(db fdb.Database) {

	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(es.space)
		return nil, nil
	})

}

func (es *EventStore) Append(db fdb.Database, stream string, records []EventRecord) {

	rand := nextRandom()

	globalSpace := es.space.Sub("glob", rand)

	// TODO add random key to reduce contention

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		// TODO : use get next index to sort them more nicely

		for _, evt := range records {

			gKey := globalSpace.Sub(time.Now().Unix(), evt.contract)
			//sKey := streamSpace.Item(tuple.Tuple{time.Now().Unix(), evt.contract})

			// TODO - join data and meta
			tr.Set(gKey.Sub("data"), evt.Data)
			tr.Set(gKey.Sub("meta"), evt.Meta)
			//tr.Set(sKey.Item(tuple.Tuple{"data"}).AsFoundationDbKey(), evt.Data)
			//tr.Set(sKey.Item(tuple.Tuple{"meta"}).AsFoundationDbKey(), evt.Meta)

		}

		return nil, nil

	})

	if err != nil {
		panic(err)
	}

}
