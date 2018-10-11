package rados

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"os"
	"strings"
	"sync"
)

type Datastore struct {
	mu       sync.Mutex
	conn     *rados.Conn
	confPath string
	pool     string
}

func NewDatastore(confPath string, pool string) (*Datastore, error) {
	var err error
	ds := &Datastore{confPath: confPath, pool: pool}
	ds.conn, err = rados.NewConn()
	if err != nil {
		return nil, err
	}

	err = ds.conn.ReadConfigFile(confPath)
	if err != nil {
		return nil, err
	}
	err = ds.conn.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to rados\n")
		return nil, err
	}
	return ds, nil
}

func (ds *Datastore) Shutdown() {
	ds.conn.Shutdown()
}

func (ds *Datastore) Put(key datastore.Key, value []byte) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.Write(key.String(), value, 0)
	return err
}

func (ds *Datastore) Get(key datastore.Key) (value []byte, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	var ioctx *rados.IOContext
	ioctx, err = ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return
	}
	defer ioctx.Destroy()
	var result bytes.Buffer
	var buf []byte = make([]byte, 1024)
	var offset uint64
	for {
		var count int
		count, err = ioctx.Read(key.String(), buf, offset)
		if err != nil {
			if err == rados.RadosErrorNotFound {
				err = datastore.ErrNotFound
				return
			}
			return
		}
		if count < len(buf) {
			result.Write(buf[:count])
			break
		}
		offset += uint64(count)
		result.Write(buf)
	}
	value = result.Bytes()
	return
}

func (ds *Datastore) Delete(key datastore.Key) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.Delete(key.String())
	return err
}

func (ds *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return nil, err
	}

	reschan := make(chan dsq.Result, dsq.KeysOnlyBufSize)
	go func() {
		defer close(reschan)
		defer ioctx.Destroy()
		iter, err := ioctx.Iter()
		defer iter.Close()
		if err != nil {
			reschan <- dsq.Result{Error: errors.New("Failed to fetch rados iterator")}
			return
		}
		for iter.Next() {
			if q.Prefix != "" && !strings.HasPrefix(iter.Value(), q.Prefix) {
				continue
			}
			if q.KeysOnly {
				reschan <- dsq.Result{Entry: dsq.Entry{Key: iter.Value()}}
			} else {
				v, err := ds.Get(datastore.NewKey(iter.Value()))
				if err != nil {
					fmt.Errorf("Failed to fetch value for key '%s'", iter.Value())
					return
				}
				reschan <- dsq.Result{Entry: dsq.Entry{Key: iter.Value(), Value: v}}
			}
		}
	}()
	qr := dsq.ResultsWithChan(q, reschan)

	for _, f := range q.Filters {
		qr = dsq.NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = dsq.NaiveOrder(qr, o)
	}
	qr = dsq.NaiveOffset(qr, q.Offset)
	if q.Limit > 0 {
		qr = dsq.NaiveLimit(qr, q.Limit)
	}
	return qr, nil
}

func (ds *Datastore) Has(key datastore.Key) (exists bool, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return
	}
	defer ioctx.Destroy()
	_, err = ioctx.Stat(key.String())
	if err != nil {
		if err == rados.RadosErrorNotFound {
			err = nil
			return
		}
		return
	} else {
		exists = true
	}
	return
}

func (ds *Datastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *Datastore) Close() error {
	ds.Shutdown()
	return nil
}
