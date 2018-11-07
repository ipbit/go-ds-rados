package rados

import (
	"bytes"
	"fmt"
	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"os"
	"testing"
)

func TestPutGetBytes(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("/foo"), []byte("bar")
	err = ds.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ds.Get(key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicQuery(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("/basic"), []byte("basicvalue")
	err = ds.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	q := dsq.Query{Prefix: "/basic"}
	qr, err := ds.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	all, err := qr.Rest()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range all {
		if bytes.Compare(v.Value, val) == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal(fmt.Errorf("Failed to query"))
	}
	err = ds.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
}

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

func TestQuery(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	addTestCases(t, ds, testcases)

	rs, err := ds.Query(dsq.Query{Prefix: "/a/", KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = ds.Query(dsq.Query{
		Prefix:   "/a/",
		Offset:   2,
		Limit:    2,
		KeysOnly: true,
		Orders:   []dsq.Order{dsq.OrderByKey{}}})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)
	removeTestCases(t, ds, testcases)
}

func addTestCases(t *testing.T, ds *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		if err := ds.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		v2, err := ds.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}
}

func removeTestCases(t *testing.T, ds *Datastore, testcases map[string]string) {
	for k, _ := range testcases {
		dsk := datastore.NewKey(k)
		if err := ds.Delete(dsk); err != nil {
			t.Fatal(err)
		}
	}
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func TestPutGetDeleteEmpty(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("empty"), []byte{}
	err = ds.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ds.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	err = ds.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	err = ds.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	err = ds.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ds.Get(key)
	if err == nil {
		t.Fatal(err)
	}
}

func TestGetSize(t *testing.T) {
	ds, err := newOrAbort(t)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("/foo"), []byte("bar")
	err = ds.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}

	size, err := ds.GetSize(key)
	if err != nil {
		t.Fatal(err)
	}
	if size != len("bar") {
		t.Fail()
	}
	err = ds.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
}

func newOrAbort(t *testing.T) (*Datastore, error) {
	confPath := os.Getenv("CEPH_CONF")
	pool := os.Getenv("CEPH_POOL")
	ds, err := NewDatastore(confPath, pool)
	if err != nil {
		t.Log("could not connect to a redis instance")
		t.SkipNow()
	}
	return ds, err
}
