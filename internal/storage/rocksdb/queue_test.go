package rocksdb

import (
	"fmt"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/xunterr/crawler/internal/storage"
)

func openTest() (*grocksdb.DB, error) {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	return grocksdb.OpenDb(opts, "/tmp/crawler/test/")
}

func TestQueue(t *testing.T) {
	db, err := openTest()
	defer db.Close()
	if err != nil {
		t.Fatal(err.Error())
	}

	storage := NewRocksdbStorage[string](db)
	queue := NewRocksdbQueue(storage, []byte("myqueue1"))
	if err := fill(queue); err != nil {
		t.Fatal(err.Error())
	}

	data, err := queue.Pop()
	if err != nil {
		t.Fatal(err.Error())
	}

	data, err = queue.Pop()
	if err != nil {
		t.Fatal(err.Error())
	}

	if string(data) != "1" {
		t.Fatalf("Wrong data: %s", string(data))
	}
}

func fill(queue *RocksdbQueue[string]) error {
	for i := 0; i < 10; i++ {
		err := queue.Push(fmt.Sprintf("%d", i))
		if err != nil {
			return err
		}
	}
	return nil
}

func TestOverflow(t *testing.T) {
	db, err := openTest()
	defer db.Close()
	if err != nil {
		t.Fatal(err.Error())
	}

	st := NewRocksdbStorage[string](db)
	queue := NewRocksdbQueue(st, []byte("myqueue2"))
	for i := 2; i < 12; i++ {
		queue = NewRocksdbQueue(st, []byte(fmt.Sprintf("myqueue%d", i+1)))
		if err := fill(queue); err != nil {
			t.Fatal(err.Error())
		}
	}

	for i := 0; i < 11; i++ {
		_, err := queue.Pop()
		if err != nil {
			if err == storage.NoNextItem && i == 10 {
				return
			}
			t.Fatal(err.Error())
		}
	}
	t.Fatal("Queue overflowed!")
}
