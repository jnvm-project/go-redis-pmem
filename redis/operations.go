package redis

import (
	"time"
	"errors"
	"fmt"
	"math/rand"

	"github.com/vmware/go-pmem-transaction/transaction"
)

type Operations struct {
	s Server
}

func (op *Operations) Del(key string) error {
	bkey := []byte(key)

	txn("undo") {
	op.s.db.lockKeyWrite(bkey)

	if !op.s.db.delete(bkey) {
		fmt.Printf("fail deleteing key=%s\n", key)
		return errors.New("fail deleting key\n")
	}
	}

	//fmt.Printf("[Del] key=%s\n", key)

	return nil
}

func (op *Operations) Get(key string) ([]byte,error){
	var v []byte
	var ok bool
	bkey := []byte(key)

	txn("undo") {
	op.s.db.lockKeyRead(bkey)

	i := op.s.db.lookupKeyRead(bkey)
	v, ok = getString(i)
	if !ok {
		fmt.Printf("failed Getting key=%s\n", key)
		return nil,errors.New("fail Getting key key\n")
	}
	}

	//fmt.Printf("[Get] key=%s value=%s\n", key, v)

	return v, nil
}

func (op *Operations) Set(key string, value string, expiration time.Duration) error {
	txn("undo") {
	op.s.db.lockKeyWrite([]byte(key))

	/* returns true if update and false if insert */
	op.s.db.setKey(shadowCopyToPmem([]byte(key)), shadowCopyToPmemI([]byte(value)))
	}

	//fmt.Printf("[Set] key=%s value=%s\n", key, value)

	return nil
}

func (op *Operations) Hget(key string, field string) ([]byte,error){
	bkey := []byte(key)
	var v []byte
	var ok bool

	txn("undo") {
	op.s.db.lockKeyRead(bkey)

	/* Fetch the dictionnary associated to key in global dict */
	i := op.s.db.lookupKeyRead(bkey)
	if i == nil {
		fmt.Printf("failed looking up key=%s\n", key)
	}
	/* Fetch the field in the dictionnary retrieved above */
	v, ok = getString(hashTypeGetValue(i, []byte(field)))
	if !ok {
		fmt.Printf("failed Getting key=%s\n", key)
		return nil,errors.New("fail Getting key key\n")
	}
	}

	//fmt.Printf("[Hget] key=%s field=%s, value=%s\n", key, field, v)

	return v, nil
}

func (op *Operations) GetLen(t int) int {
	return len(op.s.db.dict.tab[t].bucket)
}

func (op *Operations) Hset(key string, values map[string][]byte) error {
	//why not lockKeysWrite(argv[1:],2)? it takes all indices at modulo 2. Does it lock fields ?
	//arv[0]->
	//argv[1]->key
	//argv[2]->field
	//argv[3]->value
	txn("undo") {
	op.s.db.lockKeyWrite([]byte(key))

	o := myHashTypeLookupWriteOrCreate(&op.s, []byte(key))
	switch o.(type) {
	case *dict:
		if o == nil {
			fmt.Printf("fail looking up or creating\n")
			return errors.New("fail looking up or creating\n")
		}
	default:
		panic("Wrong type")
	}

	for field, val := range values {
		myHashTypeSet(op.s.db, []byte(key), o, shadowCopyToPmem([]byte(field)), shadowCopyToPmemI(val))
		//fmt.Printf("[Hset] key=%s field=%s val=%s\n", key, field, val)
	}
	}

	return nil
}

func myHashTypeLookupWriteOrCreate(s *Server, key []byte) interface{} {
	o := s.db.lookupKeyWrite(key)
	if o == nil {
		o = NewDict(10, 10) // implicitly convert to interface
		s.db.setKey(shadowCopyToPmem(key), o)
	}
	return o
}

func myHashTypeSet(db *redisDb, key []byte, o interface{}, field []byte, value interface{}) bool {
	var update bool
	switch d := o.(type) {
	case *dict:
		_, _, _, de := d.find(field)
		if de != nil {
			txn("undo") {
			de.value = value
			update = true
			}
		} else {
			d.set(field, value)
			go myhashTypeBgResize(db, key)
		}
	default:
		panic("Unknown hash encoding")
	}
	return update
}

func myhashTypeBgResize(db *redisDb, key []byte) {
	if key == nil {
		return
	}
	// only triger resize with some probability.
	p := rand.Intn(100)
	if p > 5 {
		return
	}
	txn("undo") {
	rehash := true
	for rehash {
		// need to lock and get kv pair in every transaction
		db.lockKeyWrite(key)
		o := db.lookupKeyWrite(key)
		var d *dict
		switch v := o.(type) {
		case *dict:
			d = v
		case *zset:
			d = v.dict
		default:
			rehash = false
		}
		if d != nil {
			if d.rehashIdx == -1 {
				_, _, size1 := d.resizeIfNeeded()
				if size1 == 0 {
					rehash = false
				} else {
					//println("Rehash hash key", string(key), "to size", size1)
				}
			} else if d.rehashIdx == -2 {
				d.rehashSwap()
				rehash = false
			} else {
				d.rehashStep()
			}
		}
	}
	}
}

func CreateOperations(filepath string)(Operations) {
	op := Operations{}

	op.s = Server{}
	op.s.Init(filepath)

	return op
}
