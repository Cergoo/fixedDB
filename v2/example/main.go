package main

import (
	"fmt"
	"github.com/Cergoo/fixedDB/v2"
)

func main() {
	var (
		db  *fixedDBv2.TDB
		key []byte
	)
	e := fixedDBv2.CreateDB("./db", fixedDBv2.THeaderCreate{FileSize: 1, Buckets: []uint32{10, 20, 3000}})
	fmt.Println("create: ", e)
	db, e = fixedDBv2.OpenDB("./db")
	fmt.Println("open: ", e)

	val := make([]byte, 8)
	val = append(val, []byte("test1")...)
	key, e = db.Set(nil, val, true)
	fmt.Println("set: ", key)
	fmt.Println("get: ", db.Get(key))

	//db.Del([]byte{20, 0, 0, 0, 0, 0, 1, 0, 0, 0})
	db.Close()
}
