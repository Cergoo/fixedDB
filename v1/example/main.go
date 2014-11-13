package main

import (
	"fmt"
	"github.com/Cergoo/fixedDB/v1"
	"time"
)

func main() {
	var (
		e error
		b []byte
	)
	fixedDB.CreateDB("./db/1", fixedDB.THeaderBase{100, 16})
	db := fixedDB.OpenDB("./db/1", 10, 1000)

	r := db.NewRecord()
	r.ID = 1
	r.Val = append(r.Val, []byte("test")...)

	e = db.Set(r) // asynchronous write

	time.Sleep(1 * time.Second)
	b, e = db.Get(1)
	fmt.Println(string(b), e)

	db.Del(r) // asynchronous write

	time.Sleep(1 * time.Second)
	b, e = db.Get(1)
	fmt.Println(string(b), e)

	db.Close()
}
