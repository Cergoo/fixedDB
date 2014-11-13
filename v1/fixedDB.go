// (c) 2014 Cergoo
// under terms of ISC license

package fixedDB

import (
	"encoding/binary"
	"github.com/Cergoo/gol/err"
	"github.com/Cergoo/gol/jsonConfig"
	"github.com/Cergoo/gol/stack/stacklf"
	"github.com/Cergoo/gol/sync/mrswUint/mrswd"
	"math"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	conExtensionDB = ".fd" // data base file extension
	conExtensionHB = ".fh" // header base file extension

	errMismatchLen   = "Mismatch len of record"
	errMismatchMaxID = "Mismatch id"
	errMaxID         = "MaxId need a < MaxUint64"
	errMaxLength     = "Need 0 < Max record length < MaxUint16"
	errEmptyRecord   = "Empty record"
)

const (
	prefixLen      = 2
	ErrMismatchLen = iota
	ErrMismatchMaxID
	ErrMaxID
	ErrMaxLength
	ErrEmptyRecord
)

type (
	// TRecord type
	TRecord struct {
		ID  uint64
		Val []byte
	}

	tDispatcher struct {
	}

	// TDB it's a base main struct
	TDB struct {
		h          THeaderBase
		fWriter    *os.File
		fReaders   chan *os.File
		chWriter   chan *TRecord
		dispatcher *mrswd.TDispatcher
		closeWait  sync.WaitGroup
		rStack     stacklf.Tstack
		bufStack   stacklf.Tstack
	}
	// THeaderBase type: first item maxid, second recordLength
	THeaderBase struct {
		MaxId  uint64
		Length uint32
	}
)

// Create new BD files
func CreateDB(path string, h THeaderBase) (e error) {
	if h.MaxId == math.MaxUint64 {
		e = err.New(errMaxID, ErrMaxID)
		return
	}
	if h.Length > math.MaxUint16 || h.Length <= 0 {
		e = err.New(errMaxLength, ErrMaxLength)
		return
	}
	h.Length += prefixLen

	var f *os.File
	// check file exist
	_, e = os.Stat(path + conExtensionDB)
	if e == nil {
		return os.ErrExist
	}
	// create file
	f, e = os.Create(path + conExtensionDB)
	defer f.Close()
	if e != nil {
		return
	}
	// resize
	e = f.Truncate(int64(h.MaxId * uint64(h.Length)))
	if e != nil {
		return
	}
	// save database config
	jsonConfig.Save(h, path+conExtensionHB)
	return
}

// TruncateDB truncate data base file, if error then panic
func TruncateDB(path string, maxId uint64) {
	if maxId == math.MaxUint64 {
		panic(err.New(errMaxID, ErrMaxID))
		return
	}
	h := THeaderBase{}
	jsonConfig.Load(path+conExtensionHB, &h)
	if h.MaxId == maxId {
		return
	}
	h.MaxId = maxId
	e := os.Truncate(path+conExtensionDB, int64(h.MaxId*uint64(h.Length)))
	err.Panic(e)
	jsonConfig.Save(h, path+conExtensionHB)
}

// OpenDB open existing database for work
// readersCount - count of a threads readers;
// timeAccess  - time to access record;  hdd ~ 1000mks,  ssd ~ 200mks
func OpenDB(file string, readersCount uint16, timeAccess time.Duration) (db *TDB) {
	var e error
	db = &TDB{
		chWriter:   make(chan *TRecord, 100),
		dispatcher: mrswd.New(readersCount, timeAccess),
		rStack:     stacklf.Tstack{},
		bufStack:   stacklf.Tstack{},
		fReaders:   make(chan *os.File, readersCount),
	}

	db.fWriter, e = os.OpenFile(file+conExtensionDB, os.O_RDWR, 0777)
	err.Panic(e)

	defer func() {
		if e != nil {
			for f := range db.fReaders {
				f.Close()
			}
		}
	}()

	for i := 0; i < int(readersCount); i++ {
		f, e := os.OpenFile(file+conExtensionDB, os.O_RDONLY, 0777)
		err.Panic(e)
		db.fReaders <- f
	}

	jsonConfig.Load(file+conExtensionHB, &db.h)
	db.closeWait.Add(1)

	go db.set()
	return db
}

// Close close opened DB
func (t *TDB) Close() {
	close(t.chWriter)
	t.closeWait.Wait()
	var f *os.File
	for i := 0; i < len(t.fReaders); i++ {
		f = <-t.fReaders
		f.Close()
	}
	t.fWriter.Close()
}

// Header get header DB
func (t *TDB) Header() THeaderBase {
	return t.h
}

// NewRecord create new record
func (t *TDB) NewRecord() (r *TRecord) {
	v := t.rStack.Pop()
	if v == nil {
		return &TRecord{Val: make([]byte, prefixLen, t.h.Length)}
	}
	r = v.(*TRecord)
	r.Val = r.Val[:prefixLen]
	return
}

// Set set value
func (t *TDB) Set(record *TRecord) (e error) {
	if len(record.Val) > int(t.h.Length) {
		e = err.New(errMismatchLen, ErrMismatchLen)
		return
	}
	if len(record.Val) <= prefixLen {
		e = err.New(errEmptyRecord, ErrEmptyRecord)
		return
	}
	if record.ID > t.h.MaxId {
		e = err.New(errMismatchMaxID, ErrMismatchMaxID)
		return
	}

	binary.LittleEndian.PutUint16(record.Val[:prefixLen], uint16(len(record.Val)-prefixLen))
	t.chWriter <- record
	return
}

// Del set zero length value
func (t *TDB) Del(record *TRecord) (e error) {
	if record.ID > t.h.MaxId {
		e = err.New(errMismatchMaxID, ErrMismatchMaxID)
		return
	}
	copy(record.Val[:2], []byte{0, 0})
	t.chWriter <- record
	return
}

// Get get value
func (t *TDB) Get(id uint64) (valReal []byte, e error) {
	if id > t.h.MaxId {
		e = err.New(errMismatchMaxID, ErrMismatchMaxID)
		return
	}

	var val []byte
	v := t.bufStack.Pop()
	if v == nil {
		val = make([]byte, t.h.Length)
	} else {
		val = v.([]byte)
	}

	threadId := t.dispatcher.RLock(id)
	fd := <-t.fReaders
	_, e = fd.ReadAt(val, int64(id*uint64(t.h.Length)))
	t.fReaders <- fd
	t.dispatcher.RUnlock(threadId)
	err.Panic(e)
	// check a real record length if 0 then record deleted
	ln := binary.LittleEndian.Uint16(val[:2])
	if ln == 0 || ln > uint16(t.h.Length) {
		t.bufStack.Push(val)
		return
	}

	valReal = make([]byte, ln)
	copy(valReal, val[2:2+ln])
	t.bufStack.Push(val)
	return
}

// single threade writer
func (t *TDB) set() {
	defer t.closeWait.Done()
	var e syscall.Errno
	fd := t.fWriter
	for record := range t.chWriter {
		t.dispatcher.Lock(record.ID)
		fd.WriteAt(record.Val, int64(record.ID*uint64(t.h.Length)))
		t.dispatcher.Unlock()
		t.rStack.Push(record) // reuse
		if e != 0 {
			panic(error(e))
		}
	}
}
