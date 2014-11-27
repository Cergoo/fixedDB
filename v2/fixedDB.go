// (c) 2014 Cergoo
// under terms of ISC license

package fixedDBv2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Cergoo/gol/fastbuf"
	"github.com/Cergoo/gol/filepath"
	"github.com/Cergoo/gol/jsonConfig"
	"github.com/Cergoo/gol/stack/bytestack"

	//"github.com/davecgh/go-spew/spew"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
)

/*
header: bodyLen 4 + crc32 4
body:   extensionLen 1 (.jpg, .gif, ...) + extension + data
*/

const (
	headerLen  = 8         // header: bodyLen 4 + crc32 4
	emptyidLen = 6         // file id 2 + record id 4
	keyLen     = 10        // bucket id 4 + file id 2 + record id 4
	logdir     = "log"     //
	logfile    = "log.log" //
)

const (
	// error message
	errMaxRecordLen = "Max len of record %d overhead %d"
	errFileNotFound = "File %d not found in bucket %x"
	// error code
	ErrMaxRecordLen = iota
	ErrFileNotFound
)

const (
	workTypeGet = iota
	workTypeSet
	workTypeDel
)

type (

	// TDB it's a base main struct
	TDB struct {
		chWorker   chan interface{}
		h          THeaderBase    //
		path       string         // path to database
		closeWait  sync.WaitGroup //
		maxDataLen uint32         //
		logErr     *log.Logger    //
		logFile    *os.File       //
	}

	// THeaderBase type: first item maxid, second recordLength
	THeaderBase struct {
		Buckets  tBuckets
		FileSize uint16 // files size Gb
	}

	// THeaderCreate
	THeaderCreate struct {
		Buckets  []uint32 // bucket size bytes
		FileSize uint16   // max files size Gb
	}

	tWorkWrite struct {
		f    *os.File
		at   int64
		key  []byte
		val  []byte
		wait sync.WaitGroup
	}

	tWorkRead struct {
		f  *os.File
		at int64
		ln uint32
		ch chan<- []byte
	}
)

// Create new BD files
func CreateDB(path string, h THeaderCreate) (e error) {
	var (
		dirname    string
		bucketname string
		f          *os.File
	)
	path = filepath.PathEndSeparator(path)

	// create HeaderBase from HeaderCreate
	HeaderBase := THeaderBase{
		Buckets:  make([]*tBucket, 0, len(h.Buckets)),
		FileSize: h.FileSize,
	}

	for _, val := range h.Buckets {
		HeaderBase.Buckets = append(HeaderBase.Buckets, &tBucket{PartLen: val, MaxIdPerFile: int64(h.FileSize) * 1024 * 1024 * 1024 / int64(val)})
	}

	// create dataBase log directory
	e = os.Mkdir(path+logdir, 0666)
	if e != nil {
		return
	}
	// create main log file
	f, e = os.Create(path + logdir + string(os.PathSeparator) + logfile)
	if e != nil {
		return
	}

	// create dataBase from HeaderBase
	for _, val := range HeaderBase.Buckets {
		bucketname = strconv.FormatInt(int64(val.PartLen), 16)
		dirname = path + bucketname

		e = os.Mkdir(dirname, 0666)
		if e != nil {
			return
		}
		f, e = os.Create(dirname + string(os.PathSeparator) + "0")
		if e != nil {
			return
		}
		e = f.Truncate(val.MaxIdPerFile * int64(val.PartLen))
		if e != nil {
			return
		}

		dirname = path + logdir + string(os.PathSeparator) + bucketname + ".empty"
		f, e = os.Create(dirname)
		if e != nil {
			return
		}

	}

	// save database config
	jsonConfig.Save(HeaderBase, path+"config.json")
	return
}

// Open BD files
func OpenDB(path string) (t *TDB, e error) {

	t = &TDB{
		h:        THeaderBase{},
		chWorker: make(chan interface{}, 10),
		path:     filepath.PathEndSeparator(path),
		logErr:   log.New(os.Stderr, "fixedDB: ", log.LstdFlags),
	}

	jsonConfig.Load(t.path+"config.json", &t.h)
	sort.Sort(t.h.Buckets)

	var (
		bucketName string
		tmpstr     string
		log        []byte
		bucketid   uint32
		b          *tBucket
		buketFiles *tBuketFiles
	)

	// range of a buckets in database
	for _, v := range t.h.Buckets {

		v.emptyid = bytestack.New(emptyidLen)
		bucketName = strconv.FormatInt(int64(v.PartLen), 16)

		buketFiles, e = parseBuketFiles(t.path + bucketName)
		if e != nil {
			return
		}

		v.files = make([]*os.File, buketFiles.max+1)

		for _, f := range buketFiles.files {
			v.files[f.valint], e = os.OpenFile(t.path+bucketName+string(os.PathSeparator)+f.valstr, os.O_RDWR, 0666)
			if e != nil {
				return
			}
		}

		// open emptyid files
		tmpstr = t.path + logdir + string(os.PathSeparator) + bucketName
		v.emptyFile, e = os.OpenFile(tmpstr+".empty", os.O_RDWR, 0666)
		if e != nil {
			return
		}
		v.emptyid.Stack, e = ioutil.ReadAll(v.emptyFile)
		if e != nil {
			return
		}
		if len(v.emptyid.Stack) > 0 {
			if len(v.emptyid.Stack) < 8 {
				e = fmt.Errorf("Error parse bucket log file: %x", v.PartLen)
				return
			}
			v.currentId = int64(binary.LittleEndian.Uint32(v.emptyid.Stack[:8]))
			v.emptyid.Stack = v.emptyid.Stack[8:]
		}
	}

	// open log file
	t.logFile, e = os.OpenFile(t.path+logdir+string(os.PathSeparator)+logfile, os.O_RDWR, 0666)
	if e != nil {
		return
	}
	log, e = ioutil.ReadAll(t.logFile)
	if e != nil {
		return
	}
	buf := fastbuf.New(log, 0, nil)
	key := make([]byte, keyLen+1)
	for _, e1 := buf.Read(key); e1 == nil; _, e1 = buf.Read(key) {
		bucketid = uint32(binary.LittleEndian.Uint32(key[1:5]))
		if b, e = t.h.Buckets.Search(bucketid); e != nil {
			return
		}
		if b.PartLen != bucketid {
			e = errors.New("error parse WAL: bucket not found <" + string(key[1:5]) + ">")
			return
		}
		if key[0] == 0 {
			b.emptyid.DelLast(key[5:])
		} else {
			b.emptyid.Push(key[5:])
		}
	}

	if e = t.flashLog(); e != nil {
		return
	}

	t.maxDataLen = t.h.Buckets[len(t.h.Buckets)-1].PartLen

	go t.worker()
	//spew.Dump(t)
	return
}

// Close close opened DB
func (t *TDB) Close() {
	close(t.chWorker)
	t.closeWait.Wait()
}

// Set - Set(key, val)
// Del - Set(key, nil)
func (t *TDB) Set(key []byte, val []byte, sync bool) (newKey []byte, e error) {
	if uint32(len(val)) > t.maxDataLen {
		e = fmt.Errorf(errMaxRecordLen, t.maxDataLen)
		return
	}
	if key == nil {
		if len(val) <= headerLen {
			e = fmt.Errorf("Empty key and empty val")
			return
		}
	} else {
		if len(key) < keyLen {
			e = fmt.Errorf("Bad key value: %v", key)
			return
		}
	}

	if val == nil {
		// remove to old
		bucketid, fileid, recordAt := keyParse(key)
		bucket, _ := t.h.Buckets.Search(bucketid)
		p := &tWorkWrite{
			f:   bucket.files[fileid],
			at:  recordAt,
			key: key,
		}
		p.wait.Add(1)
		t.chWorker <- p
		if sync {
			p.wait.Wait()
		}
		return
	}

	var (
		f1        *os.File
		recordAt1 int64
	)

	binary.LittleEndian.PutUint32(val[:4], uint32(len(val)-headerLen))
	binary.LittleEndian.PutUint32(val[4:8], crc32.ChecksumIEEE(val[headerLen:]))

	bucket, _ := t.h.Buckets.Search(uint32(len(val)))

	if key == nil {
		// write to new
		f1, recordAt1, newKey = t.getNewID(bucket)
		p := &tWorkWrite{
			f:   f1,
			at:  recordAt1,
			key: newKey,
			val: val,
		}
		p.wait.Add(1)
		t.chWorker <- p
		if sync {
			p.wait.Wait()
		}
		return
	}

	bucketid, fileid, recordAt := keyParse(key)

	if bucketid == bucket.PartLen {
		// write to old
		newKey = key
		p := &tWorkWrite{
			f:   bucket.files[fileid],
			at:  recordAt,
			key: key,
			val: val,
		}
		p.wait.Add(1)
		t.chWorker <- p
		if sync {
			p.wait.Wait()
		}
	} else {
		// write to new
		f1, recordAt1, newKey = t.getNewID(bucket)
		p := &tWorkWrite{
			f:   f1,
			at:  recordAt1,
			key: newKey,
			val: val,
		}
		p.wait.Add(1)
		t.chWorker <- p
		// remove to old
		t.chWorker <- &tWorkWrite{
			f:   bucket.files[fileid],
			at:  recordAt,
			key: key,
		}
		if sync {
			p.wait.Wait()
		}
	}

	return
}

func (t *TDB) Get(key []byte) []byte {
	if len(key) != keyLen {
		return nil
	}
	bucketid, fileid, recordAt := keyParse(key)
	bucket, _ := t.h.Buckets.Search(bucketid)
	if bucketid != bucket.PartLen {
		return nil
	}

	ch := make(chan []byte, 1)
	t.chWorker <- &tWorkRead{
		f:  bucket.files[fileid],
		at: recordAt,
		ln: bucketid,
		ch: ch,
	}
	return <-ch
}

func (t *TDB) flashLog() (e error) {
	for _, b := range t.h.Buckets {
		v := make([]byte, 8, len(b.emptyid.Stack)+8)
		binary.LittleEndian.PutUint32(v, uint32(b.currentId))
		v = append(v, b.emptyid.Stack...)
		e = b.emptyFile.Truncate(0)
		if e != nil {
			return
		}
		_, e = b.emptyFile.Seek(0, 0)
		if e != nil {
			return
		}
		_, e = b.emptyFile.Write(v)
		if e != nil {
			return
		}
	}
	_, e = t.logFile.Seek(0, 0)
	if e != nil {
		return
	}
	e = t.logFile.Truncate(0)
	return
}

// addFile add new part file to bucket
func (t *TDB) addFile(b *tBucket) (e error) {
	var f *os.File
	bucketName := strconv.FormatInt(int64(b.PartLen), 16)
	fileName := t.path + bucketName + string(os.PathSeparator) + strconv.FormatInt(int64(len(b.files)), 10)
	f, e = os.Create(fileName)
	if e != nil {
		return
	}
	e = f.Truncate(b.MaxIdPerFile * int64(b.PartLen))
	if e != nil {
		return
	}
	b.files = append(b.files, f)
	return
}

// getNewID get new id from bucket
func (t *TDB) getNewID(bucket *tBucket) (f *os.File, recordAt int64, newkey []byte) {
	newkey = make([]byte, keyLen)
	binary.LittleEndian.PutUint32(newkey, bucket.PartLen)
	emptyid := bucket.emptyid.PopPoint()
	if emptyid != nil {
		f = bucket.files[binary.LittleEndian.Uint16(emptyid[:2])]
		recordAt = int64(binary.LittleEndian.Uint32(emptyid[2:])) * int64(bucket.PartLen)
		copy(newkey[4:], emptyid)
	} else {
		if bucket.currentId > bucket.MaxIdPerFile {
			t.addFile(bucket)
			bucket.currentId = 0
		}
		fid := uint16(len(bucket.files) - 1)
		f = bucket.files[fid]
		recordAt = int64(bucket.currentId) * int64(bucket.PartLen)
		binary.LittleEndian.PutUint16(newkey[4:6], fid)
		binary.LittleEndian.PutUint32(newkey[6:], uint32(bucket.currentId))
		bucket.currentId++
	}
	return
}

func keyParse(key []byte) (bucketid uint32, fileid uint16, recordAt int64) {
	bucketid = binary.LittleEndian.Uint32(key[:4])
	fileid = binary.LittleEndian.Uint16(key[4:6])
	recordAt = int64(binary.LittleEndian.Uint32(key[6:])) * int64(bucketid)
	return
}

// disk operations worker
func (t *TDB) worker() {
	t.closeWait.Add(1)
	defer func() {
		t.flashLog()
		// close all file
		for _, v := range t.h.Buckets {
			for _, f := range v.files {
				f.Close()
			}
			v.emptyFile.Close()
		}
		t.logFile.Close()
		t.closeWait.Done()
	}()

	var (
		w        interface{}
		wr       *tWorkRead
		ww       *tWorkWrite
		e        error
		logCount uint32
	)
	logkey := make([]byte, keyLen+1)
	valNil := []byte{0, 0, 0, 0}

	const maxLogCount = 1000

	for w = range t.chWorker {
		switch w.(type) {
		case *tWorkRead:
			wr = w.(*tWorkRead)
			buf := make([]byte, wr.ln)
			_, e = wr.f.ReadAt(buf, wr.at)
			if e != nil {
				wr.ch <- nil
				t.logErr.Panicln(e)
			}
			wr.ch <- buf
		case *tWorkWrite:
			ww = w.(*tWorkWrite)
			if ww.val == nil {
				logkey[0] = 0
				ww.val = valNil
			} else {
				logkey[0] = 1
			}
			copy(logkey[1:], ww.key)

			_, e = t.logFile.Write(logkey)
			if e != nil {
				t.logErr.Panicln(e)
			}

			_, e = ww.f.WriteAt(ww.val, ww.at)
			if e != nil {
				t.logErr.Panicln(e)
			}
			ww.wait.Done()

			logCount++
			if logCount > maxLogCount {
				e = t.flashLog()
				if e != nil {
					t.logErr.Panicln(e)
				}
				logCount = 0
			}
		}
	}
}
