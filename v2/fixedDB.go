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
	"github.com/Cergoo/gol/sync/mrswString/mrswd"

	//"github.com/davecgh/go-spew/spew"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"
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

type (
	tSetRecord struct {
		key, val []byte
		newkey   chan<- []byte
	}

	// TDB it's a base main struct
	TDB struct {
		h          THeaderBase      //
		path       string           // path to database
		chWriter   chan *tSetRecord //
		closeWait  sync.WaitGroup   //
		maxDataLen uint32           //
		logErr     *log.Logger      //
		logFile    *os.File         //
		dispatcher mrswd.TDispatcher
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
)

// Create new BD files
func CreateDB(path string, h THeaderCreate) (e error) {
	var (
		dirname, bucketname string
		f                   *os.File
	)
	path = filepath.PathEndSeparator(path)

	// create HeaderBase from HeaderCreate
	HeaderBase := THeaderBase{Buckets: make([]*tBucket, 0, len(h.Buckets))}
	HeaderBase.FileSize = h.FileSize
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
func OpenDB(path string, threadCount uint8, timeOnSleep time.Duration) (t *TDB, e error) {

	t = &TDB{
		h:          THeaderBase{},
		chWriter:   make(chan *tSetRecord, 10),
		path:       filepath.PathEndSeparator(path),
		logErr:     log.New(os.Stderr, "fixedDB: ", log.LstdFlags),
		dispatcher: mrswd.New(uint16(threadCount), timeOnSleep),
	}
	//fmt.Println(t.path)
	jsonConfig.Load(t.path+"config.json", &t.h)
	sort.Sort(t.h.Buckets)

	var (
		dir        []os.FileInfo
		f          os.FileInfo
		i1, imax   int64
		bucketName string
		fileName   string
		tmpstr     string
		log        []byte
		bucketid   uint32
		b          *tBucket
	)

	// range of a buckets in database
	for _, v := range t.h.Buckets {
		v.emptyid = bytestack.New(emptyidLen)
		bucketName = strconv.FormatInt(int64(v.PartLen), 16)
		if dir, e = ioutil.ReadDir(t.path + bucketName); e != nil {
			return
		}

		if len(dir) == 0 {
			e = errors.New("Not database file: " + t.path + bucketName + "/")
			return
		}

		// get max file name
		for _, f = range dir {
			i1, e = strconv.ParseInt(f.Name(), 10, 32)
			if e != nil {
				e = errors.New("It's not database file: " + t.path + bucketName + "/" + f.Name())
				return
			}
			if i1 > imax {
				imax = i1
			}
		}

		v.files = make([]*tAccessor, imax+1)

		// range of a files in bucket
		for _, f = range dir {
			i1, _ = strconv.ParseInt(f.Name(), 10, 32)
			fileName = t.path + bucketName + string(os.PathSeparator) + f.Name()
			v.files[i1] = new(tAccessor)
			if v.files[i1].writer, e = os.OpenFile(fileName, os.O_RDWR, 0666); e != nil {
				return
			}
			v.files[i1].reader = make([]*os.File, threadCount)
			for j := range v.files[i1].reader {
				v.files[i1].reader[j], e = os.OpenFile(fileName, os.O_RDONLY, 0666)
				if e != nil {
					return
				}
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
	t.closeWait.Add(1)
	go t.set()
	//spew.Dump(t)
	return
}

// Close close opened DB
func (t *TDB) Close() {
	// wait finished write
	close(t.chWriter)
	t.closeWait.Wait()

	// flash Log
	t.flashLog()

	// close all file
	for _, v := range t.h.Buckets {
		for _, f := range v.files {
			f.writer.Close()
			for j := range f.reader {
				f.reader[j].Close()
			}
		}
		v.emptyFile.Close()
	}
	t.logFile.Close()
}

//
func (t *TDB) Set(key []byte, val []byte) (newkey <-chan []byte, e error) {
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

	binary.LittleEndian.PutUint32(val[:4], uint32(len(val)-headerLen))
	binary.LittleEndian.PutUint32(val[4:8], crc32.ChecksumIEEE(val[headerLen:]))

	r := make(chan []byte)
	t.chWriter <- &tSetRecord{key: key, val: val, newkey: r}
	newkey = r
	return
}

func (t *TDB) Del(key []byte) {
	if len(key) == keyLen {
		t.chWriter <- &tSetRecord{key: key}
	}
}

func (t *TDB) Get(key []byte) (val []byte) {
	if len(key) != keyLen {
		return
	}
	bucketid, fileid, recordAt := keyParse(key)
	bucket, _ := t.h.Buckets.Search(bucketid)
	if bucketid != bucket.PartLen {
		return
	}

	i := t.dispatcher.RLock(string(key))
	f := bucket.files[fileid].reader[i]
	val = make([]byte, bucket.PartLen)
	_, e := f.ReadAt(val, recordAt)
	t.dispatcher.RUnlock(i)

	t.logWrite(e)
	return
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

func (t *TDB) logWrite(e error) {
	if e != nil {
		t.logErr.Println(e)
	}
}

// addFile add new part file to bucket
func (t *TDB) addFile(b *tBucket) (e error) {
	access := new(tAccessor)
	bucketName := strconv.FormatInt(int64(b.PartLen), 16)
	fileName := t.path + bucketName + string(os.PathSeparator) + strconv.FormatInt(int64(len(b.files)), 10)
	access.writer, e = os.Create(fileName)
	if e != nil {
		return
	}
	e = access.writer.Truncate(b.MaxIdPerFile * int64(b.PartLen))
	if e != nil {
		return
	}
	access.reader = make([]*os.File, len(b.files[0].reader))

	for i := range access.reader {
		access.reader[i], e = os.OpenFile(fileName, os.O_RDONLY, 0666)
		if e != nil {
			return
		}
	}

	b.files = append(b.files, access)
	return
}

// getNewID get new id from bucket
func (t *TDB) getNewID(bucket *tBucket) (f *os.File, recordAt int64, newkey []byte) {
	newkey = make([]byte, keyLen)
	binary.LittleEndian.PutUint32(newkey, bucket.PartLen)
	emptyid := bucket.emptyid.PopPoint()
	if emptyid != nil {
		f = bucket.files[binary.LittleEndian.Uint16(emptyid[:2])].writer
		recordAt = int64(binary.LittleEndian.Uint32(emptyid[2:])) * int64(bucket.PartLen)
		copy(newkey[4:], emptyid)
	} else {
		if bucket.currentId > bucket.MaxIdPerFile {
			t.addFile(bucket)
			bucket.currentId = 0
		}
		fid := uint16(len(bucket.files) - 1)
		f = bucket.files[fid].writer
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

func (t *TDB) prepareWrite(key, val []byte) (f1 *os.File, f2 *os.File, recordAt1 int64, recordAt2 int64, newkey []byte) {

	bucket, _ := t.h.Buckets.Search(uint32(len(val)))
	if key == nil {
		f1, recordAt1, newkey = t.getNewID(bucket)
		return
	}

	bucketid, fileid, recordAt := keyParse(key)

	// delete
	if val == nil {
		bucket, _ := t.h.Buckets.Search(bucketid)
		if bucketid == bucket.PartLen {
			f2 = bucket.files[fileid].writer
			recordAt2 = recordAt
			bucket.emptyid.Push(key[4:])
		}
		return
	}

	if bucketid == bucket.PartLen {
		f1 = bucket.files[fileid].writer
		recordAt1 = recordAt
		newkey = key
		return
	}

	f1, recordAt1, newkey = t.getNewID(bucket)
	bucket, _ = t.h.Buckets.Search(bucketid)
	if bucketid == bucket.PartLen {
		f2 = bucket.files[fileid].writer
		recordAt2 = recordAt
		bucket.emptyid.Push(key[4:])
	}
	return
}

// Set set value
func (t *TDB) set() {

	var (
		recordAt1, recordAt2 int64
		f1, f2               *os.File
		newkey               []byte
		record               *tSetRecord
		e                    error
		logCount             uint32
	)

	defer func() {
		t.closeWait.Done()
		if e := recover(); e != nil {
			close(record.newkey)
			t.logErr.Printf("error: %s\nat:\n%s", e, debug.Stack())
		}
	}()

	logkey := make([]byte, keyLen+1)
	const maxLogCount = uint32(122016116) // 10Gb

	for record = range t.chWriter {
		f1, f2, recordAt1, recordAt2, newkey = t.prepareWrite(record.key, record.val)

		if f1 != nil {

			logkey[0] = 1
			copy(logkey[1:], newkey)
			_, e = t.logFile.Write(logkey)
			t.logWrite(e)
			t.dispatcher.Lock(string(newkey))
			_, e = f1.WriteAt(record.val, recordAt1)
			t.dispatcher.Unlock()
			t.logWrite(e)

			record.newkey <- newkey
		}

		if f2 != nil {
			logkey[0] = 0
			copy(logkey[1:], record.key)
			_, e = t.logFile.Write(logkey)
			t.logWrite(e)
			t.dispatcher.Lock(string(record.key))
			_, e = f2.WriteAt([]byte{0}, recordAt2)
			t.dispatcher.Unlock()
			t.logWrite(e)
		}

		logCount++
		if logCount > maxLogCount {
			e = t.flashLog()
			t.logWrite(e)
			logCount = 0
		}
	}
}
