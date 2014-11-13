// (c) 2014 Cergoo
// under terms of ISC license

package fixedDBv2

import (
	"encoding/binary"
	"fmt"
	"github.com/Cergoo/gol/err"
	"github.com/Cergoo/gol/stack/bytestack"
	"github.com/Cergoo/gol/util"
	"os"
)

type (
	tBuckets []*tBucket

	tBucket struct {
		MaxIdPerFile int64             //
		currentId    int64             //
		PartLen      uint32            // bucket parts len in bytes
		files        []*tAccessor      //
		emptyid      *bytestack.TStack // empty id
		emptyFile    *os.File          //
	}

	// accessor to each file
	tAccessor struct {
		writer *os.File
		reader []*os.File
	}
)

func (t tBuckets) Len() int           { return len(t) }
func (t tBuckets) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t tBuckets) Less(i, j int) bool { return t[i].PartLen < t[j].PartLen }

func (t tBuckets) Search(key uint32) (*tBucket, error) {
	var i int
	for i = range t {
		if key <= t[i].PartLen {
			return t[i], nil
		}
	}
	return nil, err.New(fmt.Sprintf(errMaxRecordLen, t[i].PartLen, key), ErrMaxRecordLen)
}

// setEmpty scan all data base files and find empty slots
func (t *tBucket) setEmpty() (e error) {
	var (
		f      *os.File
		enable bool = true
	)
	b := make([]byte, 4)
	lastFile := len(t.files) - 1
	// range files of bucket
	for fid := 0; fid <= lastFile; fid++ {
		f = t.files[fid].reader[0]
		for n := t.MaxIdPerFile; n > 0; n-- {
			f.ReadAt(b, n*int64(t.PartLen))
			if util.Zero(b) {
				if (n == int64(lastFile)) && enable {
					t.currentId = n
					continue
				}
				emptyid := make([]byte, emptyidLen)
				binary.LittleEndian.PutUint16(emptyid, uint16(fid))
				binary.LittleEndian.PutUint32(emptyid[2:], uint32(n))
				t.emptyid.Push(emptyid)
			}
			enable = false
		}
	}
	return
}
