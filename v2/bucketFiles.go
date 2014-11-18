// (c) 2014 Cergoo
// under terms of ISC license

package fixedDBv2

import (
	"errors"
	"io/ioutil"
	"os"
	"strconv"
)

type (
	tFileTuple struct {
		valstr string
		valint int64
	}

	tBuketFiles struct {
		files []tFileTuple
		max   int64
	}
)

func parseBuketFiles(path string) (bucketFiles *tBuketFiles, e error) {
	var dir []os.FileInfo

	dir, e = ioutil.ReadDir(path)
	if e != nil {
		return
	}

	if len(dir) == 0 {
		e = errors.New("Not database file: " + path)
		return
	}

	bucketFiles = &tBuketFiles{files: make([]tFileTuple, len(dir))}
	for i := range dir {
		bucketFiles.files[i].valstr = dir[i].Name()
		bucketFiles.files[i].valint, e = strconv.ParseInt(bucketFiles.files[i].valstr, 10, 32)
		if e != nil {
			e = errors.New("It's not database file: " + path + "/" + bucketFiles.files[i].valstr)
			return
		}
		if bucketFiles.max < bucketFiles.files[i].valint {
			bucketFiles.max = bucketFiles.files[i].valint
		}
	}

	return
}
