package warc

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/slyrz/warc"
)

type WarcWriter struct {
	buff        *bytes.Buffer
	path        string
	currFile    string
	maxFileSize int64
	maxBuffSize int64
}

func NewWarcWriter(path string) *WarcWriter {
	ww := &WarcWriter{
		buff:        bytes.NewBuffer(make([]byte, 0)),
		path:        path,
		maxFileSize: 5 * int64(math.Pow(10, 6)),
		maxBuffSize: int64(math.Pow(10, 6)),
	}

	ww.initNewBuff()
	return ww
}

func (w *WarcWriter) Write(record *warc.Record) error {
	writer := warc.NewWriter(w.buff)
	_, err := writer.WriteRecord(record)
	if err != nil {
		return err
	}

	if w.buff.Len() >= int(w.maxBuffSize) {
		err := w.dumpToFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WarcWriter) dumpToFile() error {
	var file *os.File
	if w.currFile == "" {
		var err error
		file, err = w.newFile()
		if err != nil {
			return err
		}
		w.currFile = file.Name()
	} else {
		var err error
		file, err = w.open(w.currFile)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	err := writeGzip(w.buff, file)
	if err != nil {
		return err
	}

	stats, err := file.Stat()
	if err != nil {
		return err
	}
	if stats.Size() >= w.maxFileSize {
		w.currFile = ""
		err = w.initNewBuff()
		if err != nil {
			return err
		}
	}
	return nil
}

func truncate(file *os.File) error {
	err := file.Truncate(0)
	if err != nil {
		return err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}
	return nil
}

func (w *WarcWriter) open(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0644)
}

func (w *WarcWriter) newFile() (*os.File, error) {
	name := fmt.Sprintf("%s.warc.gz", time.Now().Format("2006-01-02T15:04:05Z"))
	file, err := os.Create(filepath.Join(w.path, name))
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (w *WarcWriter) initNewBuff() error {
	w.buff.Reset()
	warcInfo, err := w.warcInfo()
	if err != nil {
		return err
	}

	writer := warc.NewWriter(w.buff)
	_, err = writer.WriteRecord(warcInfo)
	return err
}

func (w *WarcWriter) warcInfo() (*warc.Record, error) {
	return WarcinfoRecord(make(map[string]string))
}

func writeGzip(data io.Reader, file *os.File) error {
	gz := gzip.NewWriter(file)
	_, err := io.Copy(gz, data)
	defer gz.Close()
	return err
}
