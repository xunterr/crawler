package warc

import (
	"bufio"
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
	buff            *bytes.Buffer
	path            string
	currFile        string
	maxFileSize     int64
	maxBuffSize     int64
	bytesSinceFlush int64
}

func NewWarcWriter(path string) *WarcWriter {
	ww := &WarcWriter{
		buff:            bytes.NewBuffer(make([]byte, 0)),
		path:            path,
		maxFileSize:     1 * int64(math.Pow(10, 9)),
		maxBuffSize:     100 * int64(math.Pow(10, 6)),
		bytesSinceFlush: 0,
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
		return w.dumpToFile()
	}
	return nil
}

func (w *WarcWriter) dumpToFile() error {
	file, err := w.getFile()
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(file)

	n, err := io.Copy(buf, w.buff)
	if err != nil {
		return err
	}

	w.bytesSinceFlush += n
	buf.Flush()

	if w.bytesSinceFlush >= w.maxFileSize {
		file.Seek(0, 0)

		go func(filename string) {
			defer file.Close()
			if err := w.zip(filename, file); err != nil { //todo: improve error handling
				return
			}
			if err := os.Remove(filename); err != nil {
				return
			}
		}(w.currFile)

		if err := w.reset(); err != nil {
			return err
		}
	} else {
		file.Close()
	}
	return nil
}

func (w *WarcWriter) getFile() (*os.File, error) {
	var file *os.File
	if w.currFile == "" {
		var err error
		file, err = w.newFile()
		if err != nil {
			return nil, err
		}
		w.currFile = file.Name()
		return file, err
	} else {
		return w.open(w.currFile)
	}
}

func (w *WarcWriter) zip(filename string, r io.Reader) error {
	zipped, err := w.open(fmt.Sprintf("%s.gz", filename))
	if err != nil {
		return err
	}
	defer zipped.Close()
	return writeGzip(r, zipped)
}

func (w *WarcWriter) reset() error {
	w.currFile = ""
	w.bytesSinceFlush = 0
	return w.initNewBuff()
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
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
}

func (w *WarcWriter) newFile() (*os.File, error) {
	name := fmt.Sprintf("%s.warc", time.Now().Format("2006-01-02T15:04:05Z"))
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
