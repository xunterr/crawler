package warc

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/slyrz/warc"
)

type WarcWriter struct {
	path        string
	currentWarc *os.File
	maxSize     int64
}

func (w *WarcWriter) Write(record *warc.Record) error {
	stat, err := w.currentWarc.Stat()
	if err != nil {
		return err
	}

	if w.currentWarc == nil || stat.Size() >= w.maxSize {
		err := w.newWarc()
		if err != nil {
			return err
		}
	}

	return writeGzip(record, w.currentWarc)
}

func (w *WarcWriter) newFile() (*os.File, error) {
	name := fmt.Sprintf("%s.warc.gz", time.Now().Format("2006-01-02T15:04:05Z"))
	file, err := os.Create(filepath.Join(w.path, name))
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (w *WarcWriter) warcInfo() (*warc.Record, error) {
	return WarcinfoRecord(make(map[string]string))
}

func (w *WarcWriter) newWarc() error {
	file, err := w.newFile()
	if err != nil {
		return err
	}

	warcinfo, err := w.warcInfo()
	if err != nil {
		return err
	}

	err = writeGzip(warcinfo, file)
	if err != nil {
		return err
	}
	w.currentWarc = file
	return nil
}

func writeGzip(record *warc.Record, file *os.File) error {
	gz := gzip.NewWriter(file)
	warcWriter := warc.NewWriter(gz)
	_, err := warcWriter.WriteRecord(record)
	return err
}
