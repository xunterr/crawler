package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	warcparser "github.com/slyrz/warc"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/parser"
	"github.com/xunterr/crawler/internal/warc"
)

type resource struct {
	u  *url.URL
	at time.Time
}

type result struct {
	err   error
	url   *url.URL
	ttr   time.Duration
	links []*url.URL
}

type Worker struct {
	fetcher fetcher.Fetcher

	in  chan resource
	out chan result

	warcWriter *warc.WarcWriter
	mu         sync.Mutex
}

func (w *Worker) runN(ctx context.Context, wg *sync.WaitGroup, n int) {
	for range n {
		wg.Add(1)
		go func() {
			w.run(ctx)
			wg.Done()
		}()
	}
}

func (w *Worker) run(ctx context.Context) {
	for {
		select {
		case r := <-w.in:
			w.out <- w.waitAndProcess(ctx, r)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) waitAndProcess(ctx context.Context, res resource) result {
	var timer <-chan time.Time
	if time.Until(res.at).Seconds() > 10 {
		timer = time.After(10 * time.Second)
	} else {
		timer = time.After(time.Until(res.at))
	}

	select {
	case <-timer:
		return w.process(ctx, res)
	case <-ctx.Done():
		return result{
			err: errors.New("Canceled"),
		}
	}
}

func (w *Worker) process(ctx context.Context, res resource) result {
	details, err := w.fetcher.Fetch(res.u)
	if err != nil {
		return result{
			err:   err,
			url:   res.u,
			ttr:   time.Duration(0),
			links: []*url.URL{},
		}
	}

	bytes, err := readPage(details.Response)
	if err != nil {
		return result{
			err:   err,
			url:   res.u,
			ttr:   details.TTR,
			links: []*url.URL{},
		}
	}

	pageInfo, err := parser.ParsePage(bytes)
	if err != nil {
		return result{
			err:   err,
			url:   res.u,
			ttr:   details.TTR,
			links: []*url.URL{},
		}
	}

	w.mu.Lock()
	err = writeWarc(w.warcWriter, res.u, bytes, details.TTR)
	w.mu.Unlock()

	return result{
		err:   err,
		url:   res.u,
		ttr:   details.TTR,
		links: pageInfo.Links,
	}
}

func readPage(resp *http.Response) ([]byte, error) {
	reader := bufio.NewReader(resp.Body)
	data, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil

}

func writeWarc(writer *warc.WarcWriter, url *url.URL, data []byte, ttr time.Duration) error {
	respRecord, err := warc.ResourceRecord(data, url.String(), "application/http")
	if err != nil {
		return err
	}

	metadata := make(map[string]string)
	metadata["fetchTimeMs"] = strconv.Itoa(int(ttr.Milliseconds()))
	metadataRecord, err := warc.MetadataRecord(metadata, url.String())
	if err != nil {
		return err
	}

	warc.Capture(respRecord, []*warcparser.Record{metadataRecord})
	err = writer.Write(respRecord)
	if err != nil {
		return err
	}

	err = writer.Write(metadataRecord)
	if err != nil {
		return err
	}
	return nil
}
