package main

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jimsmart/grobotstxt"
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

var ErrCrawlForbidden error = errors.New("Crawl forbidden")

func (w *Worker) runN(ctx context.Context, wg *sync.WaitGroup, n int) {
	for i := 0; i < n; i++ {
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
			can, err := w.canCrawl(r.u)
			if err == nil && !can {
				err = ErrCrawlForbidden
			}

			if err != nil {
				w.out <- result{
					err: err,
					url: r.u,
				}
				break
			}

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
			err: err,
			url: res.u,
		}
	}

	pageInfo, err := parser.ParsePage(details.Body)
	if err != nil {
		return result{
			err: err,
			url: res.u,
			ttr: details.TTR,
		}
	}

	w.mu.Lock()
	err = writeWarc(w.warcWriter, res.u, details)
	w.mu.Unlock()

	return result{
		err:   err,
		url:   res.u,
		ttr:   details.TTR,
		links: pageInfo.Links,
	}
}

func (w *Worker) canCrawl(res *url.URL) (bool, error) {
	robotsUrl := *res
	robotsUrl.Path = "/robots.txt"
	robotsUrl.RawQuery = ""
	robotsUrl.Fragment = ""

	details, err := w.fetcher.Fetch(&robotsUrl)
	if err != nil {
		return false, err
	}

	ok := grobotstxt.AgentAllowed(string(details.Body), "GoBot/1.0", res.String())
	return ok, nil
}

func writeWarc(writer *warc.WarcWriter, url *url.URL, details *fetcher.FetchDetails) error {
	respRecord, err := warc.ResourceRecord(details.Body, url.String(), "application/http")
	if err != nil {
		return err
	}

	metadata := make(map[string]string)
	metadata["fetchTimeMs"] = strconv.Itoa(int(details.TTR.Milliseconds()))
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
