package fetcher

import (
	"bufio"
	"io"
	"net/http"
	"net/url"
	"time"
)

type FetchDetails struct {
	Body []byte
	TTR  time.Duration
}

type Fetcher interface {
	Fetch(*url.URL) (*FetchDetails, error)
}

type DefaultFetcher struct {
	timeout time.Duration
	client  http.Client
}

func NewDefaultFetcher(timeout time.Duration) *DefaultFetcher {
	return &DefaultFetcher{
		timeout: timeout,
		client: http.Client{
			Timeout: timeout,
		},
	}
}

func (df *DefaultFetcher) Fetch(url *url.URL) (*FetchDetails, error) {
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	df.setHeaders(req)

	start := time.Now()
	resp, err := df.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	ttr := time.Since(start)

	bytes, err := readPage(resp)
	if err != nil {
		return nil, err
	}

	return &FetchDetails{
		Body: bytes,
		TTR:  ttr,
	}, nil
}

func (df *DefaultFetcher) setHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0")
}

func readPage(resp *http.Response) ([]byte, error) {
	reader := bufio.NewReader(resp.Body)
	data, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}
