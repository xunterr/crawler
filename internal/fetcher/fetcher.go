package fetcher

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/dnscache"
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
	resolver := &dnscache.Resolver{}

	go refreshDNSCacheLoop(resolver)

	return &DefaultFetcher{
		timeout: timeout,
		client: http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return dialContext(ctx, resolver, network, addr)
				},
			},
		},
	}
}

func refreshDNSCacheLoop(resolver *dnscache.Resolver) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for range t.C {
		resolver.Refresh(true)
	}
}

func dialContext(ctx context.Context, res *dnscache.Resolver, network string, addr string) (conn net.Conn, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := res.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		var dialer net.Dialer
		conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			break
		}
	}
	return
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
