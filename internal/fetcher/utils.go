package fetcher

import (
	"bufio"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/opesun/goquery"
)

func get(link *url.URL) ([]byte, time.Duration, error) {
	start := time.Now()

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(link.String())
	if err != nil {
		return nil, time.Duration(-1), err
	}
	ttr := time.Since(start)
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	data, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return nil, time.Duration(-1), err
	}
	return data, ttr, nil
}

type PageInfo struct {
	title string
	body  []byte
	links []*url.URL
}

func parsePage(input []byte) (*PageInfo, error) {
	x, err := goquery.ParseString(string(input))
	if err != nil {
		return nil, err
	}

	return &PageInfo{
		body:  []byte(x.Text()),
		title: parseTitle(x),
		links: parseLinks(x),
	}, nil
}

func parseLinks(x goquery.Nodes) (links []*url.URL) {
	for _, href := range x.Find("a").Attrs("href") {
		link, err := url.Parse(href)
		if err != nil {
			continue
		}

		if link.Scheme == "" && !strings.HasPrefix(link.String(), "/") {
			continue
		}

		links = append(links, link)
	}
	return
}

func parseTitle(x goquery.Nodes) string {
	return x.Find("head title").Text()
}

func normalize(prev *url.URL, input *url.URL) *url.URL {
	if input.Host == "" {
		input.Host = prev.Host
	}
	if input.Scheme != prev.Scheme {
		input.Scheme = prev.Scheme
	}

	parts := strings.Split(input.Host, ".")

	if parts[0] == "www" {
		parts = parts[1:]
	}

	input.Host = strings.Join(parts, ".")
	return input
}
