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

func get(link url.URL) ([]byte, time.Duration, error) {
	start := time.Now()
	resp, err := http.Get(link.String())
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

func parseLinks(input []byte) (links []url.URL) {
	x, err := goquery.ParseString(string(input))
	if err == nil {
		for _, href := range x.Find("a").Attrs("href") {
			link, err := url.Parse(href)
			if err != nil {
				continue
			}

			if link.Scheme == "" && !strings.HasPrefix(link.String(), "/") {
				continue
			}

			links = append(links, *link)
		}
	}
	return
}

func normalize(prev url.URL, input url.URL) url.URL {
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
