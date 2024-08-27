package fetcher

import (
	"net/url"
	"time"
)

type Response struct {
	TTR   time.Duration
	Links []url.URL
}

type Fetcher interface {
	Fetch(url.URL) (Response, error)
}

type DefaultFetcher struct{}

func (df *DefaultFetcher) Fetch(page url.URL) (Response, error) {
	data, ttr, err := get(page)
	if err != nil {
		return Response{}, err
	}

	links := parseLinks(data)
	var linksNormalized []url.URL

	for _, e := range links {
		linksNormalized = append(linksNormalized, normalize(page, e))
	}

	return Response{
		Links: linksNormalized,
		TTR:   ttr,
	}, nil
}
