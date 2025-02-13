package parser

import (
	"net/url"
	"strings"

	"github.com/opesun/goquery"
)

type PageInfo struct {
	Body  []byte
	Title string
	Links []*url.URL
}

func ParsePage(url *url.URL, input []byte) (*PageInfo, error) {
	x, err := goquery.ParseString(string(input))
	if err != nil {
		return nil, err
	}

	return &PageInfo{
		Body:  []byte(x.Text()),
		Title: parseTitle(x),
		Links: parseLinks(url, x),
	}, nil
}

func parseLinks(base *url.URL, x goquery.Nodes) (links []*url.URL) {
	for _, href := range x.Find("a").Attrs("href") {
		link, err := base.Parse(href)
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
