package fetcher

import (
	"context"
	"net/url"
	"time"

	pb "github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
)

type Response struct {
	Url   *url.URL
	TTR   time.Duration
	Links []*url.URL
}

type Fetcher interface {
	Fetch(*url.URL) (Response, error)
}

type DefaultFetcher struct {
	client pb.IndexerClient
	logger *zap.SugaredLogger
}

func NewDefaultFetcher(logger *zap.Logger, client pb.IndexerClient) *DefaultFetcher {
	return &DefaultFetcher{client: client, logger: logger.Sugar()}
}

func (df *DefaultFetcher) Fetch(page *url.URL) (Response, error) {
	data, ttr, err := get(page)
	if err != nil {
		return Response{Url: page, Links: nil, TTR: ttr}, err
	}

	pageInfo, err := parsePage(data)
	if err != nil {
		return Response{Url: page, Links: nil, TTR: ttr}, err
	}

	var linksNormalized []*url.URL
	for _, e := range pageInfo.links {
		linksNormalized = append(linksNormalized, normalize(page, e))
	}

	go func() {
		isOk, err := df.sendIndexRequest(page.String(), pageInfo.title, pageInfo.body)
		if err != nil {
			df.logger.Errorw("Indexer returned error on sendIndexRequest",
				"error", err.Error())
			return
		}

		if !isOk {
			df.logger.Warnln("Indexer returned negative result on sendIndexRequest")
			return
		}
	}()

	return Response{
		Url:   page,
		Links: linksNormalized,
		TTR:   ttr,
	}, nil
}

func (df *DefaultFetcher) sendIndexRequest(url string, title string, body []byte) (bool, error) {
	res, err := df.client.Index(context.Background(), &pb.IndexRequest{
		Document: &pb.Document{
			Title: title,
			Body:  body,
			Url:   url,
		},
	})

	if err != nil {
		return false, err
	}

	return res.IsOk, nil
}
