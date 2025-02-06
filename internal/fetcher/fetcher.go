package fetcher

import (
	"net/http"
	"net/url"
	"time"

	pb "github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
)

type FetchDetails struct {
	Response *http.Response
	TTR      time.Duration
}

type Fetcher interface {
	Fetch(*url.URL) (FetchDetails, error)
}

type DefaultFetcher struct {
	client pb.IndexerClient
	logger *zap.SugaredLogger
}

func NewDefaultFetcher(logger *zap.Logger, client pb.IndexerClient) *DefaultFetcher {
	return &DefaultFetcher{client: client, logger: logger.Sugar()}
}

func (df *DefaultFetcher) Fetch(url *url.URL) (FetchDetails, error) {
	start := time.Now()

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(url.String())
	if err != nil {
		return FetchDetails{}, err
	}
	ttr := time.Since(start)
	return FetchDetails{
		Response: resp,
		TTR:      ttr,
	}, nil
}
