package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/frontier"
	p2p "github.com/xunterr/crawler/internal/net"
	"github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr          string = "127.0.0.1:6969"
	bootstrapNode string = ""
	indexer       string = "localhost:8080"
	seed          string = ""
	distributed   bool   = false
)

func init() {
	flag.StringVar(&addr, "addr", addr, "defines node address")
	flag.StringVar(&bootstrapNode, "node", "", "node to bootstrap with")
	flag.StringVar(&seed, "u", "", "seed list path")
	flag.StringVar(&indexer, "i", "", "indexer address")
	flag.BoolVar(&distributed, "d", distributed, "distributed vs local deployment")
}

func initLogger(level zapcore.Level) *zap.Logger {
	conf := zap.NewProductionEncoderConfig()
	conf.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewConsoleEncoder(conf)
	core := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
	l := zap.New(core)
	return l
}

func readSeed(path string) ([]*url.URL, error) {
	dat, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(dat)
	urls := []*url.URL{}

	for scanner.Scan() {
		url, err := url.Parse(scanner.Text())
		if err != nil {
			return urls, err
		}

		urls = append(urls, url)
	}
	return urls, nil
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	flag.Parse()

	defaultLogger := initLogger(zapcore.InfoLevel)
	defer defaultLogger.Sync()
	logger := defaultLogger.Sugar()

	var frontier frontier.Frontier

	if distributed {
		frontier = makeDistributedFrontier(logger)
	} else {
		frontier = makeFrontier()
	}

	urls, err := readSeed(seed)
	if err != nil {
		logger.Errorf("Can't parse URL: %s", err.Error())
	}

	for _, u := range urls {
		err = frontier.Put(u)
		if err != nil {
			logger.Errorln(err)
		}
	}

	conn, err := grpc.NewClient(indexer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Panicf(err.Error())
	}
	defer conn.Close()

	client := proto.NewIndexerClient(conn)

	fetcher := fetcher.NewDefaultFetcher(defaultLogger, client)
	loop(logger, frontier, fetcher)

	wg.Wait()
}

func makeDistributedFrontier(logger *zap.SugaredLogger) frontier.Frontier {
	peer := p2p.NewPeer(logger.Desugar())

	go peer.Listen(context.Background(), addr)

	bfFrontier := makeFrontier()

	dispatcherConf := frontier.DistributedFrontierConf{
		Addr:              addr,
		BatchPeriodMs:     40_000,
		CheckKeysPeriodMs: 30_000,
	}

	distributedFrontier, err := frontier.NewDistributed(logger.Desugar(), peer, bfFrontier.(*frontier.BfFrontier), dispatcherConf)
	if err != nil {
		logger.Fatalln("Failed to init dispatcher: %s", err.Error())
		return nil
	}

	fmt.Printf("Node to bootstrap from: %s\n", bootstrapNode)
	if bootstrapNode != "" {
		err := distributedFrontier.Bootstrap(bootstrapNode)
		if err != nil {
			logger.Infof("Failed to bootstrap from node %s: %s", bootstrapNode, err.Error())
		}
	}

	return distributedFrontier
}

func makeFrontier() frontier.Frontier {
	qp := frontier.InMemoryQueueProvider{}
	return frontier.NewBfFrontier(qp)
}

func loop(logger *zap.SugaredLogger, frontier frontier.Frontier, fetcher fetcher.Fetcher) {
	counter := ratecounter.NewRateCounter(1 * time.Second)
	var total atomic.Uint64

	go func() {
		t := time.Tick(5 * time.Second)
		for range t {
			logger.Infof("Ops/sec: %d; Total: %d", counter.Rate(), total.Load())
		}
	}()

	for {
		url, accessAt, err := frontier.Get()
		if err != nil {
			log.Println(err)
			return
		}

		go func() {
			time.Sleep(time.Until(accessAt))

			resp, err := fetcher.Fetch(url)
			if err != nil {
				return
			}

			frontier.MarkProcessed(url, resp.TTR)

			for _, e := range resp.Links {
				err := frontier.Put(e)
				if err != nil {
					logger.Errorln(err.Error())
				}
			}

			counter.Incr(1)
			total.Add(1)
		}()
	}
}
