package main

import (
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
	"github.com/xunterr/crawler/internal/dispatcher"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/frontier"
	p2p "github.com/xunterr/crawler/internal/net"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	addr          string = "127.0.0.1:6969"
	bootstrapNode string = ""
	seed          string = ""
)

func init() {
	flag.StringVar(&addr, "addr", addr, "defines node address")
	flag.StringVar(&bootstrapNode, "node", "", "node to bootstrap with")
	flag.StringVar(&seed, "u", "", "seed url")
}

func initLogger(level zapcore.Level) *zap.Logger {
	conf := zap.NewProductionEncoderConfig()
	conf.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewConsoleEncoder(conf)
	core := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
	l := zap.New(core)
	return l
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	flag.Parse()

	defaultLogger := initLogger(zapcore.InfoLevel)
	defer defaultLogger.Sync()
	logger := defaultLogger.Sugar()

	seedUrl, err := url.Parse(seed)
	if err != nil {
		logger.Fatalln("Can't parse URL'")
		return
	}

	router := p2p.NewRouter()
	peer := p2p.NewPeer(defaultLogger, router)

	go peer.Listen(context.Background(), addr)

	qp := frontier.InMemoryQueueProvider{}
	frontier := frontier.NewBfFrontier(qp)

	dispatcherConf := dispatcher.DispatcherConfig{
		Addr:          addr,
		BatchPeriodMs: 30_000,
	}

	dispatcher, err := dispatcher.NewDispatcher(defaultLogger, peer, router, frontier.Put, dispatcherConf)
	if err != nil {
		logger.Infof("Failed to init dispatcher: %s", err.Error())
		return
	}

	fmt.Printf("Node to bootstrap from: %s\n", bootstrapNode)
	if bootstrapNode != "" {
		err := dispatcher.Bootstrap(bootstrapNode)
		if err != nil {
			logger.Infof("Failed to bootstrap from node %s", bootstrapNode)
		}
	}

	if seed != "" {
		err = dispatcher.Dispatch(seedUrl)
		if err != nil {
			logger.Fatalln(err)
			return
		}
	}

	fetcher := fetcher.DefaultFetcher{}
	loop(logger, dispatcher, frontier, &fetcher)

	wg.Wait()
}

func loop(logger *zap.SugaredLogger, dispatcher *dispatcher.Dispatcher, frontier *frontier.BfFrontier, fetcher fetcher.Fetcher) {
	counter := ratecounter.NewRateCounter(1 * time.Second)
	var total atomic.Uint64

	go func() {
		t := time.Tick(5 * time.Second)
		for range t {
			logger.Infof("Ops/sec: %d; Total: %d", counter.Rate(), total.Load())
		}
	}()

	for {
		time.Sleep(50 * time.Millisecond)
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

			frontier.Processed(url, resp.TTR)

			for _, e := range resp.Links {
				dispatcher.Dispatch(e)
			}

			counter.Incr(1)
			total.Add(1)
		}()
	}
}
