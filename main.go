package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/paulbellamy/ratecounter"
	boom "github.com/tylertreat/BoomFilters"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/frontier"
	p2p "github.com/xunterr/crawler/internal/net"
	"github.com/xunterr/crawler/internal/storage/rocksdb"
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

	db, err := openRocksDB("data/bloom/")
	if err != nil {
		panic(err.Error())
	}

	storage := rocksdb.NewRocksdbStorage[*boom.ScalableBloomFilter](db, encode, decode)
	return frontier.NewBfFrontier(qp, storage)
}

func encode(bloom *boom.ScalableBloomFilter) ([]byte, error) {
	var bytes bytes.Buffer
	writer := io.Writer(&bytes)
	bloom.WriteTo(writer)
	return bytes.Bytes(), nil
}

func decode(data []byte) (*boom.ScalableBloomFilter, error) {
	bloom := boom.NewDefaultScalableBloomFilter(0.01)
	buf := bytes.NewReader(data)
	_, err := bloom.ReadFrom(buf)

	if err != nil {
		return nil, err
	}
	return bloom, err
}

func openRocksDB(path string) (*grocksdb.DB, error) {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	return grocksdb.OpenDb(opts, path)
}

func loop(logger *zap.SugaredLogger, frontier frontier.Frontier, fet fetcher.Fetcher) {
	var wg sync.WaitGroup
	counter := ratecounter.NewRateCounter(1 * time.Second)
	var total uint32

	urls := make(chan resource, 128)
	processed := make(chan fetcher.Response, 16)

	go func() {
		t := time.Tick(5 * time.Second)
		for range t {
			logger.Infof("Ops/sec: %d; Total: %d", counter.Rate(), total)
		}
	}()

	go func() {
		for r := range processed {
			frontier.MarkProcessed(r.Url, r.TTR)
			counter.Incr(1)
			total++
			for _, u := range r.Links {
				err := frontier.Put(u)
				if err != nil {
					logger.Errorln(err.Error())
				}
			}
		}
	}()

	for i := 0; i < 128; i++ {
		wg.Add(1)
		go worker(&wg, fet, urls, processed)
	}
	for {
		url, accessAt, err := frontier.Get()
		if err != nil {
			log.Println(err)
			continue
		}
		urls <- resource{
			u:  url,
			at: accessAt,
		}
	}
}

type resource struct {
	u  *url.URL
	at time.Time
}

type response struct {
	url  *url.URL
	info *fetcher.PageInfo
}

func worker(wg *sync.WaitGroup, fetcher fetcher.Fetcher, urls chan resource, processed chan fetcher.Response) {
	for u := range urls {
		time.Sleep(time.Until(u.at))

		resp, err := fetcher.Fetch(u.u)
		if err != nil {
			continue
		}

		processed <- resp
	}

	wg.Done()
}
