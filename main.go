package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/paulbellamy/ratecounter"
	boom "github.com/tylertreat/BoomFilters"
	"github.com/xunterr/crawler/internal/dht"
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

	defaultLogger := initLogger(zapcore.InfoLevel)
	defer defaultLogger.Sync()
	logger := defaultLogger.Sugar()

	conf, err := ReadConf()
	if err != nil {
		logger.Fatalln(err)
	}

	go func() {
		logger.Fatalln(http.ListenAndServe(":8081", nil))
	}()

	var frontier frontier.Frontier

	if conf.Distributed.Addr != "" {
		frontier = makeDistributedFrontier(logger, conf.Distributed)
	} else {
		frontier = makeFrontier()
	}

	urls, err := readSeed(conf.Frontier.Seed)
	if err != nil {
		logger.Errorf("Can't parse URL: %s", err.Error())
	}

	for _, u := range urls {
		err = frontier.Put(u)
		if err != nil {
			logger.Errorln(err)
		}
	}

	println(conf.Fetcher.Indexer)
	conn, err := grpc.NewClient(conf.Fetcher.Indexer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Panicf(err.Error())
	}
	defer conn.Close()

	client := proto.NewIndexerClient(conn)

	fetcher := fetcher.NewDefaultFetcher(defaultLogger, client)
	loop(logger, frontier, fetcher)

	wg.Wait()
}

func makeDistributedFrontier(logger *zap.SugaredLogger, conf DistributedConf) frontier.Frontier {
	peer := p2p.NewPeer(logger.Desugar())

	go peer.Listen(context.Background(), conf.Addr)

	bfFrontier := makeFrontier()

	dispatcherConf := frontier.DistributedFrontierConf{
		Addr:              conf.Addr,
		BatchPeriodMs:     conf.BatchPeriodMs,     //40_000
		CheckKeysPeriodMs: conf.CheckKeysPeriodMs, //30_000
	}

	dht, err := makeDHT(logger.Desugar(), peer, conf.Addr, conf.Dht)
	if err != nil {
		logger.Fatalln(err)
		return nil
	}

	distributedFrontier, err := frontier.NewDistributed(logger.Desugar(), peer, bfFrontier.(*frontier.BfFrontier), dht, dispatcherConf)
	if err != nil {
		logger.Fatalln("Failed to init dispatcher: %s", err.Error())
		return nil
	}

	bootstrap(logger, conf.Addr, distributedFrontier)

	return distributedFrontier
}

func makeDHT(logger *zap.Logger, peer *p2p.Peer, addr string, conf DhtConf) (*dht.DHT, error) {
	dhtConf := dht.DhtConfig{
		Addr:               addr,
		SuccListLength:     conf.SuccListLength,     //2
		StabilizeInterval:  conf.StabilizeInterval,  //10_000
		FixFingersInterval: conf.FixFingersInterval, //15_000
		VnodeNum:           conf.VnodeNum,           //32
	}

	table, err := dht.NewDHT(logger, peer, dhtConf)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func bootstrap(logger *zap.SugaredLogger, addr string, distributedFrontier *frontier.DistributedFrontier) {
	fmt.Printf("Node to bootstrap from: %s\n", addr)
	if addr != "" {
		err := distributedFrontier.Bootstrap(addr)
		if err != nil {
			logger.Infof("Failed to bootstrap from node %s: %s", addr, err.Error())
		}
	}
}

func makeFrontier(conf FrontierConf) frontier.Frontier {
	qp := frontier.InMemoryQueueProvider{}

	db, err := openRocksDB("data/bloom/")
	if err != nil {
		panic(err.Error())
	}

	storage := rocksdb.NewRocksdbStorage[*boom.ScalableBloomFilter](db, encode, decode)
	//storage := inmem.NewInMemoryStorage[*boom.ScalableBloomFilter]()
	return frontier.NewBfFrontier(qp, storage, frontier.BfFrontierConfig{
		MaxActiveQueues:      conf.MaxActiveQueues,
		PolitenessMultiplier: conf.Politeness,
		DefaultSessionBudget: conf.DefaultSessionBudget,
	})
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
			logger.Infof("Len of urls buffer: %d, len of processed buffer: %d", len(urls), len(processed))
		}
	}()

	go func() {
		for r := range processed {
			counter.Incr(1)
			total++
			for _, u := range r.Links {
				err := frontier.Put(u)
				if err != nil {
					logger.Errorln(err.Error())
				}
			}
			frontier.MarkProcessed(r.Url, r.TTR)
		}
	}()

	for i := 0; i < 256; i++ {
		wg.Add(1)
		go worker(&wg, fet, urls, processed)
	}
	for {
		url, accessAt, err := frontier.Get()
		if err != nil {
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
		if time.Until(u.at).Seconds() > 10 {
			time.Sleep(10 * time.Second)
		} else {
			time.Sleep(time.Until(u.at))
		}

		resp, err := fetcher.Fetch(u.u)
		processed <- resp
		if err != nil {
			continue
		}
	}

	wg.Done()
}
