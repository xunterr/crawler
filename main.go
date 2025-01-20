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
	"github.com/xunterr/crawler/internal/storage"
	"github.com/xunterr/crawler/internal/storage/rocksdb"
	"github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type persistentQp struct {
	db              *grocksdb.DB
	queueStorage    *rocksdb.RocksdbStorage[frontier.Url]
	metadataStorage *rocksdb.RocksdbStorage[string]
}

func newPersistentQp(path string) (*persistentQp, error) {
	db, cfs, err := createDefaultDBWithCF(path, []string{"metadata", "data"})
	if err != nil {
		return nil, err
	}

	metadataCF := cfs[0]
	dataCF := cfs[1]

	metadataStorage := rocksdb.NewRocksdbStorage[string](db, rocksdb.WithCF(metadataCF))
	queueStorage := rocksdb.NewRocksdbStorage[frontier.Url](db, rocksdb.WithCF(dataCF))
	return &persistentQp{
		db:              db,
		queueStorage:    queueStorage,
		metadataStorage: metadataStorage,
	}, nil
}

func createDefaultDBWithCF(path string, cfs []string) (*grocksdb.DB, grocksdb.ColumnFamilyHandles, error) {
	cfs = append(cfs, "default")
	var opts []*grocksdb.Options
	for _ = range cfs {
		opts = append(opts, grocksdb.NewDefaultOptions())
	}
	return grocksdb.OpenDbColumnFamilies(getDbOpts(), path, cfs, opts)
}

func (qp *persistentQp) Get(id string) (storage.Queue[frontier.Url], error) {
	err := qp.metadataStorage.Put(id, "")
	if err != nil {
		return nil, err
	}
	return rocksdb.NewRocksdbQueue(qp.queueStorage, []byte(id)), nil
}

func (qp *persistentQp) GetAll() (map[string]storage.Queue[frontier.Url], error) {
	queueMap := make(map[string]storage.Queue[frontier.Url])
	metadata, err := qp.metadataStorage.GetAll()
	if err != nil {
		return nil, err
	}

	for k, _ := range metadata {
		queue := rocksdb.NewRocksdbQueue(qp.queueStorage, []byte(k))
		queueMap[k] = queue
	}
	return queueMap, nil
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
		frontier = makeDistributedFrontier(logger, makeFrontier(conf.Frontier), conf.Distributed)
	} else {
		frontier = makeFrontier(conf.Frontier)
	}

	println(conf.Frontier.Seed)
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

func makeDistributedFrontier(logger *zap.SugaredLogger, bfFrontier *frontier.BfFrontier, conf DistributedConf) frontier.Frontier {
	peer := p2p.NewPeer(logger.Desugar(), conf.Addr)

	go peer.Listen(context.Background())

	dht, err := makeDHT(logger.Desugar(), peer, conf.Dht)
	if err != nil {
		logger.Fatalln(err)
		return nil
	}

	var opts []frontier.DistributedOption
	if conf.CheckKeysPeriodMs > 0 {
		opts = append(opts, frontier.WithCheckKeysPeriod(conf.CheckKeysPeriodMs))
	}
	if conf.BatchPeriodMs > 0 {
		opts = append(opts, frontier.WithBatchPeriod(conf.BatchPeriodMs))
	}

	distributedFrontier, err := frontier.NewDistributed(logger.Desugar(), peer, bfFrontier, dht, opts...)
	if err != nil {
		logger.Fatalln("Failed to init dispatcher: %s", err.Error())
		return nil
	}

	bootstrap(logger, conf.Bootstrap, distributedFrontier)

	return distributedFrontier
}

func makeDHT(logger *zap.Logger, peer *p2p.Peer, conf DhtConf) (*dht.DHT, error) {
	options := []struct {
		condition bool
		option    dht.DhtOption
	}{
		{conf.FixFingersInterval > 0, dht.WithFixFingersIntervaal(conf.FixFingersInterval)},
		{conf.StabilizeInterval > 0, dht.WithStabilizeInterval(conf.StabilizeInterval)},
		{conf.SuccListLength > 0, dht.WithSuccListLength(conf.SuccListLength)},
		{conf.VnodeNum > 0, dht.WithVnodeNum(conf.VnodeNum)},
	}

	var opts []dht.DhtOption
	for _, opt := range options {
		if opt.condition {
			opts = append(opts, opt.option)
		}
	}

	table, err := dht.NewDHT(logger, peer, opts...)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func bootstrap(logger *zap.SugaredLogger, addr string, distributedFrontier *frontier.DistributedFrontier) {
	fmt.Printf("Node to bootstrap from: %s\n", addr)
	if len(addr) > 0 {
		err := distributedFrontier.Bootstrap(addr)
		if err != nil {
			logger.Infof("Failed to bootstrap from node %s: %s", addr, err.Error())
		}
	}
}

func makeFrontier(conf FrontierConf) *frontier.BfFrontier {
	qp, err := newPersistentQp("data/queues/")
	if err != nil {
		panic(err.Error())
	}

	bloomDb, err := grocksdb.OpenDb(getDbOpts(), "data/bloom/")
	if err != nil {
		panic(err.Error())
	}

	storage := rocksdb.NewRocksdbStorageWithEncoderDecoder[*boom.ScalableBloomFilter](bloomDb, encode, decode)
	//storage := inmem.NewInMemoryStorage[*boom.ScalableBloomFilter]()
	queues, err := qp.GetAll()
	if err != nil {
		panic(err)
	}

	opts := []frontier.BfFrontierOption{}
	if conf.DefaultSessionBudget > 0 {
		opts = append(opts, frontier.WithSessionBudget(conf.DefaultSessionBudget))
	}
	if conf.Politeness > 0 {
		opts = append(opts, frontier.WithPolitenessMultiplier(conf.Politeness))
	}
	if conf.MaxActiveQueues > 0 {
		opts = append(opts, frontier.WithMaxActiveQueues(conf.MaxActiveQueues))
	}

	frontier := frontier.NewBfFrontier(qp, storage, opts...)
	frontier.LoadQueues(queues)
	return frontier
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

func getDbOpts() *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	return opts
}

func openRocksDB(path string) (*grocksdb.DB, error) {
	return grocksdb.OpenDb(getDbOpts(), path)
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
