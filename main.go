package main

import (
	"context"
	"log"
	"net/url"
	"time"

	"sync/atomic"

	"github.com/paulbellamy/ratecounter"
	"github.com/xunterr/crawler/internal/dispatcher"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/frontier"
	"github.com/xunterr/crawler/internal/net"
)

func main() {
	//	bootstrapNode := flag.String("b", "", "bootstrap node")
	//	seedUrl := flag.String("u", "", "seed url")
	//	port := flag.Int("p", 6969, "port number")
	//	flag.Parse()
	//
	//	golog.SetAllLoggers(golog.LevelError)
	//	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	//
	//	if err != nil {
	//		log.Fatalln(err)
	//		return
	//	}
	//
	//	opts := []libp2p.Option{
	//		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *port)),
	//		libp2p.Identity(privKey),
	//		libp2p.DefaultTransports,
	//		libp2p.DefaultMuxers,
	//		libp2p.DefaultSecurity,
	//		libp2p.NATPortMap(),
	//	}

	//	qp, err := frontier.NewPersistentQueueProvider("abc")
	//	if err != nil {
	//		log.Panicf(err.Error())
	//		return
	//	}
	qp := frontier.InMemoryQueueProvider{}
	frontier := frontier.NewBfFrontier(qp)

	//	queues, err := qp.GetAll()
	//	if err != nil {
	//		log.Panicln(err.Error())
	//		return
	//	}
	//	frontier.LoadQueues(queues)

	//	dispatcher, err := SetupDispatcher(context.Background(), opts, frontier.Put)
	//	if err != nil {
	//		log.Fatalf("Failed to setup dispatcher: %s", err.Error())
	//		return
	//	}
	//
	//	if *bootstrapNode != "" {
	//		dispatcher.BootstrapFrom(context.Background(), []string{*bootstrapNode})
	//	}
	//
	//	addresses, err := dispatcher.GetAddress()
	//	if err != nil {
	//		log.Fatalf("Error building address: %s", err.Error())
	//		return
	//	}
	//
	//	for _, e := range addresses {
	//		log.Println("I can be reached at: %s", e.String())
	//	}

	time.Sleep(500 * time.Millisecond)

	parsedUrl, err := url.Parse(*seedUrl)
	if err != nil {
		log.Fatalln("Failed to parse url")
		return
	}

	counter := ratecounter.NewRateCounter(1 * time.Second)
	var total atomic.Uint64
	go func() {
		t := time.Tick(5 * time.Second)
		for range t {
			log.Printf("Ops/sec: %d; Total: %d", counter.Rate(), total.Load())
		}
	}()

	client := net.NewClient()
	router := net.NewRouter()
	server := net.NewServer(router)
	dispatcher, err := dispatcher.NewDispatcher(client, router, "127.0.0.1:6969")
	if err != nil {
		panic(err)
	}

	go server.Listen(context.Background(), "127.0.0.1:6969")
	err = dispatcher.Dispatch(*parsedUrl)
	if err != nil {
		log.Fatalln(err)
		return
	}

	fetcher := fetcher.DefaultFetcher{}

	for {
		time.Sleep(50 * time.Millisecond)
		url, accessAt, err := frontier.Get()
		if err != nil {
			log.Println(err)
			return
		}

		go func() {

			time.Sleep(time.Until(accessAt))
			// log.Printf("Crawling %s", url.Hostname())

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
