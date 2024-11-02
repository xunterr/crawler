package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/xunterr/crawler/internal/dispatcher"
	"github.com/xunterr/crawler/internal/fetcher"
	"github.com/xunterr/crawler/internal/frontier"
	p2p "github.com/xunterr/crawler/internal/net"
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

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	flag.Parse()

	seedUrl, err := url.Parse(seed)
	if err != nil {
		log.Println("Can't parse seed url!")
		return
	}

	router := p2p.NewRouter()
	peer := p2p.NewPeer(router)

	go peer.Listen(context.Background(), addr)

	qp := frontier.InMemoryQueueProvider{}
	frontier := frontier.NewBfFrontier(qp)

	dispatcherConf := dispatcher.DispatcherConfig{
		Addr:        addr,
		UrlCallback: frontier.Put,
	}
	dispatcher, err := dispatcher.NewDispatcher(peer, router, dispatcherConf)
	if err != nil {
		log.Printf("Failed to init dispatcher: %s", err.Error())
		return
	}

	time.Sleep(5000 * time.Millisecond)

	fmt.Printf("Node to bootstrap from: %s\n", bootstrapNode)
	if bootstrapNode != "" {
		err := dispatcher.Bootstrap(bootstrapNode)
		if err != nil {
			log.Printf("Failed to bootstrap from node %s", bootstrapNode)
		}
	}

	counter := ratecounter.NewRateCounter(1 * time.Second)
	var total atomic.Uint64

	go func() {
		t := time.Tick(5 * time.Second)
		for range t {
			log.Printf("Ops/sec: %d; Total: %d", counter.Rate(), total.Load())
		}
	}()

	if seed != "" {
		err = dispatcher.Dispatch(*seedUrl)
		if err != nil {
			log.Fatalln(err)
			return
		}
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
	wg.Wait()
}
