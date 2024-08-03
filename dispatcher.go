package main

import (
	"bufio"
	"context"
	"log"
	"net/url"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

type UrlDiscoveredCallback func(url url.URL)

const PROTOCOL = "/urlDiscovery/1.0.0"

type Dispatcher struct {
	host        *rhost.RoutedHost
	dht         *dht.IpfsDHT
	urlCallback UrlDiscoveredCallback
}

var NOPUrlDiscoveredCallback UrlDiscoveredCallback = func(url url.URL) {}

func NewDispatcher(host host.Host, dht *dht.IpfsDHT, callback UrlDiscoveredCallback) Dispatcher {
	d := Dispatcher{
		host:        rhost.Wrap(host, dht),
		dht:         dht,
		urlCallback: callback,
	}
	d.host.SetStreamHandler(PROTOCOL, d.discoveryHandler)
	return d
}

func (d Dispatcher) discoveryHandler(s network.Stream) {
	log.Printf("Incoming stream from node %s", s.Conn().RemotePeer().String())

	if d.urlCallback == nil {
		log.Println("No url callback set, aborting")
		return
	}
	scanner := bufio.NewScanner(s)
	for scanner.Scan() {
		str := scanner.Text()

		url, err := url.Parse(str)
		if err != nil {
			log.Printf("Failed to parse incoming URL: %s", err.Error())
			return
		}

		d.urlCallback(*url)
	}
}

func (d Dispatcher) Dispatch(url url.URL) error {
	id, err := calculateCID([]byte(url.Hostname()))
	if err != nil {
		log.Printf("Failed to calculate cid: %s", err.Error()) //codestyle: handling and passing !?
		return err
	}

	providers, err := d.dht.FindProviders(context.Background(), id) //todo: partitioning here
	if err != nil {
		log.Printf("Failed to find providers for key %s: %s", id.String(), err)
		return err
	}

	if len(providers) < 1 {
		log.Printf("No providers found for CID %s. Providing key...", id.String())
		d.dht.Provide(context.Background(), id, true)
		d.urlCallback(url)
	} else if providers[0].ID == d.host.ID() {
		d.urlCallback(url)
	} else {
		log.Printf("Redirecting key %s to node %s", id.String(), providers[0].ID.String())
		err := d.writeToNode(context.Background(), providers[0].ID, PROTOCOL, url.String()+"\n")
		if err != nil {
			log.Printf("Failed to write to node %s: %s", providers[0].ID.String(), err.Error())
			return err
		}
	}
	return nil
}

func calculateCID(data []byte) (cid.Cid, error) {
	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Raw),
		MhType:   mh.SHA2_256,
		MhLength: -1, // default length
	}

	id, err := pref.Sum(data)
	if err != nil {
		return cid.Cid{}, err
	}

	return id, nil
}

func (d Dispatcher) writeToNode(ctx context.Context, peerID peer.ID, protocol protocol.ID, message string) error {
	s, err := d.host.NewStream(context.Background(), peerID, protocol)
	if err != nil {
		return err
	}
	_, err = s.Write([]byte(message))
	if err != nil {
		return err
	}
	return nil
}
