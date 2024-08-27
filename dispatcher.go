package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	rhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	"github.com/multiformats/go-multiaddr"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

type UrlDiscoveredCallback func(url url.URL)

const PROTOCOL = "/urlDiscovery/1.0.0"

type Dispatcher struct {
	host        *rhost.RoutedHost
	dht         *dht.IpfsDHT
	urlCallback UrlDiscoveredCallback

	batches map[peer.ID][]string
	batchMu sync.Mutex
}

var NOPUrlDiscoveredCallback UrlDiscoveredCallback = func(url url.URL) {}

func SetupDispatcher(ctx context.Context, opts []libp2p.Option, callback UrlDiscoveredCallback) (*Dispatcher, error) {
	basicHost, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(ctx, basicHost, dht.Mode(dht.ModeAutoServer))
	if err != nil {
		return nil, err
	}

	d := &Dispatcher{
		host:        rhost.Wrap(basicHost, dht),
		dht:         dht,
		urlCallback: callback,
		batches:     make(map[peer.ID][]string),
	}

	d.host.SetStreamHandler(PROTOCOL, d.discoveryHandler)

	go d.dispatcherLoop(ctx)

	return d, nil
}

func (d *Dispatcher) BootstrapFrom(ctx context.Context, bootstrapNodes []string) error {
	if err := d.dht.Bootstrap(ctx); err != nil {
		return err
	}

	for _, bootstrapNode := range bootstrapNodes {
		bootstrapMaddr := multiaddr.StringCast(bootstrapNode)
		bootstrapPeerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapMaddr)
		if err != nil {
			log.Printf("Failed to bootstrap from node %s", bootstrapNode)
			continue
		}

		go func() {
			d.host.Peerstore().AddAddrs(bootstrapPeerInfo.ID, bootstrapPeerInfo.Addrs, peerstore.PermanentAddrTTL)
			if err := d.host.Connect(context.Background(), *bootstrapPeerInfo); err != nil {
				log.Printf("Failed to connect to node %s", bootstrapPeerInfo.String())
				return
			} else {
				log.Printf("Bootstraped with node %s", bootstrapPeerInfo.ID)
			}
		}()
	}

	return nil
}

func (d *Dispatcher) GetAddress() ([]multiaddr.Multiaddr, error) {
	hostAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ipfs/%s", d.host.ID()))
	if err != nil {
		return nil, err
	}

	var addresses []multiaddr.Multiaddr
	for _, addr := range d.host.Addrs() {
		addresses = append(addresses, addr.Encapsulate(hostAddr))
	}

	return addresses, nil
}

func (d *Dispatcher) discoveryHandler(s network.Stream) {
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

func (d *Dispatcher) Dispatch(u url.URL) error {
	id, err := calculateCID([]byte(u.Hostname()))
	if err != nil {
		log.Printf("Failed to calculate cid: %s", err.Error()) //codestyle: handling and passing !?
		return err
	}

	providers, err := d.dht.FindProviders(context.Background(), id) //todo: partitioning here
	if err != nil {
		//log.Printf("Failed to find providers for key %s: %s", id.String(), err.Error())
		return err
	}

	if len(providers) < 1 {
		//log.Printf("No providers found for CID %s. Providing key...", id.String())
		d.dht.Provide(context.Background(), id, true)
		d.urlCallback(u)
		return nil
	}

	peer := providers[0]

	if peer.ID == d.host.ID() {
		d.urlCallback(u)
	} else {
		//log.Printf("Adding key %s to node %s batch", id.String(), providers[0].ID.String())
		d.createBatch(peer.ID, u)
	}
	return nil
}

func (d *Dispatcher) createBatch(peer peer.ID, u url.URL) error {
	d.batchMu.Lock()
	d.batchMu.Unlock()

	batch, ok := d.batches[peer]
	if !ok {
		batch = make([]string, 0)
	}

	batch = append(batch, u.String())
	d.batches[peer] = batch
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

func (d *Dispatcher) writeToNode(ctx context.Context, peerID peer.ID, protocol protocol.ID, message string) error {
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

func (d *Dispatcher) dispatcherLoop(ctx context.Context) {
	t := time.Tick(30 * time.Second)

	for {
		select {
		case <-t:
			d.sendBatches(ctx)
		case <-ctx.Done():
			break
		}
	}
}

func (d *Dispatcher) sendBatches(ctx context.Context) {
	d.batchMu.Lock()
	defer d.batchMu.Unlock()
	for k, v := range d.batches {
		log.Printf("Sending batch to node %s", k)

		err := d.writeBatch(ctx, k, PROTOCOL, v)
		if err != nil {
			log.Printf("Failed to write to node %s: %s", k.String(), err.Error())
			continue
		}

		d.batches[k] = []string{}
	}
}

func (d *Dispatcher) writeBatch(ctx context.Context, peerID peer.ID, protocol protocol.ID, messages []string) error {

	s, err := d.host.NewStream(context.Background(), peerID, protocol)
	if err != nil {
		return err
	}

	for _, m := range messages {
		_, err = s.Write([]byte(fmt.Sprintf("%s\n", m)))
		if err != nil {
			return err
		}
	}
	return nil
}
