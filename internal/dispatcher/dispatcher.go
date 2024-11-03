package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/xunterr/crawler/internal/dht"
	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"google.golang.org/protobuf/proto"
)

type UrlDiscoveredCallback func(url *url.URL)

const SCOPE = "dispatcher.urlFound"

type DispatcherConfig struct {
	Addr        string
	UrlCallback UrlDiscoveredCallback
}

type Dispatcher struct {
	peer        *p2p.Peer
	dht         *dht.DHT
	urlCallback UrlDiscoveredCallback

	batches map[string][]string
	batchMu sync.Mutex
}

func NewDispatcher(peer *p2p.Peer, router *p2p.Router, conf DispatcherConfig) (*Dispatcher, error) {
	dht, err := dht.NewDHT(peer, router, conf.Addr)
	if err != nil {
		return nil, err
	}

	d := &Dispatcher{
		peer:        peer,
		dht:         dht,
		urlCallback: conf.UrlCallback,
		batches:     make(map[string][]string),
	}

	router.AddRequestHandler(SCOPE, d.urlFoundHandler)
	go d.dispatcherLoop(context.Background())
	return d, nil
}

func (d *Dispatcher) Bootstrap(addr string) error {
	node, err := dht.ToNode(addr)
	if err != nil {
		return err
	}
	return d.dht.Join(node)
}

func (d *Dispatcher) Dispatch(u *url.URL) error {
	succ, err := d.dht.FindSuccessor(d.dht.MakeKey([]byte(u.Host)))
	if err != nil {
		return err
	}

	if bytes.Compare(succ.Id, d.dht.GetID()) == 0 {
		d.urlCallback(u)
		return nil
	} else {
		return d.createBatch(succ.Addr.String(), u)
	}
}

func (d *Dispatcher) urlFoundHandler(ctx context.Context, data *p2p.Request, rw *p2p.ResponseWriter) {
	batch := &pb.UrlBatch{}
	if err := proto.Unmarshal(data.Payload, batch); err != nil {
		rw.Response(false, []byte{})
		return
	}

	for _, e := range batch.Url {
		url, err := url.Parse(e)
		if err != nil {
			continue
		}
		d.urlCallback(url)
	}

	rw.Response(true, []byte{})
}

func (d *Dispatcher) createBatch(node string, u *url.URL) error {
	d.batchMu.Lock()
	d.batchMu.Unlock()

	batch, ok := d.batches[node]
	if !ok {
		batch = make([]string, 0)
	}

	batch = append(batch, u.String())
	d.batches[node] = batch
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

		err := d.writeBatch(ctx, k, SCOPE, v)
		if err != nil {
			log.Printf("Failed to write to node %s: %s", k, err.Error())
			continue
		}

		d.batches[k] = []string{}
	}
}

func (d *Dispatcher) writeBatch(ctx context.Context, node string, scope string, messages []string) error {
	batch := &pb.UrlBatch{
		Url: messages,
	}

	batchBytes, err := proto.Marshal(batch)
	if err != nil {
		return err
	}

	conn, err := d.peer.Dial(node)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &p2p.Request{
		Scope:   scope,
		Payload: batchBytes,
	}

	res, err := p2p.Call(conn, req)
	if res.IsError {
		return errors.New(fmt.Sprintf("Peer %s returned error on batch send: %s", node, string(res.Payload)))
	}

	return nil
}
