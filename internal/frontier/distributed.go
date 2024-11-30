package frontier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/xunterr/crawler/internal/dht"
	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type UrlDiscoveredCallback func(url *url.URL)

const SCOPE = "dispatcher.urlFound"

type DistributedFrontierConf struct {
	BatchPeriodMs int
	Addr          string
}

type DistributedFrontier struct {
	logger *zap.SugaredLogger

	peer *p2p.Peer
	dht  *dht.DHT
	conf DistributedFrontierConf

	frontier *BfFrontier

	batches map[string][]string
	batchMu sync.Mutex
}

func NewDistributed(logger *zap.Logger, peer *p2p.Peer, router *p2p.Router, frontier *BfFrontier, conf DistributedFrontierConf) (*DistributedFrontier, error) {
	dhtConf := dht.DhtConfig{
		Addr:               conf.Addr,
		SuccListLength:     2,
		StabilizeInterval:  10_000,
		FixFingersInterval: 10_000,
	}

	dht, err := dht.NewDHT(logger, peer, router, dhtConf)
	if err != nil {
		return nil, err
	}

	d := &DistributedFrontier{
		logger:   logger.Sugar(),
		peer:     peer,
		dht:      dht,
		frontier: frontier,
		batches:  make(map[string][]string),

		conf: conf,
	}

	router.AddRequestHandler(SCOPE, d.urlFoundHandler)
	go d.dispatcherLoop(context.Background())
	return d, nil
}

func (d *DistributedFrontier) Bootstrap(addr string) error {
	node, err := dht.ToNode(addr)
	if err != nil {
		return err
	}
	return d.dht.Join(node)
}

func (d *DistributedFrontier) Get() (*url.URL, time.Time, error) {
	return d.frontier.Get()
}

func (d *DistributedFrontier) MarkProcessed(u *url.URL, ttr time.Duration) {
	d.frontier.MarkProcessed(u, ttr)
}

func (d *DistributedFrontier) Put(u *url.URL) error {
	succ, err := d.dht.FindSuccessor(d.dht.MakeKey([]byte(u.Host)))
	if err != nil {
		return err
	}

	if bytes.Compare(succ.Id, d.dht.GetID()) == 0 {
		d.frontier.Put(u)
		return nil
	} else {
		return d.createBatch(succ.Addr.String(), u)
	}
}

func (d *DistributedFrontier) urlFoundHandler(ctx context.Context, data *p2p.Request, rw *p2p.ResponseWriter) {
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
		d.frontier.Put(url)
	}

	rw.Response(true, []byte{})
}

func (d *DistributedFrontier) createBatch(node string, u *url.URL) error {
	d.batchMu.Lock()
	defer d.batchMu.Unlock()

	batch, ok := d.batches[node]

	if !ok {
		batch = make([]string, 0)
	}

	batch = append(batch, u.String())
	d.batches[node] = batch
	return nil
}

func (d *DistributedFrontier) dispatcherLoop(ctx context.Context) {
	t := time.Tick(time.Duration(d.conf.BatchPeriodMs) * time.Millisecond)

	for {
		select {
		case <-t:
			d.sendBatches(ctx)
		case <-ctx.Done():
			break
		}
	}
}

func (d *DistributedFrontier) sendBatches(ctx context.Context) {
	d.batchMu.Lock()
	defer d.batchMu.Unlock()
	for k, v := range d.batches {
		d.logger.Infof("Sending batch to node %s", k)

		err := d.writeBatch(ctx, k, SCOPE, v)
		if err != nil {
			d.logger.Infow("Failed to write to node %s: %s", k, err.Error())
			continue
		}

		d.batches[k] = []string{}
	}
}

func (d *DistributedFrontier) writeBatch(ctx context.Context, node string, scope string, messages []string) error {
	batch := &pb.UrlBatch{
		Url: messages,
	}

	batchBytes, err := proto.Marshal(batch)
	if err != nil {
		return err
	}

	req := &p2p.Request{
		Scope:   scope,
		Payload: batchBytes,
	}

	res, err := d.peer.Call(node, req)
	if err != nil {
		return err
	}
	if res.IsError {
		return errors.New(fmt.Sprintf("Peer %s returned error on batch send: %s", node, string(res.Payload)))
	}

	return nil
}
