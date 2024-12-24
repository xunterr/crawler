package frontier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/xunterr/crawler/internal/dht"
	p2p "github.com/xunterr/crawler/internal/net"
	pb "github.com/xunterr/crawler/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

type UrlDiscoveredCallback func(url *url.URL)

const (
	URL_FOUND   = "dispatcher.urlFound"
	KEYS_LOCK   = "dispatcher.keysLock"
	LOCK_NOTIFY = "dispatcher.lockNotify"
)

type DistributedFrontierConf struct {
	BatchPeriodMs     int
	CheckKeysPeriodMs int
	Addr              string
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

func NewDistributed(logger *zap.Logger, peer *p2p.Peer, frontier *BfFrontier, conf DistributedFrontierConf) (*DistributedFrontier, error) {
	dhtConf := dht.DhtConfig{
		Addr:               conf.Addr,
		SuccListLength:     2,
		StabilizeInterval:  10_000,
		FixFingersInterval: 15_000,
	}

	table, err := dht.NewDHT(logger, peer, dhtConf)
	if err != nil {
		return nil, err
	}

	d := &DistributedFrontier{
		logger:   logger.Sugar(),
		peer:     peer,
		dht:      table,
		frontier: frontier,
		batches:  make(map[string][]string),

		conf: conf,
	}

	peer.AddRequestHandler(URL_FOUND, d.urlFoundHandler)
	peer.AddRequestHandler(KEYS_LOCK, d.keysLockHandler)
	peer.AddStreamHandler(LOCK_NOTIFY, d.keyLockNotifyHandler)

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
	succ, err := d.dht.FindSuccessor(d.dht.MakeKey([]byte(u.Hostname())))
	if err != nil {
		return err
	}

	if bytes.Compare(succ.Id, d.dht.GetID()) == 0 {
		return d.frontier.Put(u)
	} else {
		return d.createBatch(succ.Addr.String(), u)
	}
}

func (d *DistributedFrontier) checkKeys() {
	repartitioned := make(map[string][]string)
	d.frontier.qmMu.Lock()
	for k, q := range d.frontier.queueMap {
		succ, err := d.dht.FindSuccessor(d.dht.MakeKey([]byte(k)))
		if err != nil {
			continue
		}

		if bytes.Compare(succ.Id, d.dht.GetID()) != 0 && !q.IsEmpty() {
			if keys, ok := repartitioned[succ.Addr.String()]; ok {
				keys = append(keys, k)
				repartitioned[succ.Addr.String()] = keys
			} else {
				repartitioned[succ.Addr.String()] = []string{k}
			}
		}
	}
	d.frontier.qmMu.Unlock()

	d.logger.Infof("Found %d conflicting keys", len(repartitioned))

	for k, v := range repartitioned {
		go d.sendKeysLock(k, v)

		conn, err := p2p.NewStreamWriter(d.peer, k).OpenStream(LOCK_NOTIFY)
		if err != nil {
			d.logger.Errorf("Error opening stream: %s", err.Error())
			continue
		}

		var wg sync.WaitGroup
		for _, e := range v {
			wg.Add(1)
			go func() {
				d.sendKeyNotify(conn, e)
				wg.Done()
			}()
		}

		wg.Wait()
		conn.Close()
	}
}

func (d *DistributedFrontier) sendKeyNotify(conn net.Conn, key string) error {
	<-d.frontier.NotifyOnEnd(key)
	notification := &pb.KeyLockNotification{
		Key:   key,
		Bloom: d.frontier.getBloom(key),
	}

	d.logger.Logw(zapcore.InfoLevel,
		"Finished processing key, notifying node.",
		"key", key,
		"node", conn.RemoteAddr().String())

	data, err := proto.Marshal(notification)
	if err != nil {
		return err
	}

	err = p2p.WriteToStream(conn, data)
	if err != nil {
		d.logger.Errorw("Error writing to stream",
			"node", conn.RemoteAddr().String(),
			"error", err.Error())
		return err
	}

	return nil
}

func (d *DistributedFrontier) keyLockNotifyHandler(ctx p2p.Context, data chan []byte, rw *p2p.ResponseWriter) {
	for req := range data {
		d.logger.Info("Received unlock")
		notif := &pb.KeyLockNotification{}
		if err := proto.Unmarshal(req, notif); err != nil {
			return
		}

		d.logger.Infof("Unlocking key: %s", notif.Key)
		d.frontier.setQueueLock(notif.Key, false)
		d.frontier.setBloom(notif.Key, notif.Bloom)
	}
}

func (d *DistributedFrontier) sendKeysLock(node string, keys []string) error {
	req := &pb.UrlBatch{
		Url: keys,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	request := &p2p.Request{
		Scope:   KEYS_LOCK,
		Payload: data,
	}

	res, err := p2p.NewRequestWriter(d.peer, node).Request(request)
	if err != nil {
		return err
	}

	if res.IsError {
		return errors.New(fmt.Sprintf("Remote node error: %s", string(res.Payload)))
	}

	return nil
}

func (d *DistributedFrontier) keysLockHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	keys := &pb.UrlBatch{}
	if err := proto.Unmarshal(data, keys); err != nil {
		rw.Response(false, []byte(err.Error()))
		return
	}

	for _, e := range keys.Url {
		d.logger.Infof("Locking key: %s", e)
		d.frontier.setQueueLock(e, true)
	}
}

func (d *DistributedFrontier) urlFoundHandler(ctx p2p.Context, data []byte, rw *p2p.ResponseWriter) {
	batch := &pb.UrlBatch{}
	if err := proto.Unmarshal(data, batch); err != nil {
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
	go func() {
		t := time.Tick(time.Duration(d.conf.BatchPeriodMs) * time.Millisecond)
		for {
			select {
			case <-t:
				go d.sendBatches(ctx)
			case <-ctx.Done():
				break
			}
		}
	}()

	go func() {
		t := time.Tick(time.Duration(d.conf.CheckKeysPeriodMs) * time.Millisecond)
		for {
			select {
			case <-t:
				d.checkKeys()
			case <-ctx.Done():
				break
			}
		}
	}()
}

func (d *DistributedFrontier) sendBatches(ctx context.Context) {
	d.logger.Infof("Sending batches (%d)", len(d.batches))
	d.batchMu.Lock()
	defer d.batchMu.Unlock()
	for k, v := range d.batches {
		d.logger.Infof("Sending batch to node %s", k)

		err := d.writeBatch(ctx, k, URL_FOUND, v)
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

	res, err := p2p.NewRequestWriter(d.peer, node).Request(req)
	if err != nil {
		return err
	}
	if res.IsError {
		return errors.New(fmt.Sprintf("Peer %s returned error on batch send: %s", node, string(res.Payload)))
	}

	return nil
}
