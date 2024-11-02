package dispatcher

import (
	"context"
	"log"
	"net/url"

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
	}

	router.AddHandler(SCOPE, d.urlFoundHandler)
	return d, nil
}

func (d *Dispatcher) Bootstrap(addr string) error {
	node, err := dht.ToNode(addr)
	if err != nil {
		return err
	}
	return d.dht.Join(node)
}

func (d *Dispatcher) Dispatch(u url.URL) error {
	succ, err := d.dht.FindSuccessor(d.dht.MakeKey([]byte(u.Host)))
	if err != nil {
		return err
	}

	d.urlFoundRequest(succ.Addr.String(), u.String())

	return nil
}

func (d *Dispatcher) urlFoundHandler(ctx context.Context, conn *p2p.Conn) {
	bytes, err := p2p.ReadMessage(conn)
	if err != nil {
		return
	}

	msg := &pb.URL{}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return
	}

	url, err := url.Parse(msg.GetUrl())
	if err != nil {
		log.Printf("Malformed URL: %s", msg.GetUrl())
		return
	}

	d.urlCallback(url)
}

func (d *Dispatcher) urlFoundRequest(node string, url string) error {
	req := &pb.URL{
		Url: url,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	c, err := d.peer.Dial(SCOPE, node)
	if err != nil {
		return err
	}

	_, err = p2p.WriteMessage(c, data)
	return err
}
