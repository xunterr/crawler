package net

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/hashicorp/yamux"
	"google.golang.org/protobuf/proto"
)

type Peer struct {
	quit chan struct{}

	router *Router

	connPool map[string]*yamux.Session
	mu       sync.Mutex
}

func NewPeer(router *Router) *Peer {
	return &Peer{
		quit:     make(chan struct{}),
		router:   router,
		connPool: make(map[string]*yamux.Session),
	}
}

type HandlerType int

const (
	RequestHandler HandlerType = iota
	StreamHandler
)

type RequestHandlerFunc func(context.Context, *Request, *ResponseWriter)
type StreamHandlerFunc func(context.Context, *Stream, chan []byte, *ResponseWriter)

type ResponseWriter struct {
	c net.Conn
}

type Router struct {
	requestHandlers map[string]RequestHandlerFunc
	streamHandlers  map[string]StreamHandlerFunc
}

func NewRouter() *Router {
	return &Router{
		requestHandlers: make(map[string]RequestHandlerFunc),
		streamHandlers:  make(map[string]StreamHandlerFunc),
	}
}

func newResponseWriter(c net.Conn) *ResponseWriter {
	return &ResponseWriter{c}
}

func (rw *ResponseWriter) Response(isOk bool, data []byte) error {
	res := &Response{
		IsError: !isOk,
		Payload: data,
	}

	resBytes := res.Marshal()
	msg := &Message{
		Length:  uint32(len(resBytes)),
		Version: 1,
		Type:    ResponseMsg,
		Data:    resBytes,
	}

	msgBytes := msg.Marshal()
	_, err := rw.c.Write(msgBytes)

	rw.c.Close()
	return err
}

func (p *Peer) Dial(addr string) (net.Conn, error) {
	p.mu.Lock()
	session, ok := p.connPool[addr]
	p.mu.Unlock()
	if !ok {
		log.Printf("Creating new session and connection with %s", addr)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		session, err = yamux.Client(conn, nil)
		if err != nil {
			return nil, err
		}

		p.mu.Lock()
		p.connPool[addr] = session
		p.mu.Unlock()
	}

	stream, err := session.Open()
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (p *Peer) CallProto(addr string, scope string, req proto.Message, res proto.Message) error {
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	request := &Request{
		Scope:   scope,
		Payload: reqBytes,
	}

	response, err := p.Call(addr, request)

	return proto.Unmarshal(response.Payload, res)
}

func (p *Peer) Call(addr string, req *Request) (*Response, error) {
	conn, err := p.Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	data := req.Marshal()
	msg := &Message{
		Length:  uint32(len(data)),
		Type:    RequestMsg,
		Version: 1,
		Data:    data,
	}

	_, err = conn.Write(msg.Marshal())
	if err != nil {
		return nil, err
	}

	resMsg, err := ParseMessage(conn)
	if err != nil {
		return nil, err
	}

	if resMsg.Type != ResponseMsg {
		return nil, errors.New("Unexpected response type")
	}

	return ParseResponse(resMsg.Data)
}

func (p *Peer) OpenStream(scope string, addr string) (net.Conn, error) {
	c, err := p.Dial(addr)
	if err != nil {
		return nil, err
	}

	stream := &Stream{
		Scope: scope,
	}

	streamBytes := stream.Marshal()
	msg := &Message{
		Length:  uint32(len(streamBytes)),
		Type:    StreamMsg,
		Version: 1,
		Data:    streamBytes,
	}

	_, err = c.Write(msg.Marshal())
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (p *Peer) Listen(ctx context.Context, addr string) error {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", addr)
	fmt.Println("Starting...")
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		close(p.quit)
		log.Println("Shutting service down...")
		l.Close()
	}()

	for {
		c, err := l.Accept()
		log.Println("New connection!")
		if err != nil {
			select {
			case <-p.quit:
				break
			default:
				log.Printf("Error accepting connection: %s", err.Error())
			}
		}
		go func() {
			session, err := yamux.Server(c, nil)
			if err != nil {
				log.Printf("Failed to open a session: %s", err.Error())
				return
			}

			p.handleSession(session)
			session.Close()
		}()
	}
}

func (p *Peer) handleSession(session *yamux.Session) error {
	for {
		stream, err := session.Accept()
		if err != nil {
			log.Println(err.Error())
			if session.IsClosed() {
				break
			}
			continue
		}

		err = p.handleRequest(context.Background(), stream)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Peer) handleRequest(ctx context.Context, c net.Conn) error {
	msg, err := ParseMessage(c)
	if err != nil {
		return err
	}

	rw := newResponseWriter(c)

	switch msg.Type {
	case RequestMsg:
		req, err := ParseRequest(msg.Data)
		if err != nil {
			return err
		}
		p.router.routeRequest(ctx, rw, req, req.Scope)
	case StreamMsg:
		st, err := ParseStream(msg.Data)
		if err != nil {
			return err
		}

		stream := p.handleStream(ctx, c)
		p.router.routeStream(ctx, rw, st, stream, st.Scope)
	default:
		return errors.New("Unsupported request message type")
	}

	return nil
}

func (p *Peer) handleStream(ctx context.Context, c net.Conn) chan []byte {
	stream := make(chan []byte)
	go func() {
		data, err := readData(c)
		if err != nil {
			stream <- nil
			return
		}

		stream <- data
	}()
	return stream
}

func (r *Router) routeRequest(ctx context.Context, rw *ResponseWriter, req *Request, scope string) {
	handler, ok := r.requestHandlers[scope]
	if ok {
		handler(ctx, req, rw)
	}
}

func (r *Router) routeStream(ctx context.Context, rw *ResponseWriter, stream *Stream, data chan []byte, scope string) {
	handler, ok := r.streamHandlers[scope]
	if ok {
		go handler(ctx, stream, data, rw)
	}
}

func (r *Router) AddStreamHandler(scope string, handler StreamHandlerFunc) {
	r.streamHandlers[scope] = handler
}

func (r *Router) AddRequestHandler(scope string, handler RequestHandlerFunc) {
	r.requestHandlers[scope] = handler
}
