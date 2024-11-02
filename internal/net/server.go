package net

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/hashicorp/yamux"
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

type Handler func(context.Context, *Conn)

type Router struct {
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		handlers: map[string]Handler{},
	}
}

func (p *Peer) Dial(scope string, addr string) (*Conn, error) {
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

	sendHeader(scope, stream)

	return &Conn{
		Remote: session.Addr(),
		Scope:  scope,
		stream: stream,
	}, nil
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

		header, err := readHeader(stream)
		if err != nil {
			log.Println(err.Error())
			continue
		}

		conn := Conn{
			Remote: stream.RemoteAddr(),
			Scope:  header.GetScope(),
			stream: stream,
		}

		p.router.route(context.Background(), &conn)
	}

	return nil
}

func (r *Router) route(ctx context.Context, conn *Conn) {
	handler, ok := r.handlers[conn.Scope]
	if !ok {
		log.Printf("No handler found for scope %s", conn.Scope)
		return
	}

	handler(ctx, conn)
}

func (r *Router) AddHandler(scope string, handler Handler) {
	r.handlers[scope] = handler
}
