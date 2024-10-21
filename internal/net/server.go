package net

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/hashicorp/yamux"
)

type Server struct {
	quit   chan struct{}
	router *Router
}

func NewServer(router *Router) *Server {
	return &Server{
		quit:   make(chan struct{}),
		router: router,
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

func (s *Server) Listen(ctx context.Context, addr string) error {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", addr)
	fmt.Println("Starting...")
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		close(s.quit)
		log.Println("Shutting service down...")
		l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-s.quit:
				break
			default:
				log.Println("Error accepting connection: %s", err.Error())
			}
		}

		go s.handleConn(c)
	}
}

func (s *Server) handleConn(conn net.Conn) error {
	session, err := yamux.Server(conn, nil)
	if err != nil {
		log.Printf("Failed to open a session: %s", err.Error())
		return err
	}

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

		s.router.route(context.Background(), &conn)
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
