package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"os"

	"github.com/xunterr/crawler/internal/dispatcher"
	p2p "github.com/xunterr/crawler/internal/net"
)

var (
	addr          string = "127.0.0.1:6969"
	bootstrapNode string = ""
)

func init() {
	flag.StringVar(&addr, "addr", addr, "defines node address")
	flag.StringVar(&bootstrapNode, "node", "", "node to bootstrap with")
}

func main() {
	flag.Parse()

	client := p2p.NewClient()
	router := p2p.NewRouter()
	server := p2p.NewServer(router)

	go server.Listen(context.Background(), addr)
	d, err := dispatcher.NewDispatcher(client, router, addr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node to bootstrap from: %s\n", bootstrapNode)
	if bootstrapNode != "" {
		println("joining...")
		bn, err := dispatcher.ToNode(bootstrapNode)
		if err != nil {
			panic(err)
		}
		err = d.Join(bn)
		if err != nil {
			panic(err)
		}
		println("ok")
	}
	id := d.GetID()
	fmt.Printf("My ID: %s\n", string(id))

	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()

		id := sha1.New()
		id.Write([]byte(scanner.Text()))
		succ, err := d.FindSuccessor(id.Sum(nil))
		if err != nil {
			panic(err)
		}

		println(id)
		println(succ.Addr.String())
	}
}
