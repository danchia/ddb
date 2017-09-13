package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port = flag.Int("port", 9090, "Port to listen on.")

func main() {
	fmt.Printf("Starting server\n")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	gs := grpc.NewServer()
	ds := server.NewServer()
	pb.RegisterDdbServer(gs, ds)

	reflection.Register(gs)

	fmt.Printf("Listening on port %d...\n", *port)

	if err := gs.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
