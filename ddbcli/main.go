package main

import (
	"flag"
	"fmt"
	"net"

	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/server"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port = flag.Int("port", 9090, "Port to listen on.")

func main() {
	flag.Parse()

	glog.Info("Starting DDB server")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Fatalf("Failed to listen: %v", err)
	}
	gs := grpc.NewServer()
	ds := server.NewServer(server.DefaultOptions("/tmp/ddb"))
	pb.RegisterDdbServer(gs, ds)

	reflection.Register(gs)

	glog.Infof("Listening on port %d...\n", *port)

	if err := gs.Serve(lis); err != nil {
		glog.Fatalf("Failed to serve: %v", err)
	}
}
