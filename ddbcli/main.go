package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"

	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/server"
	"github.com/golang/glog"
	ocgrpc "go.opencensus.io/plugin/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port = flag.Int("port", 9090, "Port to listen on.")
var debugPort = flag.Int("debug_port", 9091, "Port to listen on for debug requests.")

func main() {
	flag.Parse()

	glog.Info("Starting DDB server")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Fatalf("Failed to listen: %v", err)
	}
	ocStats := ocgrpc.NewServerStatsHandler()
	gs := grpc.NewServer(grpc.StatsHandler(ocStats))
	ds := server.NewServer(server.DefaultOptions("/tmp/ddb"))
	pb.RegisterDdbServer(gs, ds)

	reflection.Register(gs)

	glog.Infof("Listening on ports %d (main), %d (debug)...\n", *port, *debugPort)

	go startDebugServer()

	if err := gs.Serve(lis); err != nil {
		glog.Fatalf("Failed to serve: %v", err)
	}
}

func startDebugServer() {
	http.HandleFunc("/requests", server.Traces)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *debugPort), nil); err != nil {
		glog.Fatalf("Failed to serve debug: %v", err)
	}
}
