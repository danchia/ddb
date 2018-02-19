//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // add debug handlers

	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/server"
	"github.com/golang/glog"
	ocgrpc "go.opencensus.io/plugin/grpc"
	"go.opencensus.io/zpages"
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
	zpages.AddDefaultHTTPHandlers()
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *debugPort), nil); err != nil {
		glog.Fatalf("Failed to serve debug: %v", err)
	}
}
