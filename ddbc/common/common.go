package common

import (
	pb "github.com/danchia/ddb/proto"
	"google.golang.org/grpc"
)

// GetDDB returns a DDB stub.
func GetDDB(addr string) (pb.DdbClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewDdbClient(conn)
	return client, nil
}
