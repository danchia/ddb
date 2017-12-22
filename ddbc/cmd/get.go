package cmd

import (
	"context"
	"log"

	"github.com/danchia/ddb/ddbc/common"
	pb "github.com/danchia/ddb/proto"
	"github.com/spf13/cobra"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a key/value.",
	Long:  `Gets a key/value pair on DDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := common.GetDDB(serverAddr)
		if err != nil {
			log.Fatal(err)
		}

		req := &pb.GetRequest{
			Key: args[0],
		}
		log.Printf("Get %v", req)
		resp, err := c.Get(context.Background(), req)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Resp %v", resp)
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
