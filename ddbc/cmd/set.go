package cmd

import (
	"context"
	"log"

	"github.com/danchia/ddb/ddbc/common"
	pb "github.com/danchia/ddb/proto"
	"github.com/spf13/cobra"
)

// setCmd represents the set command
var setCmd = &cobra.Command{
	Use:   "set",
	Short: "Sets a key/value.",
	Long:  `Sets a key/value pair on DDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := common.GetDDB(serverAddr)
		if err != nil {
			log.Fatal(err)
		}

		req := &pb.SetRequest{
			Key:   args[0],
			Value: []byte(args[1]),
		}
		log.Printf("Set %v", req)
		resp, err := c.Set(context.Background(), req)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Resp %v", resp)
	},
}

func init() {
	rootCmd.AddCommand(setCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// setCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// setCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
