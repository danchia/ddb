package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/danchia/ddb/ddbc/common"
	pb "github.com/danchia/ddb/proto"
	"github.com/spf13/cobra"
)

var (
	keySize   int
	valueSize int
	n         int
)

// loaddataCmd represents the loaddata command
var loaddataCmd = &cobra.Command{
	Use:   "loaddata",
	Short: "Loads data into DDB.",
	Long:  `Loads synthetic data into DDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := common.GetDDB(serverAddr)
		if err != nil {
			log.Fatal(err)
		}

		keyGen := newKeyGenerator()

		for i := 0; i < n; i++ {
			req := &pb.SetRequest{
				Key:   keyGen.next(),
				Value: genValue(),
			}
			_, err := c.Set(context.Background(), req)
			if err != nil {
				log.Fatal(err)
			}
		}

		fmt.Printf("Wrote %v entries.", n)
	},
}

const keyAlpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvyxyz1234567890"

type keyGenerator struct {
	rnd *rand.Rand
}

func newKeyGenerator() *keyGenerator {
	return &keyGenerator{rand.New(rand.NewSource(1))}
}

func (g *keyGenerator) next() string {
	var buf bytes.Buffer
	for i := 0; i < keySize; i++ {
		buf.WriteByte(keyAlpha[g.rnd.Intn(len(keyAlpha))])
	}
	return buf.String()
}

func genValue() []byte {
	res := make([]byte, valueSize)
	for i := 0; i < valueSize; i++ {
		res[i] = byte(rand.Intn(255))
	}
	return res
}

func init() {
	rootCmd.AddCommand(loaddataCmd)

	loaddataCmd.Flags().IntVar(&keySize, "key_size", 10, "Key length")
	loaddataCmd.Flags().IntVar(&valueSize, "value_size", 800, "Value length")
	loaddataCmd.Flags().IntVar(&n, "n", 1000, "Number of entries")
}
