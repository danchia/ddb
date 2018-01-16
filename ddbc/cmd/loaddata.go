package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/danchia/ddb/ddbc/common"
	pb "github.com/danchia/ddb/proto"
	"github.com/spf13/cobra"
)

var (
	keySize   int
	valueSize int
	n         int
	nWorkers  int
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

		var wg sync.WaitGroup
		ch := make(chan *pb.SetRequest, nWorkers)

		for i := 0; i < nWorkers; i++ {
			go func() {
				for r := range ch {
					_, err := c.Set(context.Background(), r)
					if err != nil {
						log.Fatal(err)
					}
				}
				wg.Done()
			}()
			wg.Add(1)
		}

		t := time.Now()

		keyGen := newKeyGenerator()
		for i := 0; i < n; i++ {
			req := &pb.SetRequest{
				Key:   keyGen.next(),
				Value: genValue(),
			}
			ch <- req
		}

		close(ch)
		wg.Wait()

		elapsed := time.Now().Sub(t)
		fmt.Printf("Wrote %v entries in %v.", n, elapsed)
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
	loaddataCmd.Flags().IntVar(&nWorkers, "num_workers", 20, "Number of load workers")
	loaddataCmd.Flags().IntVar(&n, "n", 1000, "Number of entries")
}
