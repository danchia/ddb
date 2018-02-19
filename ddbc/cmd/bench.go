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

package cmd

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/danchia/ddb/ddbc/common"
	pb "github.com/danchia/ddb/proto"
	"github.com/spf13/cobra"
)

type benchOptions struct {
	keySize  int
	n        int
	qps      float64
	duration time.Duration
	nWorkers int
}

var benchOpts benchOptions

// benchCmd represents the benchmark command
var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Benchmark DDB.",
	Long:  `Runs a configurable benchmark against DDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		go common.SetupDebugServer()

		c, err := common.GetDDB(serverAddr)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Generating keys.")
		keys := make([]string, 0, benchOpts.n)
		keyGen := newKeyGenerator()
		for i := 0; i < benchOpts.n; i++ {
			keys = append(keys, keyGen.next())
		}
		fmt.Println("Done generating keys.")

		endTime := time.Now().Add(benchOpts.duration)
		wStats := make([]*stats, benchOpts.nWorkers)
		var wg sync.WaitGroup
		wg.Add(benchOpts.nWorkers)

		for i := 0; i < benchOpts.nWorkers; i++ {
			wStats[i] = &stats{hist: hdrhistogram.New(0, 60000000, 3)}
			args := workerArgs{
				endTime: endTime,
				keys:    keys,
				client:  c,
				qps:     float64(benchOpts.qps) / float64(benchOpts.nWorkers),
				wg:      &wg,
				s:       wStats[i],
			}
			go benchWorker(args)
		}

		wg.Wait()

		cs := stats{hist: hdrhistogram.New(0, 60000000, 3)}
		for _, s := range wStats {
			cs.hist.Merge(s.hist)
			cs.errors += s.errors
		}

		fmt.Printf("Run complete. Average QPS: %.3f, Total Requests:%v, Errors:%v\n",
			float64(cs.hist.TotalCount())/benchOpts.duration.Seconds(), cs.hist.TotalCount(), cs.errors)

		fmt.Printf("p50: %vus, p95: %vus, p99: %vus",
			cs.hist.ValueAtQuantile(.5), cs.hist.ValueAtQuantile(.95), cs.hist.ValueAtQuantile(.99))
	},
}

type stats struct {
	hist   *hdrhistogram.Histogram
	errors int64
}

type workerArgs struct {
	endTime time.Time
	keys    []string
	client  pb.DdbClient
	qps     float64
	wg      *sync.WaitGroup
	s       *stats
}

func benchWorker(args workerArgs) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	sleepNanos := rand.Int63n(int64(float64(time.Second.Nanoseconds()) / args.qps))
	time.Sleep(time.Duration(sleepNanos) * time.Nanosecond)

	intervalNanos := int64(float64(time.Second.Nanoseconds()) / args.qps)
	ticker := time.NewTicker(time.Duration(intervalNanos) * time.Nanosecond)
	for range ticker.C {
		if time.Now().After(args.endTime) {
			break
		}
		ctx := context.Background()

		req := &pb.GetRequest{Key: args.keys[rnd.Intn(len(args.keys))]}
		start := time.Now()
		_, err := args.client.Get(ctx, req)
		elapsed := time.Now().Sub(start)

		if err != nil {
			args.s.errors++
		}
		if err := args.s.hist.RecordCorrectedValue(elapsed.Nanoseconds()/1000, intervalNanos/1000); err != nil {
			panic(fmt.Sprintf("error recording %v", err))
		}
	}
	ticker.Stop()
	args.wg.Done()
}

func init() {
	rootCmd.AddCommand(benchCmd)

	benchCmd.Flags().IntVar(&benchOpts.keySize, "key_size", 10, "Key length")
	benchCmd.Flags().IntVar(&benchOpts.n, "n", 1000, "Number of entries")
	benchCmd.Flags().IntVar(&benchOpts.nWorkers, "workers", 500, "Number of workers")
	benchCmd.Flags().Float64Var(&benchOpts.qps, "qps", 10, "QPS")
	benchCmd.Flags().DurationVar(&benchOpts.duration, "duration", 30*time.Second, "Duration of test")
}
