package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var serverAddr string
var baseDir string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "ddbc",
	Short: "A tool for interacting with DDB",
	Long:  `ddbc is a tool for interacting with DDB.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&serverAddr, "addr", "localhost:9090", "server address, host:port")
	rootCmd.PersistentFlags().StringVar(&baseDir, "base_dir", "/tmp/ddb", "DDB base directory.")
}
