package cmd

import (
	"fmt"

	"github.com/danchia/ddb/server"
	"github.com/spf13/cobra"
)

// printdescriptorCmd represents the printdescriptor command
var printdescriptorCmd = &cobra.Command{
	Use:   "printdescriptor",
	Short: "Prints database descriptor.",
	Long:  `Prints the database descriptor (potentially long) for debugging.`,
	Run: func(cmd *cobra.Command, args []string) {
		opts := server.DefaultOptions(baseDir)
		d, err := server.LoadDescriptor(opts.DescriptorDir)
		if err != nil {
			fmt.Printf("Error loading descriptor: %v", err)
			return
		}
		fmt.Printf("%v", d.Current)
	},
}

func init() {
	rootCmd.AddCommand(printdescriptorCmd)
}
