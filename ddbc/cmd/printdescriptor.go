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
