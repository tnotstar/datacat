// Copyright 2023, Antonio Alvarado Hernández <tnotstar@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/tnotstar/sqltoapi/core"
)

// `cfgFile` is the configuration file path.
var cfgFile string

// `taskName` is the name of the task to be executed.
var taskName string

// `rootCmd` represents the base command line handler.
var rootCmd = &cobra.Command{
	Use:   "sql2api",
	Short: "A SQL data fetcher & API caller tool",
	Long: `As a SQL fetcher it connects to given database and fetch
its data to one or more local NDJSON file(s).

As an API end-point caller, it reads the NDJSON file(s) and
uploads its data to a given API server.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		core.InitConfig(cfgFile)
	},
}

// `init` initializes the `root` command line handler.
func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config-file", "c", "sql2api.yaml",
		"config file (default is `sql2api.yaml` at binary path)")

	rootCmd.PersistentFlags().StringVarP(&taskName, "task-name", "t", "",
		"the name of the task to be executed")
}

// `ExecuteRoot` executes the `root` command and handles errors
// appropriately. This function is called from `main.main()`.
func ExecuteRoot() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
