/*
Copyright 2023, Antonio Alvarado Hernández <tnotstar@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package cmd

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cfgFile is the configuration file path
var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "sqltoapi",
	Short: "A SQL data fetcher & API caller tool",
	Long: `As a SQL fetcher it connects to given database and fetch
its data to one or more local JSONS file(s).

As an API end-point caller, it reads the JSONS file(s) and
uploads its data to a given API server.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Hello, world!")
	},
}

// init initializes the command line interface
func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "",
		"config file (default is `sqltoapi.yaml` at binary path)")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.AutomaticEnv()
	viper.SetConfigType("yaml")

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("sqltoapi.yaml")
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: " + viper.ConfigFileUsed())
	}
}

// Execute adds all child commands to the root command and sets flags
// appropriately.
//
// This is called from main.main(). It only needs to happen once
// to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
