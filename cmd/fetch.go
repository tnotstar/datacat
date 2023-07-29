package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

func Fetch(cmd *cobra.Command, args []string) {
	log.Println("Hello, world!")
}
