package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

func Call(cmd *cobra.Command, args []string) {
	log.Println("Hello, world!")
}
