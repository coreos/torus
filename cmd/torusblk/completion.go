package main

import (
	"os"

	"github.com/spf13/cobra"
)

var completionCommand = &cobra.Command{
	Use:   "completion",
	Short: "Output bash completion code",
	Run:   completionAction,
}

func completionAction(cmd *cobra.Command, args []string) {
	cmd.Root().GenBashCompletion(os.Stdout)
}
