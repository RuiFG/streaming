package main

import (
	"fmt"
	"github.com/spf13/cobra"
)

const Version = "0.0.1"

func init() {
	Command.AddCommand(&cobra.Command{ // versionCmd represents the version command
		Use:   "version",
		Short: "print version info",
		Long:  `print version info`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(Version)
		},
	})
}
