package main

import "github.com/spf13/cobra"

var Command = &cobra.Command{
	Use:   "athena",
	Short: "all in one",
	Long:  `source operator sink have everything that one expects to find`,
}
