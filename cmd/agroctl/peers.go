package main

import (
	"fmt"
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
)

var listPeersCommand = cli.Command{
	Name:   "list-peers",
	Usage:  "show the active storage peers in the cluster",
	Action: listPeersAction,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "csv",
			Usage: "output as csv instead",
		},
	},
}

func listPeersAction(c *cli.Context) {
	mds := mustConnectToMDS(c)
	gmd, err := mds.GlobalMetadata()
	peers, err := mds.GetPeers()
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't get peers: %s", err)
		os.Exit(1)
	}
	table := tablewriter.NewWriter(os.Stdout)
	if c.Bool("csv") {
		table.SetBorder(false)
		table.SetColumnSeparator(",")

	} else {
		table.SetHeader([]string{"Address", "UUID", "Storage Size", "Last Registered"})
	}
	for _, x := range peers {
		table.Append([]string{
			x.Address,
			x.UUID,
			humanize.IBytes(x.TotalBlocks * gmd.BlockSize),
			humanize.Time(time.Unix(0, x.LastSeen)),
		})
	}
	table.Render()
}
