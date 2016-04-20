package main

import (
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var outputAsCSV bool

var listPeersCommand = &cobra.Command{
	Use:   "list-peers",
	Short: "show the active storage peers in the cluster",
	Run:   listPeersAction,
}

func init() {
	listPeersCommand.Flags().BoolVarP(&outputAsCSV, "csv", "", false, "output as csv instead")
}

func listPeersAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	gmd, err := mds.GlobalMetadata()
	if err != nil {
		die("couldn't get global metadata: %v", err)
	}
	peers, err := mds.GetPeers()
	if err != nil {
		die("couldn't get peers: %v", err)
	}
	ring, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	members := ring.Members()
	table := tablewriter.NewWriter(os.Stdout)
	if outputAsCSV {
		table.SetBorder(false)
		table.SetColumnSeparator(",")
	} else {
		table.SetHeader([]string{"Address", "UUID", "Size", "Used", "Member", "Updated", "Reb/Rep Data"})
	}
	rebalancing := false
	for _, x := range peers {
		ringStatus := "Avail"
		if x.Address == "" {
			continue
		}
		if members.Has(x.UUID) {
			ringStatus = "OK"
		}
		table.Append([]string{
			x.Address,
			x.UUID,
			humanize.IBytes(x.TotalBlocks * gmd.BlockSize),
			humanize.IBytes(x.UsedBlocks * gmd.BlockSize),
			ringStatus,
			humanize.Time(time.Unix(0, x.LastSeen)),
			humanize.IBytes(x.RebalanceInfo.LastRebalanceBlocks*gmd.BlockSize*uint64(time.Second)/uint64(x.LastSeen+1-x.RebalanceInfo.LastRebalanceFinish)) + "/sec",
		})
		if x.RebalanceInfo.Rebalancing {
			rebalancing = true
		}
	}

	for _, x := range members {
		ringStatus := "DOWN"
		ok := false
		for _, p := range peers {
			if p.UUID == x {
				ok = true
				break
			}
		}
		if ok {
			continue
		}
		table.Append([]string{
			"",
			x,
			"???",
			"???",
			ringStatus,
			"Missing",
			"",
		})
	}
	table.Render()
	fmt.Printf("Balanced: %v\n", !rebalancing)
}
