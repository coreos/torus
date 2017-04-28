package main

import (
	"fmt"
	"os"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var (
	newPeers    torus.PeerInfoList
	allPeers    bool
	force       bool
	outputAsCSV bool
	outputAsSI  bool
)

var peerCommand = &cobra.Command{
	Use:   "peer",
	Short: "add/remove peers from the cluster",
	Run:   peerAction,
}

var peerListCommand = &cobra.Command{
	Use:   "list",
	Short: "list peers in the cluster",
	Run:   listPeersAction,
}

var peerAddCommand = &cobra.Command{
	Use:    "add ADDRESS|UUID",
	Short:  "add a peer to the cluster",
	PreRun: peerChangePreRun,
	Run:    peerAddAction,
}

var peerRemoveCommand = &cobra.Command{
	Use:    "remove ADDRESS|UUID",
	Short:  "remove a peer from the cluster",
	PreRun: peerChangePreRun,
	Run:    peerRemoveAction,
}

func init() {
	peerCommand.AddCommand(peerAddCommand, peerRemoveCommand, peerListCommand)
	peerAddCommand.Flags().BoolVar(&allPeers, "all-peers", false, "add all peers")
	peerRemoveCommand.PersistentFlags().BoolVar(&force, "force", false, "force-remove a UUID")
	peerListCommand.Flags().BoolVarP(&outputAsCSV, "csv", "", false, "output as csv instead")
	peerListCommand.Flags().BoolVarP(&outputAsSI, "si", "", false, "output sizes in powers of 1000")
}

func peerAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func peerChangePreRun(cmd *cobra.Command, args []string) {
	if allPeers && len(args) > 0 {
		die("can't have both --all-peers and a list of peers")
	}
	mds = mustConnectToMDS()
	peers, err := mds.GetPeers()
	if err != nil {
		die("couldn't get peer list: %v", err)
	}
	var out torus.PeerInfoList
	for _, arg := range args {
		found := false
		for _, p := range peers {
			if p.Address != "" {
				if p.Address == arg {
					out = out.Union(torus.PeerInfoList{p})
					found = true
				} else if p.UUID == arg {
					out = out.Union(torus.PeerInfoList{p})
					found = true
				}
			}
		}
		if !found {
			if !force {
				die("peer %s not currently healthy. To remove, use `--force`", arg)
			}
			out = out.Union(torus.PeerInfoList{&models.PeerInfo{
				UUID: arg,
			}})
		}
	}
	if allPeers {
		for _, p := range peers {
			if p.Address != "" {
				out = out.Union(torus.PeerInfoList{p})
			}
		}
	}
	newPeers = out
}

func peerAddAction(cmd *cobra.Command, args []string) {
	if !allPeers && len(args) == 0 {
		die("need to specify one of peer's address, uuid or --all-peers")
	}
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	var newRing torus.Ring
	if r, ok := currentRing.(torus.RingAdder); ok {
		newRing, err = r.AddPeers(newPeers)
	} else {
		die("current ring type cannot support adding")
	}
	if err != nil {
		die("couldn't add peer to ring: %v", err)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		die("couldn't set new ring: %v", err)
	}
}

func peerRemoveAction(cmd *cobra.Command, args []string) {
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	var newRing torus.Ring
	if r, ok := currentRing.(torus.RingRemover); ok {
		newRing, err = r.RemovePeers(newPeers.PeerList())
	} else {
		die("current ring type cannot support removal")
	}
	if err != nil {
		die("couldn't remove peer from ring: %v", err)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		die("couldn't set new ring: %v", err)
	}
}

func listPeersAction(cmd *cobra.Command, args []string) {
	var totalStorage uint64
	var usedStorage uint64

	mds := mustConnectToMDS()
	gmd := mds.GlobalMetadata()
	peers, err := mds.GetPeers()
	if err != nil {
		die("couldn't get peers: %v", err)
	}
	ring, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	members := ring.Members()
	table := NewTableWriter(os.Stdout)
	table.SetHeader([]string{"Address", "UUID", "Size", "Used", "Member", "Updated", "Reb/Rep Data"})
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
			bytesOrIbytes(x.TotalBlocks*gmd.BlockSize, outputAsSI),
			bytesOrIbytes(x.UsedBlocks*gmd.BlockSize, outputAsSI),
			ringStatus,
			humanize.Time(time.Unix(0, x.LastSeen)),
			bytesOrIbytes(x.RebalanceInfo.LastRebalanceBlocks*gmd.BlockSize*uint64(time.Second)/uint64(x.LastSeen+1-x.RebalanceInfo.LastRebalanceFinish), outputAsSI) + "/sec",
		})
		if x.RebalanceInfo.Rebalancing {
			rebalancing = true
		}
		totalStorage += x.TotalBlocks * gmd.BlockSize
		usedStorage += x.UsedBlocks * gmd.BlockSize
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
	if outputAsCSV {
		table.RenderCSV()
	} else {
		table.Render()
		fmt.Printf("Balanced: %v Usage: %5.2f%%\n", !rebalancing, (float64(usedStorage) / float64(totalStorage) * 100.0))
	}
}
