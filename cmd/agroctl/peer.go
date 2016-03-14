package main

import (
	"os"

	"github.com/coreos/agro"
	"github.com/spf13/cobra"
)

var (
	newPeers agro.PeerInfoList
	allPeers bool
)

var peerCommand = &cobra.Command{
	Use:   "peer",
	Short: "add/remove peers from the cluster",
	Run:   peerAction,
}

var peerAddCommand = &cobra.Command{
	Use:    "add",
	Short:  "add a peer to the cluster",
	PreRun: peerChangePreRun,
	Run:    peerAddAction,
}

var peerRemoveCommand = &cobra.Command{
	Use:    "remove",
	Short:  "remove a peer from the cluster",
	PreRun: peerChangePreRun,
	Run:    peerRemoveAction,
}

func init() {
	peerCommand.AddCommand(peerAddCommand, peerRemoveCommand)
	peerAddCommand.Flags().BoolVar(&allPeers, "all-peers", false, "add all peers")
}

func peerAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func peerChangePreRun(cmd *cobra.Command, args []string) {
	mds = mustConnectToMDS()
	peers, err := mds.GetPeers()
	if err != nil {
		die("couldn't get peer list: %v", err)
	}
	if allPeers && len(args) > 0 {
		die("can't have both --all-peers and a list of peers")
	}
	var out agro.PeerInfoList
	for _, arg := range args {
		found := false
		for _, p := range peers {
			if p.Address != "" {
				if p.Address == arg {
					out = out.Union(agro.PeerInfoList{p})
					found = true
				} else if p.UUID == arg {
					out = out.Union(agro.PeerInfoList{p})
					found = true
				}
			}
		}
		if !found {
			die("peer %s not currently healthy", arg)
		}
	}
	if allPeers {
		for _, p := range peers {
			if p.Address != "" {
				out = out.Union(agro.PeerInfoList{p})
			}
		}
	}
	newPeers = out
}

func peerAddAction(cmd *cobra.Command, args []string) {
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	var newRing agro.Ring
	if r, ok := currentRing.(agro.RingAdder); ok {
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
	var newRing agro.Ring
	if r, ok := currentRing.(agro.RingRemover); ok {
		newRing, err = r.RemovePeers(newPeers.PeerList())
	} else {
		die("current ring type cannot support removal")
	}
	if err != nil {
		die("couldn't add peer to ring: %v", err)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		die("couldn't set new ring: %v", err)
	}
}
