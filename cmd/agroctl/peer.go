package main

import (
	"fmt"
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
		fmt.Fprintf(os.Stderr, "couldn't get peer list: %s\n", err)
		os.Exit(1)
	}
	if allPeers && len(args) > 0 {
		fmt.Fprintf(os.Stderr, "can't have both --all-peers and a list of peers")
		os.Exit(1)
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
			fmt.Fprintf(os.Stderr, "peer %s not currently healthy\n", err)
			os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "couldn't get ring: %s\n", err)
		os.Exit(1)
	}
	var newRing agro.Ring
	if r, ok := currentRing.(agro.RingAdder); ok {
		newRing, err = r.AddPeers(newPeers)
	} else {
		fmt.Fprintf(os.Stderr, "current ring type cannot support adding")
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't add peer to ring: %s\n", err)
		os.Exit(1)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't set new ring: %s\n", err)
		os.Exit(1)
	}
}

func peerRemoveAction(cmd *cobra.Command, args []string) {
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't get ring: %s\n", err)
		os.Exit(1)
	}
	var newRing agro.Ring
	if r, ok := currentRing.(agro.RingRemover); ok {
		newRing, err = r.RemovePeers(newPeers.PeerList())
	} else {
		fmt.Fprintf(os.Stderr, "current ring type cannot support removal")
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't add peer to ring: %s\n", err)
		os.Exit(1)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't set new ring: %s\n", err)
		os.Exit(1)
	}
}
