package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/coreos/torus"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/coreos/torus/models"
	"github.com/coreos/torus/ring"
	"github.com/spf13/cobra"
)

var (
	ringType  string
	peers     torus.PeerInfoList
	uuids     []string
	allUUIDs  bool
	repFactor int
	mds       torus.MetadataService
)

var ringCommand = &cobra.Command{
	Use:   "ring",
	Short: "modify the ring of the cluster (ADVANCED)",
	Run:   ringAction,
}

var ringChangeReplicationCommand = &cobra.Command{
	Use:   "set-replication AMOUNT",
	Short: "set the replication count for the cluster",
	Run:   ringChangeReplicationAction,
}

var ringChangeCommand = &cobra.Command{
	Use:    "manual-change",
	Short:  "apply a new ring to the cluster",
	PreRun: ringChangePreRun,
	Run:    ringChangeAction,
}

var ringGetCommand = &cobra.Command{
	Use:   "get",
	Short: "get the ring from the cluster",
	Run:   ringGetAction,
}

func init() {
	ringCommand.AddCommand(ringChangeReplicationCommand)
	ringCommand.AddCommand(ringChangeCommand)
	ringCommand.AddCommand(ringGetCommand)
	ringChangeCommand.Flags().StringSliceVar(&uuids, "uuids", []string{}, "uuids to incorporate in the ring")
	ringChangeCommand.Flags().BoolVar(&allUUIDs, "all-peers", false, "use all peers in the ring")
	ringChangeCommand.Flags().StringVar(&ringType, "type", "single", "type of ring to create")
	ringChangeCommand.Flags().IntVarP(&repFactor, "replication", "r", 2, "number of replicas")
}

func ringAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	fmt.Print("\n\tOne ring to rule them all, one ring to find them,\n\tOne ring to bring them all and in the darkness bind them\n\n")
	os.Exit(1)
}

func ringGetAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	ring, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	fmt.Println(ring.Describe())
}

func ringChangeAction(cmd *cobra.Command, args []string) {
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	var newRing torus.Ring
	switch ringType {
	case "empty":
		newRing, err = ring.CreateRing(&models.Ring{
			Type:    uint32(ring.Empty),
			Version: uint32(currentRing.Version() + 1),
		})
	case "single":
		newRing, err = ring.CreateRing(&models.Ring{
			Type:    uint32(ring.Single),
			Peers:   peers,
			Version: uint32(currentRing.Version() + 1),
		})
	case "mod":
		newRing, err = ring.CreateRing(&models.Ring{
			Type:              uint32(ring.Mod),
			Peers:             peers,
			ReplicationFactor: uint32(repFactor),
			Version:           uint32(currentRing.Version() + 1),
		})
	case "ketama":
		newRing, err = ring.CreateRing(&models.Ring{
			Type:              uint32(ring.Ketama),
			Peers:             peers,
			ReplicationFactor: uint32(repFactor),
			Version:           uint32(currentRing.Version() + 1),
		})
	default:
		panic("still unknown ring type")
	}
	if err != nil {
		die("couldn't create new ring: %v", err)
	}
	cfg := flagconfig.BuildConfigFromFlags()
	err = torus.SetRing("etcd", cfg, newRing)
	if err != nil {
		die("couldn't set new ring: %v", err)
	}
}

func ringChangePreRun(cmd *cobra.Command, args []string) {
	mds = mustConnectToMDS()
	currentPeers, err := mds.GetPeers()
	if allUUIDs {
		if allUUIDs && len(uuids) != 0 {
			die("use only one of --uuids or --all-peers")
		}
		if err != nil {
			die("couldn't get peer list: %v", err)
		}
		uuids = currentPeers.PeerList()
	}
	for _, p := range currentPeers {
		if p.Address == "" {
			continue
		}
		for _, x := range uuids {
			if p.UUID == x {
				peers = append(peers, p)
			}
		}
	}
	switch ringType {
	case "empty":
		uuids = nil
		return
	case "single":
		if len(peers) != 1 {
			die("require one uuid (use --uuids)\n")
		}
		return
	case "mod":
		if len(peers) == 0 {
			die("need one of --uuids or --all-peers")
		}
	case "ketama":
		if len(peers) == 0 {
			die("need one of --uuids or --all-peers")
		}
	default:
		die(`invalid ring type %s (try "empty", "mod" or "single")`, ringType)
	}
}

func ringChangeReplicationAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	amount, err := strconv.Atoi(args[0])
	if err != nil {
		die("not an integer number of replicas: %s", args[0])
	}
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		die("couldn't get ring: %v", err)
	}
	var newRing torus.Ring
	if r, ok := currentRing.(torus.ModifyableRing); ok {
		newRing, err = r.ChangeReplication(amount)
	} else {
		die("current ring type cannot support changing the replication amount")
	}
	if err != nil {
		die("couldn't change replication amount: %v", err)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		die("couldn't set new ring: %v", err)
	}
}
