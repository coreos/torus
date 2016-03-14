package main

import (
	"fmt"
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/spf13/cobra"
)

var (
	ringType  string
	peers     agro.PeerInfoList
	uuids     []string
	allUUIDs  bool
	repFactor int
	mds       agro.MetadataService
)

var ringCommand = &cobra.Command{
	Use:   "ring",
	Short: "modify the ring of the cluster (ADVANCED)",
	Run:   ringAction,
}

var ringChangeCommand = &cobra.Command{
	Use:    "change",
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
	ringCommand.AddCommand(ringChangeCommand)
	ringCommand.AddCommand(ringGetCommand)
	ringChangeCommand.Flags().StringSliceVar(&uuids, "uuids", []string{}, "uuids to incorporate in the ring")
	ringChangeCommand.Flags().BoolVar(&allUUIDs, "all-peers", false, "use all peers in the ring")
	ringChangeCommand.Flags().StringVar(&ringType, "type", "single", "type of ring to create")
	ringChangeCommand.Flags().IntVarP(&repFactor, "replication", "r", 2, "type of ring to create")
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
	var newRing agro.Ring
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
	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	err = agro.SetRing("etcd", cfg, newRing)
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
