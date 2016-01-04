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
	ringType string
	uuids    []string
	allUUIDs bool
	mds      agro.MetadataService
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
		fmt.Fprintf(os.Stderr, "couldn't get ring: %s\n", err)
		os.Exit(1)
	}
	fmt.Println(ring.Describe())
}

func ringChangeAction(cmd *cobra.Command, args []string) {
	if mds == nil {
		mds = mustConnectToMDS()
	}
	currentRing, err := mds.GetRing()
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't get ring: %s\n", err)
		os.Exit(1)
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
			UUIDs:   uuids,
			Version: uint32(currentRing.Version() + 1),
		})
	case "mod":
		newRing, err = ring.CreateRing(&models.Ring{
			Type:    uint32(ring.Mod),
			UUIDs:   uuids,
			Version: uint32(currentRing.Version() + 1),
		})
	default:
		panic("still unknown ring type")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't create new ring: %s\n", err)
		os.Exit(1)
	}
	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	err = agro.SetRing("etcd", cfg, newRing)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't set new ring: %s\n", err)
		os.Exit(1)
	}
}

func ringChangePreRun(cmd *cobra.Command, args []string) {
	if allUUIDs {
		if allUUIDs && len(uuids) != 0 {
			fmt.Fprint(os.Stderr, "use only one of --uuids or --all-peers")
			os.Exit(1)
		}
		mds = mustConnectToMDS()
		peers, err := mds.GetPeers()
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't get peer list: %s\n", err)
			os.Exit(1)
		}
		for _, p := range peers {
			if p.Address != "" {
				uuids = append(uuids, p.UUID)
			}
		}
	}
	switch ringType {
	case "empty":
		uuids = nil
		return
	case "single":
		if len(uuids) != 1 {
			fmt.Fprint(os.Stderr, "require one uuid (use --uuids)\n")
			os.Exit(1)
		}
		return
	case "mod":
		if len(uuids) == 0 {
			fmt.Fprint(os.Stderr, "need one of --uuids or --all-peers")
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "invalid ring type %s (try \"empty\", \"mod\" or \"single\")", ringType)
		os.Exit(1)
	}
}
