package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "ringtool")

var (
	ringType       = flag.String("ring", "mod", "Ring Type")
	replication    = flag.Int("rep", 2, "Start Replication")
	replicationEnd = flag.Int("repEnd", 0, "Target Replication (0 = same as start)")
	nodes          = flag.Int("nodes", 0, "Number of nodes to start")
	delta          = flag.Int("delta", 2, "Number of nodes to add (positive)/remove (negative)")
	blockSizeStr   = flag.String("block-size", "256KiB", "Blocksize")
	totalDataStr   = flag.String("total-data", "1TiB", "Total data simulated")
	partition      = flag.Int("rewrite-edge", 40, "Percentage of files with small writes")
	blockSize      uint64
	totalData      uint64
	uuids          []string
)

var maxIterations = 30

type ClusterState map[string][]agro.BlockRef

type RebalanceStats struct {
	BlocksKept uint64
	BlocksSent uint64
}

func main() {
	var err error
	flag.Parse()
	if *replicationEnd == 0 {
		*replicationEnd = *replication
	}
	nPeers := *nodes + *delta
	if *delta <= 0 {
		nPeers = *nodes
	}
	uuids = make([]string, nPeers)
	for i := 0; i < nPeers; i++ {
		uuids[i], _ = metadata.MakeOrGetUUID("")
	}
	blockSize, err = humanize.ParseBytes(*blockSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-size: %s\n", err)
		os.Exit(1)
	}
	totalData, err = humanize.ParseBytes(*totalDataStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing total-data: %s\n", err)
		os.Exit(1)
	}
	nblocks := totalData / blockSize
	var blocks []agro.BlockRef
	inode := agro.INodeID(1)
	part := float64(*partition) / 100.0
	for len(blocks) < int(nblocks) {
		perFile := rand.Intn(1000) + 1
		f := rand.NormFloat64()
		var out []agro.BlockRef
		if f < part {
			out, inode = generateRewrittenFile(agro.VolumeID(1), inode, perFile)
		} else {
			out, inode = generateLinearFile(agro.VolumeID(1), inode, perFile)
		}
		blocks = append(blocks, out...)
	}
	r1, r2 := createRings()
	fmt.Printf("Unique blocks: %d\n", len(blocks))
	cluster := assignData(blocks, r1)
	fmt.Println("@START *****")
	cluster.printBalance()
	newc, rebalance := cluster.Rebalance(r1, r2)
	fmt.Println("@END *****")
	newc.printBalance()
	fmt.Println("Changes:")
	rebalance.printStats()
}

func createRings() (agro.Ring, agro.Ring) {
	ftype, ok := ring.RingTypeFromString(*ringType)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown ring type: %s\n", *ringType)
		os.Exit(1)
	}
	from, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ftype),
		Version:           1,
		ReplicationFactor: uint32(*replication),
		UUIDs:             uuids[:*nodes],
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating from-ring: %s\n", err)
		os.Exit(1)
	}

	if v, ok := from.(agro.RingAdder); *delta > 0 && ok {
		to, err := v.AddPeers(agro.PeerList(uuids[*nodes:]), ring.ReplicationLevel(*replicationEnd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error adding peers to ring: %s\n", err)
			os.Exit(1)
		}
		return from, to
	}
	if v, ok := from.(agro.RingRemover); *delta <= 0 && ok {
		to, err := v.RemovePeers(agro.PeerList(uuids[*nodes+*delta:]), ring.ReplicationLevel(*replicationEnd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error removing peers from ring: %s\n", err)
			os.Exit(1)
		}
		return from, to
	}

	ttype, ok := ring.RingTypeFromString(*ringType)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown ring type: %s\n", *ringType)
		os.Exit(1)
	}
	to, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ttype),
		Version:           2,
		ReplicationFactor: uint32(*replicationEnd),
		UUIDs:             uuids[:(*nodes + *delta)],
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating from-ring: %s\n", err)
		os.Exit(1)
	}
	return from, to
}

func assignData(blocks []agro.BlockRef, r agro.Ring) ClusterState {
	out := make(map[string][]agro.BlockRef)
	for _, p := range r.Members() {
		out[p] = make([]agro.BlockRef, 0)
	}
	for _, b := range blocks {
		peers, err := r.GetPeers(b)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error in the ring: %s\n", err)
			os.Exit(1)
		}
		for _, p := range peers.Peers[:peers.Replication] {
			out[p] = append(out[p], b)
		}
	}
	return out
}

func (c ClusterState) printBalance() {
	fmt.Println("Balance:")
	total := 0
	for p, l := range c {
		fmt.Printf("\t%s: %d\n", p, len(l))
		total += len(l)
	}
	mean := float64(total) / float64(len(c))
	v := float64(0)
	for _, l := range c {
		v += math.Pow(float64(len(l))-mean, 2.0)
	}
	v = math.Sqrt(v / float64(len(c)))
	//	fmt.Printf("Total: %d, Mean: %0.2f, Stddev: %0.4f\n", total, mean, v)
	fmt.Printf("Total: %s, Mean: %s, Stddev: %s\n",
		humanize.IBytes(uint64(total)*blockSize),
		humanize.IBytes(uint64(mean)*blockSize),
		humanize.IBytes(uint64(v)*blockSize),
	)
}

func (c ClusterState) Rebalance(oldRing, newRing agro.Ring) (ClusterState, RebalanceStats) {
	var stats RebalanceStats
	out := make(map[string][]agro.BlockRef)
	for _, p := range newRing.Members() {
		out[p] = make([]agro.BlockRef, 0)
	}
	for p, l := range c {
		for _, ref := range l {
			newp, err := newRing.GetPeers(ref)
			newpeers := newp.Peers[:newp.Replication]
			if err != nil {
				fmt.Fprintf(os.Stderr, "error in the new ring: %s\n", err)
				os.Exit(1)
			}
			oldp, err := oldRing.GetPeers(ref)
			oldpeers := oldp.Peers[:oldp.Replication]
			if err != nil {
				fmt.Fprintf(os.Stderr, "error in the old ring: %s\n", err)
				os.Exit(1)
			}
			myIndex := oldpeers.IndexAt(p)
			if newpeers.Has(p) {
				out[p] = append(out[p], ref)
				stats.BlocksKept++
			}
			diffpeers := newpeers.AndNot(oldpeers)
			if myIndex >= len(diffpeers) {
				// downsizing
				continue
			}
			if myIndex == len(oldpeers)-1 && len(diffpeers) > len(oldpeers) {
				for i := myIndex; i < len(diffpeers); i++ {
					p := diffpeers[i]
					out[p] = append(out[p], ref)
					stats.BlocksSent++
				}
			} else {
				p := diffpeers[myIndex]
				out[p] = append(out[p], ref)
				stats.BlocksSent++
			}
		}
	}
	return out, stats
}

func (s RebalanceStats) printStats() {
	fmt.Printf("Blocks Kept: %d\n", s.BlocksKept)
	fmt.Printf("Blocks Sent: %d\n", s.BlocksSent)
	fmt.Printf("Percentage Sent: %0.2f\n", ((float64(s.BlocksSent) * 100) / (float64(s.BlocksSent + s.BlocksKept))))
	fmt.Printf("Network Traffic: %s\n", humanize.IBytes(s.BlocksSent*blockSize))
	total := float64((s.BlocksSent + s.BlocksKept) * blockSize)
	perfect := total * math.Abs(float64(*delta)/float64(*delta+*nodes))
	fmt.Printf("Perfect Traffic: %s\n", humanize.IBytes(uint64(perfect)))
}

func generateLinearFile(vol agro.VolumeID, in agro.INodeID, size int) ([]agro.BlockRef, agro.INodeID) {
	var out []agro.BlockRef
	for x := 1; x <= size; x++ {
		out = append(out, agro.BlockRef{
			INodeRef: agro.NewINodeRef(vol, in),
			Index:    agro.IndexID(x),
		})
	}
	return out, in + 1
}

func generateRewrittenFile(vol agro.VolumeID, inStart agro.INodeID, size int) ([]agro.BlockRef, agro.INodeID) {
	file, inode := generateLinearFile(vol, inStart, size)
	its := rand.Intn(maxIterations)
	for i := 0; i < its; i++ {
		off := rand.Intn(len(file))
		len := rand.Intn(len(file) - off)
		piece, in := generateLinearFile(vol, inode, len)
		inode = in
		copy(file[off:], piece)
	}
	return file, inode
}
