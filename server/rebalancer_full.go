package server

import (
	"encoding/json"
	"errors"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
)

func init() {
	rebalancerRegistry[Full] = func(d *distributor, newRing agro.Ring) Rebalancer {
		return &full{
			d:       d,
			oldRing: d.ring,
			newRing: newRing,
		}
	}
}

type full struct {
	d        *distributor
	oldRing  agro.Ring
	newRing  agro.Ring
	locked   bool
	replaced bool
	state    map[string]agro.INodeID
	store    *unionStorage
}

func (f *full) leaderMessage(phase int32) *models.RebalanceStatus {
	s := f.makeMessage(phase)
	s.FromLeader = true
	return s
}

func (f *full) makeMessage(phase int32) *models.RebalanceStatus {
	return &models.RebalanceStatus{
		RebalanceType: int32(Full),
		Phase:         phase,
		UUID:          f.d.UUID(),
	}
}

func (f *full) abort(out chan *models.RebalanceStatus, phase int32, err error) {
	rlog.Error(err)
	rlog.Errorf("state %d, safe to abort", phase)
	out <- f.leaderMessage(-1)
	f.Timeout()
}

func (f *full) Leader(inOut [2]chan *models.RebalanceStatus) {
	rlog.Info("full: starting full rebalance")
	in, out := inOut[0], inOut[1]
	// Phase 1: Everyone stop printing inodes.
	out <- f.leaderMessage(1)
	f.doState(1)
	waitAll(in, f.newRing, 1)
	// Phase 2: Capture the inode map, save it as the rebalance state.
	//          Everyone else grabs this, injects, and goes again.
	rlog.Info("full: capturing inode map")
	state, err := f.d.srv.mds.GetINodeIndexes()
	if err != nil {
		f.abort(out, 1, err)
		return
	}
	b, err := json.Marshal(state)
	if err != nil {
		f.abort(out, 1, err)
		return
	}
	err = f.d.srv.mds.SetRebalanceSnapshot(uint64(Replace), b)
	out <- f.leaderMessage(2)
	f.doState(2)
	waitAll(in, f.newRing, 2)
	// Phase 3: Begin copying blocks! (Until complete)
	rlog.Info("full: copying blocks")
	out <- f.leaderMessage(3)
	f.doState(3)
	waitAll(in, f.newRing, 3)
	// Phase 4: Begin copying inodes! (Until complete)
	rlog.Info("full: copying inodes")
	out <- f.leaderMessage(4)
	f.doState(4)
	waitAll(in, f.newRing, 4)
	// Phase 5: Replace the ring and remove self from storage
	rlog.Info("full: finishing")
	out <- f.leaderMessage(5)
	f.doState(5)
	waitAll(in, f.newRing, 5)
}

func (f *full) AdvanceState(s *models.RebalanceStatus) (*models.RebalanceStatus, bool, error) {
	rlog.Debugf("full: follower, starting phase %d", s.Phase)
	err := f.doState(s.Phase)
	rlog.Debugf("full: follower, finished phase %d", s.Phase)
	if err != nil {
		// Abort
		rlog.Error(err)
		f.Timeout()
		return nil, true, err
	}
	if s.Phase == 5 {
		return f.makeMessage(s.Phase), true, nil
	}
	return f.makeMessage(s.Phase), false, nil
}

func (f *full) doState(phase int32) error {
	switch phase {
	case 1:
		f.d.srv.gc.Stop()
		f.d.srv.writeableLock.Lock()
		f.locked = true
	case 2:
		f.d.srv.writeableLock.Unlock()
		f.locked = false
		_, b, err := f.d.srv.mds.GetRebalanceSnapshot()
		if err != nil {
			return err
		}
		err = json.Unmarshal(b, &f.state)
		if err != nil {
			return err
		}
		gmd, err := f.d.srv.mds.GlobalMetadata()
		if err != nil {
			return err
		}
		newBlock, err := agro.CreateBlockStore(f.d.blocks.Kind(), "rebalance", f.d.srv.cfg, gmd)
		if err != nil {
			return err
		}
		newINode, err := agro.CreateINodeStore(f.d.inodes.Kind(), "rebalance", f.d.srv.cfg)
		if err != nil {
			return err
		}
		f.d.mut.Lock()
		defer f.d.mut.Unlock()
		f.store = &unionStorage{
			oldBlock: f.d.blocks,
			newBlock: newBlock,
			oldINode: f.d.inodes,
			newINode: newINode,
		}
		f.d.blocks = f.store
		f.d.inodes = f.store
		f.d.ring = ring.NewUnionRing(f.d.ring, f.newRing)
		f.replaced = true
	case 3:
		err := f.sendAllBlocks()
		if err != nil {
			return err
		}
	case 4:
		err := f.sendAllINodes()
		if err != nil {
			return err
		}
	case 5:
		f.d.mut.Lock()
		defer f.d.mut.Unlock()
		blocks, err := f.store.oldBlock.ReplaceBlockStore(f.store.newBlock)
		if err != nil {
			return err
		}
		inodes, err := f.store.oldINode.ReplaceINodeStore(f.store.newINode)
		if err != nil {
			return err
		}
		f.d.inodes = inodes
		f.d.blocks = blocks
		f.replaced = false
		f.d.srv.gc.Start()
	default:
		panic("incomplete state machine")
	}
	return nil
}

func (f *full) sendBlockCache(m map[string]*models.PutBlockRequest) error {
	for peer, pbr := range m {
		_, err := f.d.client.SendRebalance(context.TODO(), peer, &models.RebalanceRequest{
			Subrequest: &models.RebalanceRequest_PutBlockRequest{
				PutBlockRequest: pbr,
			},
			Phase: 3,
			UUID:  f.d.UUID(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *full) addToBlockCache(m map[string]*models.PutBlockRequest, ref agro.BlockRef, b []byte, uuid string) {
	if _, ok := m[uuid]; !ok {
		m[uuid] = &models.PutBlockRequest{}
	}
	m[uuid].Refs = append(m[uuid].Refs, ref.ToProto())
	m[uuid].Blocks = append(m[uuid].Blocks, b)
}

func (f *full) sendAllBlocks() error {
	ob := f.store.oldBlock
	it := ob.BlockIterator()
	cache := make(map[string]*models.PutBlockRequest)
	count := 0
	for it.Next() {
		if count == 100 {
			err := f.sendBlockCache(cache)
			if err != nil {
				return err
			}
			cache = make(map[string]*models.PutBlockRequest)
			count = 0
		}
		ref := it.BlockRef()
		bytes, err := ob.GetBlock(context.TODO(), ref)
		if err != nil {
			return err
		}
		newpeers, err := f.newRing.GetBlockPeers(ref)
		if err != nil {
			return err
		}
		oldpeers, err := f.oldRing.GetBlockPeers(ref)
		if err != nil {
			return err
		}
		myIndex := peerListHas(oldpeers, f.d.UUID())
		if peerListHas(newpeers, f.d.UUID()) != -1 {
			err := f.store.newBlock.WriteBlock(context.TODO(), ref, bytes)
			if err != nil {
				return err
			}
		}
		diffpeers := peerListAndNot(newpeers, oldpeers)
		if myIndex >= len(diffpeers) {
			// downsizing
			continue
		}
		if myIndex == len(oldpeers)-1 && len(diffpeers) > len(oldpeers) {
			for i := myIndex; i < len(diffpeers); i++ {
				p := diffpeers[i]
				f.addToBlockCache(cache, ref, bytes, p)
			}
		} else {
			f.addToBlockCache(cache, ref, bytes, diffpeers[myIndex])
		}
	}
	return f.sendBlockCache(cache)
}

func peerListHas(pl []string, uuid string) int {
	for i, x := range pl {
		if x == uuid {
			return i
		}
	}
	return -1
}

func peerListAndNot(a []string, b []string) []string {
	var out []string
	for _, x := range a {
		if peerListHas(b, x) == -1 {
			out = append(out, x)
		}
	}
	return out
}

func (f *full) OnError(err error) *models.RebalanceStatus {
	return nil
}

func (f *full) Timeout() {
	if f.locked {
		f.d.srv.writeableLock.Unlock()
		f.locked = false
	}
	if f.replaced {
		f.d.mut.Lock()
		defer f.d.mut.Unlock()
		f.d.blocks = f.store.oldBlock
		f.d.inodes = f.store.oldINode
		f.d.ring = f.oldRing
	}
	return
}

func (f *full) RebalanceMessage(ctx context.Context, req *models.RebalanceRequest) (*models.RebalanceResponse, error) {
	resp := &models.RebalanceResponse{
		Ok: true,
	}
	switch req.Phase {
	case 3:
		br := req.GetPutBlockRequest()
		for i, ref := range br.Refs {
			err := f.store.newBlock.WriteBlock(ctx, agro.BlockFromProto(ref), br.Blocks[i])
			if err != nil {
				return nil, err
			}
		}
	case 4:
		ir := req.GetPutINodeRequest()
		for i, ref := range ir.Refs {
			err := f.store.newINode.WriteINode(ctx, agro.INodeFromProto(ref), ir.INodes[i])
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.New("wrong rebalance phase")
	}
	return resp, nil
}

func (f *full) sendINodeCache(m map[string]*models.PutINodeRequest) error {
	for peer, pbr := range m {
		_, err := f.d.client.SendRebalance(context.TODO(), peer, &models.RebalanceRequest{
			Subrequest: &models.RebalanceRequest_PutINodeRequest{
				PutINodeRequest: pbr,
			},
			Phase: 4,
			UUID:  f.d.UUID(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *full) addToINodeCache(m map[string]*models.PutINodeRequest, ref agro.INodeRef, b *models.INode, uuid string) {
	if _, ok := m[uuid]; !ok {
		m[uuid] = &models.PutINodeRequest{}
	}
	m[uuid].Refs = append(m[uuid].Refs, ref.ToProto())
	m[uuid].INodes = append(m[uuid].INodes, b)
}

func (f *full) sendAllINodes() error {
	ob := f.store.oldINode
	it := ob.INodeIterator()
	cache := make(map[string]*models.PutINodeRequest)
	count := 0
	for it.Next() {
		if count == 100 {
			err := f.sendINodeCache(cache)
			if err != nil {
				return err
			}
			cache = make(map[string]*models.PutINodeRequest)
			count = 0
		}
		ref := it.INodeRef()
		inode, err := ob.GetINode(context.TODO(), ref)
		if err != nil {
			return err
		}
		newpeers, err := f.newRing.GetINodePeers(ref)
		if err != nil {
			return err
		}
		oldpeers, err := f.oldRing.GetINodePeers(ref)
		if err != nil {
			return err
		}
		myIndex := peerListHas(oldpeers, f.d.UUID())
		if peerListHas(newpeers, f.d.UUID()) != -1 {
			err := f.store.newINode.WriteINode(context.TODO(), ref, inode)
			if err != nil {
				return err
			}
		}
		diffpeers := peerListAndNot(newpeers, oldpeers)
		if myIndex >= len(diffpeers) {
			// downsizing
			continue
		}
		if myIndex == len(oldpeers)-1 && len(diffpeers) > len(oldpeers) {
			for i := myIndex; i < len(diffpeers); i++ {
				p := diffpeers[i]
				f.addToINodeCache(cache, ref, inode, p)
			}
		} else {
			f.addToINodeCache(cache, ref, inode, diffpeers[myIndex])
		}
	}
	return f.sendINodeCache(cache)
}
