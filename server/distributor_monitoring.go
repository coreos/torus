package server

import "github.com/prometheus/client_golang/prometheus"

var (
	// Blocks
	promDistBlockRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_requests_total",
		Help: "Total number of blocks requested of the distributor layer",
	})
	promDistBlockCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_cached_blocks",
		Help: "Number of blocks returned from read cache of the distributor layer",
	})
	promDistBlockLocalHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_local_blocks",
		Help: "Number of blocks returned from local storage",
	})
	promDistBlockLocalFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_local_block_fails",
		Help: "Number of blocks requested from local storage that weren't found",
	})
	promDistBlockPeerHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_distributor_block_peer_blocks",
		Help: "Number of blocks returned from another peer in the cluster",
	}, []string{"peer"})
	promDistBlockPeerFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_distributor_block_peer_block_fails",
		Help: "Number of failures incurred in retrieving a block from a peer",
	}, []string{"peer"})
	promDistBlockFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_request_failures",
		Help: "Number of failed block requests",
	})
	// INodes
	promDistINodeRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_requests_total",
		Help: "Total number of inodes requested of the distributor layer",
	})
	promDistINodeCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_cached_blocks",
		Help: "Number of inodes returned from read cache of the distributor layer",
	})
	promDistINodeLocalHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_local_blocks",
		Help: "Number of inodes returned from local storage",
	})
	promDistINodeLocalFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_local_block_fails",
		Help: "Number of inodes requested from local storage that weren't found",
	})
	promDistINodePeerHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_distributor_inode_peer_blocks",
		Help: "Number of inodes returned from another peer in the cluster",
	}, []string{"peer"})
	promDistINodePeerFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_distributor_inode_peer_block_fails",
		Help: "Number of failures incurred in retrieving a inode from a peer",
	}, []string{"peer"})
	promDistINodeFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_request_failures",
		Help: "Number of failed inode requests",
	})
	// RPCs
	promDistPutBlockRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_put_block_rpcs_total",
		Help: "Number of PutBlock RPCs made to this node",
	})
	promDistPutBlockRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_put_block_rpc_failures",
		Help: "Number of PutBlock RPCs with errors",
	})
	promDistBlockRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_rpcs_total",
		Help: "Number of PutBlock RPCs made to this node",
	})
	promDistBlockRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_block_rpc_failures",
		Help: "Number of PutBlock RPCs with errors",
	})
	promDistPutINodeRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_put_inode_rpcs_total",
		Help: "Number of PutINode RPCs made to this node",
	})
	promDistPutINodeRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_put_inode_rpc_failures",
		Help: "Number of PutINode RPCs with errors",
	})
	promDistINodeRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_rpcs_total",
		Help: "Number of INode RPCs made to this node",
	})
	promDistINodeRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_rpc_failures",
		Help: "Number of INode RPCs with errors",
	})
	promDistRebalanceRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_rebalance_rpcs_total",
		Help: "Number of Rebalance RPCs made to this node",
	})
	promDistRebalanceRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_rebalance_rpc_failures",
		Help: "Number of Rebalance RPCs with errors",
	})
)

func init() {
	// Block
	prometheus.MustRegister(promDistBlockRequests)
	prometheus.MustRegister(promDistBlockCacheHits)
	prometheus.MustRegister(promDistBlockLocalHits)
	prometheus.MustRegister(promDistBlockLocalFailures)
	prometheus.MustRegister(promDistBlockPeerHits)
	prometheus.MustRegister(promDistBlockPeerFailures)
	prometheus.MustRegister(promDistBlockFailures)
	// INode
	prometheus.MustRegister(promDistINodeRequests)
	prometheus.MustRegister(promDistINodeCacheHits)
	prometheus.MustRegister(promDistINodeLocalHits)
	prometheus.MustRegister(promDistINodeLocalFailures)
	prometheus.MustRegister(promDistINodePeerHits)
	prometheus.MustRegister(promDistINodePeerFailures)
	prometheus.MustRegister(promDistINodeFailures)
	// RPC
	prometheus.MustRegister(promDistPutBlockRPCs)
	prometheus.MustRegister(promDistPutBlockRPCFailures)
	prometheus.MustRegister(promDistBlockRPCs)
	prometheus.MustRegister(promDistBlockRPCFailures)
	prometheus.MustRegister(promDistPutINodeRPCs)
	prometheus.MustRegister(promDistPutINodeRPCFailures)
	prometheus.MustRegister(promDistINodeRPCs)
	prometheus.MustRegister(promDistINodeRPCFailures)
	prometheus.MustRegister(promDistRebalanceRPCs)
	prometheus.MustRegister(promDistRebalanceRPCFailures)
}
