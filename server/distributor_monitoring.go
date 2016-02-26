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
	promDistRebalanceRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_rebalance_rpcs_total",
		Help: "Number of Rebalance RPCs made to this node",
	})
	promDistRebalanceRPCFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_rebalance_rpc_failures",
		Help: "Number of Rebalance RPCs with errors",
	})
	promDistReadaheadRPCs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_readahead_rpcs_total",
		Help: "Number of Readahead RPCs made to this node",
	})
	promDistReadaheadBlocks = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "agro_distributor_readahead_blocks",
		Help:    "Histogram of number of blocks optimistically sent from peers",
		Buckets: prometheus.LinearBuckets(1, 1, 20),
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
	// RPC
	prometheus.MustRegister(promDistPutBlockRPCs)
	prometheus.MustRegister(promDistPutBlockRPCFailures)
	prometheus.MustRegister(promDistBlockRPCs)
	prometheus.MustRegister(promDistBlockRPCFailures)
	prometheus.MustRegister(promDistRebalanceRPCs)
	prometheus.MustRegister(promDistRebalanceRPCFailures)
	prometheus.MustRegister(promDistReadaheadBlocks)
}
