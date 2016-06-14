// storage is the package which implements the underlying, on-disk storage
// API for Torus servers. A single node can be tested with just a storage
// implementation, but the distributor replaces it as a virtual implementation
// of a much larger storage pool, provided by the cluster. This is storage
// underneath a single node.
package storage

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "storage")

var (
	promBlocks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "torus_storage_blocks",
		Help: "Gauge of number of blocks in local storage",
	}, []string{"storage"})
	promBlocksAvail = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "torus_storage_blocks_total",
		Help: "Gauge of number of blocks available in local storage",
	}, []string{"storage"})
	promBlocksRetrieved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_read_blocks",
		Help: "Number of blocks returned from local block storage",
	}, []string{"storage"})
	promBlocksFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_failed_blocks",
		Help: "Number of blocks failed to be returned from local block storage",
	}, []string{"storage"})
	promBlocksWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_written_blocks",
		Help: "Number of blocks written to local block storage",
	}, []string{"storage"})
	promBlockWritesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_failed_written_blocks",
		Help: "Number of blocks failed to be written to local block storage",
	}, []string{"storage"})
	promBlocksDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_deleted_blocks",
		Help: "Number of blocks deleted from local block storage",
	}, []string{"storage"})
	promBlockDeletesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_failed_deleted_blocks",
		Help: "Number of blocks failed to be deleted from local block storage",
	}, []string{"storage"})
	promStorageFlushes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_storage_flushes",
		Help: "Number of times the storage layer is synced to disk",
	}, []string{"storage"})
	promBytesPerBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "torus_storage_block_bytes",
		Help: "Number of bytes per block in the storage layer",
	})
)

func init() {
	prometheus.MustRegister(promBlocks)
	prometheus.MustRegister(promBlocksAvail)
	prometheus.MustRegister(promBlocksRetrieved)
	prometheus.MustRegister(promBlocksFailed)
	prometheus.MustRegister(promBlocksWritten)
	prometheus.MustRegister(promBlockWritesFailed)
	prometheus.MustRegister(promBlocksDeleted)
	prometheus.MustRegister(promBlockDeletesFailed)
	prometheus.MustRegister(promStorageFlushes)
	prometheus.MustRegister(promBytesPerBlock)
}
