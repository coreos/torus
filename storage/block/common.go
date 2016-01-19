package block

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "block")

var (
	promBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "agro_storage_blocks",
		Help: "Gauge of number of blocks in local storage",
	})
	promBlocksAvail = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "agro_storage_blocks_total",
		Help: "Gauge of number of blocks available in local storage",
	})
	promBlocksRetrieved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_retrieved_blocks",
		Help: "Number of blocks returned from local block storage",
	})
	promBlocksFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_failed_blocks",
		Help: "Number of blocks failed to be returned from local block storage",
	})
	promBlocksWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_written_blocks",
		Help: "Number of blocks written to local block storage",
	})
	promBlockWritesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_failed_written_blocks",
		Help: "Number of blocks failed to be written to local block storage",
	})
	promBlocksDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_deleted_blocks",
		Help: "Number of blocks deleted from local block storage",
	})
	promBlockDeletesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_failed_deleted_blocks",
		Help: "Number of blocks failed to be deleted from local block storage",
	})
	promStorageFlushes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_storage_flushes",
		Help: "Number of times the storage layer is synced to disk",
	})
	promBytesPerBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "agro_storage_block_bytes",
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
