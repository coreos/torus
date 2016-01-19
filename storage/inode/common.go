package inode

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "inode")

var (
	promINodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agro_storage_inodes",
		Help: "Gauge of number of inodes in local storage",
	}, []string{"inodestore"})
	promINodesRetrieved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_retrieved_inodes",
		Help: "Number of inodes returned",
	}, []string{"inodestore"})
	promINodesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_failed_inodes",
		Help: "Number of inodes failed to be returned",
	}, []string{"inodestore"})
	promINodesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_written_inodes",
		Help: "Number of inodes written",
	}, []string{"inodestore"})
	promINodeWritesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_failed_written_inodes",
		Help: "Number of inodes failed to be written",
	}, []string{"inodestore"})
	promINodesDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_deleted_inodes",
		Help: "Number of inodes deleted",
	}, []string{"inodestore"})
	promINodeDeletesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_storage_failed_deleted_inodes",
		Help: "Number of inodes failed to be deleted",
	}, []string{"inodestore"})
	promINodeFlushes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_inode_flushes",
		Help: "Number of times the inode layer is synced to disk",
	}, []string{"inodestore"})
)

func init() {
	prometheus.MustRegister(promINodes)
	prometheus.MustRegister(promINodesRetrieved)
	prometheus.MustRegister(promINodesFailed)
	prometheus.MustRegister(promINodesWritten)
	prometheus.MustRegister(promINodeWritesFailed)
	prometheus.MustRegister(promINodesDeleted)
	prometheus.MustRegister(promINodeDeletesFailed)
	prometheus.MustRegister(promINodeFlushes)
}
