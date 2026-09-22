// Package metrics holds the Prometheus instrumentation of the service.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics bundles the collectors used by the ingest path and the worker pool.
type Metrics struct {
	reg *prometheus.Registry

	Accepted     prometheus.Counter
	Dropped      prometheus.Counter
	RowsWritten  prometheus.Counter
	WriteErrors  prometheus.Counter
	BatchLatency prometheus.Histogram
	BatchSize    prometheus.Histogram
}

// New creates the collectors and registers them on reg.
func New(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		reg: reg,
		Accepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logengine_logs_accepted_total",
			Help: "Log records accepted into the ingest queue.",
		}),
		Dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logengine_logs_dropped_total",
			Help: "Log records rejected because the ingest queue was full or closed.",
		}),
		RowsWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logengine_rows_written_total",
			Help: "Log records successfully written to Postgres.",
		}),
		WriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logengine_batch_write_errors_total",
			Help: "Batches that failed to be written to Postgres.",
		}),
		BatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "logengine_batch_write_duration_seconds",
			Help:    "Time spent writing one batch to Postgres.",
			Buckets: prometheus.DefBuckets,
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "logengine_batch_size_rows",
			Help:    "Number of rows per flushed batch.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 5000},
		}),
	}
	reg.MustRegister(m.Accepted, m.Dropped, m.RowsWritten, m.WriteErrors, m.BatchLatency, m.BatchSize)
	return m
}

// RegisterQueueDepth publishes the current ingest queue depth using depth as
// the source of truth.
func (m *Metrics) RegisterQueueDepth(depth func() int) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "logengine_queue_depth",
		Help: "Number of log records currently buffered in the ingest queue.",
	}, func() float64 { return float64(depth()) }))
}
