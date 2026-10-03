package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ActiveMatches is a Gauge that goes up and down as rooms start and stop.
	ActiveMatches = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "multiplayer_active_matches_total",
			Help: "The current number of active game sessions running in memory.",
		},
	)

	// QueueSize is a Gauge tracking players waiting for a match.
	QueueSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "multiplayer_queue_size_total",
			Help: "The current number of players waiting for a match.",
		},
	)

	// HTTPRequestDuration is a Histogram that tracks API latency (e.g., P99 response times).
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Histogram of response latency (seconds) of HTTP requests.",
			// DefBuckets are standard latency buckets: 5ms, 10ms, 25ms, 50ms, 100ms, etc.
			Buckets: prometheus.DefBuckets, 
		},
		[]string{"method", "path", "status"},
	)
)

// Init registers all custom metrics with the default Prometheus registry.
// This must be called exactly once during server startup.
func Init() {
	prometheus.MustRegister(ActiveMatches)
	prometheus.MustRegister(QueueSize)
	prometheus.MustRegister(HTTPRequestDuration)
}