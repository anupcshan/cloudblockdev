package cloudblock

import "github.com/prometheus/client_golang/prometheus"

// Prometheus metrics for cloudblock package.
//
// These metrics track non-fatal assertion violations that were handled gracefully
// (e.g., by falling back to S3 when cache read fails). They are useful for:
// - Production monitoring: detect issues that don't cause failures but indicate problems
// - Testing: assert that no violations occurred during test execution

var (
	// AssertionViolations counts non-fatal assertion violations.
	// Labels:
	//   - subsystem: the component where the violation occurred (e.g., "cache", "index", "s3")
	//   - violation: the specific violation (e.g., "truncated_data", "checksum_mismatch")
	AssertionViolations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cloudblockdev",
			Name:      "assertion_violations_total",
			Help:      "Non-fatal assertion violations that were handled gracefully",
		},
		[]string{"subsystem", "violation"},
	)

	// SealsTotal counts buffer seal events by trigger type.
	// Labels:
	//   - trigger: what caused the seal (flush, fua, large_write, threshold_size, threshold_blocks)
	SealsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cloudblockdev",
			Name:      "seals_total",
			Help:      "Buffer seal events by trigger type",
		},
		[]string{"trigger"},
	)

	// OutstandingWriteBytes tracks the total bytes of sealed data awaiting upload.
	OutstandingWriteBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "cloudblockdev",
			Name:      "outstanding_write_bytes",
			Help:      "Bytes of sealed data awaiting upload to S3",
		},
	)
)

func init() {
	prometheus.MustRegister(AssertionViolations)
	prometheus.MustRegister(SealsTotal)
	prometheus.MustRegister(OutstandingWriteBytes)
}

// RecordAssertionViolation increments the assertion violation counter.
// Use this when a non-fatal issue is detected and handled (e.g., fallback to S3).
func RecordAssertionViolation(subsystem, violationType string) {
	AssertionViolations.WithLabelValues(subsystem, violationType).Inc()
}

// Seal trigger types
const (
	SealTriggerFlush           = "flush"
	SealTriggerFUA             = "fua"
	SealTriggerLargeWrite      = "large_write"
	SealTriggerThresholdSize   = "threshold_size"
	SealTriggerThresholdBlocks = "threshold_blocks"
)
