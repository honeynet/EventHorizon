package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricEventSocketPath = "/tmp/tarpit_exporter.sock"

func main() {
	metricState := newMetricEventMetrics(prometheus.DefaultRegisterer)
	if _, err := startMetricEventServer(metricEventSocketPath, metricState); err != nil {
		log.Fatal("Metric event socket error: ", err)
	}

	http.Handle("/metrics", promhttp.Handler())
	log.Println("Metrics available at :9101/metrics")
	log.Fatal(http.ListenAndServe(":9101", nil))
}
