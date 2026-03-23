package main

import (
	// "bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"

	// "github.com/oschwald/geoip2-golang"
	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	totalConnects    *prometheus.CounterVec
	totalTrappedTime *prometheus.CounterVec
	activeClients    *prometheus.GaugeVec
	clients          *prometheus.CounterVec

	upnpOtherHttpRequests  *prometheus.CounterVec
	upnpMSearchRequests    *prometheus.CounterVec
	upnpNonMSearchRequests *prometheus.CounterVec

	mqttMalformedConnect prometheus.Counter
	mqttConnectVersions  *prometheus.CounterVec
	mqttSubscribeTopics  *prometheus.CounterVec
	mqttCredentials      *prometheus.CounterVec
	mqttPayloadCaptured  *prometheus.CounterVec
	mqttPublishTopics    *prometheus.CounterVec
	mqttConacks          prometheus.Counter
	mqttUnsubscribe      prometheus.Counter
	mqttPubrec           prometheus.Counter
}

// Global variable
var db *maxminddb.Reader

func NewMetrics() *metrics {
	m := &metrics{
		totalConnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "total_connects",
			Help: "Total client connections",
		}, []string{"server"}),
		totalTrappedTime: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "total_trapped_time_ms",
			Help: "Total time clients were trapped (ms)",
		}, []string{"server"}),
		activeClients: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "current_connected_clients",
			Help: "Currently connected clients",
		}, []string{"server"}),
		clients: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tarpitted_clients",
			Help: "Connected clients",
		}, []string{ /*"ip", */ "server", "country", "latitude", "longitude"}),
		// ---------------
		upnpOtherHttpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upnp_other_http_requests",
			Help: "Number of http requests that are not for the .xml file",
		}, []string{"method", "url"}),
		upnpMSearchRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upnp_M-Search_requests",
			Help: "Number of M-Search requests",
		}, []string{"ip"}),
		upnpNonMSearchRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upnp_non_M-Search_requests",
			Help: "Number of SSDP requests that are not M-SEARCH",
		}, []string{"ip"}),
		// ---------------
		mqttMalformedConnect: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_pit_malformed_connects",
			Help: "Malformed MQTT CONNECT packets received",
		}),
		mqttConnectVersions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_pit_connect_versions",
			Help: "MQTT CONNECT versions used by clients",
		}, []string{"version"}),
		mqttSubscribeTopics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_pit_subscribe_topics",
			Help: "MQTT SUBSCRIBE topics and QoS",
		}, []string{"topic", "qos"}),
		mqttCredentials: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_pit_credentials",
			Help: "MQTT credentials used",
		}, []string{"username", "password"}),
		mqttPayloadCaptured: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_pit_payload_captured",
			Help: "Captured MQTT payload metadata from attacker traffic",
		}, []string{"ip", "port", "protocol", "packet_type", "payload"}),
		mqttPublishTopics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_pit_publish_topics",
			Help: "MQTT PUBLISH topic and QoS",
		}, []string{"topic", "qos"}),
		mqttConacks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_pit_connack_counter",
			Help: "Total CONNACK requests for MQTT",
		}),
		mqttUnsubscribe: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_pit_unsub_counter",
			Help: "Total UNSUBSCRIBE requests for MQTT",
		}),
		mqttPubrec: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_pit_pubrec_counter",
			Help: "Total PUBREC requests for MQTT",
		}),
	}
	prometheus.MustRegister(m.totalConnects, m.totalTrappedTime, m.activeClients, m.clients,
		m.upnpOtherHttpRequests, m.upnpMSearchRequests, m.upnpNonMSearchRequests,
		m.mqttConacks, m.mqttUnsubscribe, m.mqttPubrec,
		m.mqttMalformedConnect, m.mqttConnectVersions, m.mqttSubscribeTopics, m.mqttCredentials, m.mqttPayloadCaptured, m.mqttPublishTopics)
	return m
}

func main() {
	var err error
	geoliteDbPath := os.Getenv("GEO_DB")
	// fmt.Print(geoliteDbPath+"\n")
	db, err = maxminddb.Open(geoliteDbPath)
	if err != nil {
		log.Fatal("Cannot open GeoLite2 database: ", err)
	}
	defer db.Close()

	// Register metrics
	m := NewMetrics()

	// test values
	// m.totalTrappedTime.WithLabelValues("Telnet").Add(10)
	// m.totalTrappedTime.WithLabelValues("UPnP").Add(20)
	// m.totalTrappedTime.WithLabelValues("MQTT").Add(30)
	// m.totalTrappedTime.WithLabelValues("CoAP").Add(40)
	// m.totalTrappedTime.WithLabelValues("SSH").Add(50)

	// Start socket listener
	go listenForMetrics("/tmp/tarpit_exporter.sock", m)

	// HTTP handler
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Metrics available at :9101/metrics")
	log.Fatal(http.ListenAndServe(":9101", nil))
}

func listenForMetrics(socketPath string, metrics *metrics) {
	// Clean old socket
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove existing socket: %v", err)
	}

	conn, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		log.Fatalf("Socket bind error: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Println("Read error:", err)
			continue
		}
		handleMetric(strings.TrimSpace(string(buf[:n])), metrics)
	}
}

func handleMetric(line string, metrics *metrics) {
	fields := strings.Fields(line)
	log.Println(fields)

	server := fields[0]
	command := fields[1]

	switch command {
	case "connect":
		ip := fields[2]
		country := geoLookup(ip)
		lat := CapitalCoordinates[country].Latitude
		lon := CapitalCoordinates[country].Longitude
		// Reduce cardinality by removing ip
		handleConnect(server, country, lat, lon, metrics)
	case "disconnect":
		// ip := fields[2]
		parsedTimeTrapped, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			fmt.Println("Error parsing timeTrapped:", err)
			return
		}
		timeTrapped := float64(parsedTimeTrapped)
		handleDisconnect(server, timeTrapped, metrics)
	// UPnP
	case "otherHttpRequests":
		method := " "
		url := " "
		if len(fields) >= 3 {
			method = fields[2]
		}
		if len(fields) >= 4 {
			url = fields[3]
		}

		metrics.upnpOtherHttpRequests.WithLabelValues(method, url).Inc()
	case "M-SEARCH":
		ip := fields[2]
		metrics.upnpMSearchRequests.WithLabelValues(ip).Inc()
	case "non-M-SEARCH":
		ip := fields[2]
		metrics.upnpNonMSearchRequests.WithLabelValues(ip).Inc()
	// MQTT
	case "CONNECT":
		version := fields[2]
		metrics.mqttConnectVersions.WithLabelValues(version).Inc()

	case "malformedConnect":
		metrics.mqttMalformedConnect.Inc()

	case "SUBSCRIBE":
		topic := fields[2]
		qos := fields[3]
		metrics.mqttSubscribeTopics.WithLabelValues(topic, qos).Inc()

	case "credentials":
		username := " "
		password := " "
		if len(fields) >= 3 {
			username = fields[2]
		}
		if len(fields) >= 4 {
			password = fields[3]
		}

		metrics.mqttCredentials.WithLabelValues(username, password).Inc()

	case "PUBLISH":
		topic := fields[2]
		qos := fields[3]
		metrics.mqttPublishTopics.WithLabelValues(topic, qos).Inc()
	case "payloadCaptured":
		if len(fields) < 7 {
			return
		}
		ip := fields[2]
		port := fields[3]
		protocol := fields[4]
		packetType := fields[5]
		payload := fields[6]
		metrics.mqttPayloadCaptured.WithLabelValues(ip, port, protocol, packetType, payload).Inc()

	case "CONNACK":
		metrics.mqttConacks.Inc()
	case "UNSUBSCRIBE":
		metrics.mqttUnsubscribe.Inc()
	case "PUBREC":
		metrics.mqttPubrec.Inc()
	}
}

func handleConnect(server string, country string, lat float64, lon float64, metrics *metrics) {
	switch server {
	case "Telnet":
		metrics.totalConnects.WithLabelValues("Telnet").Inc()
		metrics.activeClients.WithLabelValues("Telnet").Inc()
		metrics.clients.WithLabelValues("Telnet", country, fmt.Sprintf("%f", lat), fmt.Sprintf("%f", lon)).Inc()
	case "UPnP":
		metrics.totalConnects.WithLabelValues("UPnP").Inc()
		metrics.activeClients.WithLabelValues("UPnP").Inc()
		metrics.clients.WithLabelValues("UPnP", country, fmt.Sprintf("%f", lat), fmt.Sprintf("%f", lon)).Inc()
	case "MQTT":
		metrics.totalConnects.WithLabelValues("MQTT").Inc()
		metrics.activeClients.WithLabelValues("MQTT").Inc()
		metrics.clients.WithLabelValues("MQTT", country, fmt.Sprintf("%f", lat), fmt.Sprintf("%f", lon)).Inc()
	case "CoAP":
		metrics.totalConnects.WithLabelValues("CoAP").Inc()
		metrics.activeClients.WithLabelValues("CoAP").Inc()
		metrics.clients.WithLabelValues("CoAP", country, fmt.Sprintf("%f", lat), fmt.Sprintf("%f", lon)).Inc()
	case "SSH":
		metrics.totalConnects.WithLabelValues("SSH").Inc()
		metrics.activeClients.WithLabelValues("SSH").Inc()
		metrics.clients.WithLabelValues("SSH", country, fmt.Sprintf("%f", lat), fmt.Sprintf("%f", lon)).Inc()
	}
}

func handleDisconnect(server string, timeTrapped float64, metrics *metrics) {
	switch server {
	case "Telnet":
		metrics.activeClients.WithLabelValues("Telnet").Dec()
		metrics.totalTrappedTime.WithLabelValues("Telnet").Add(timeTrapped)
	case "UPnP":
		metrics.activeClients.WithLabelValues("UPnP").Dec()
		metrics.totalTrappedTime.WithLabelValues("UPnP").Add(timeTrapped)
	case "MQTT":
		metrics.activeClients.WithLabelValues("MQTT").Dec()
		metrics.totalTrappedTime.WithLabelValues("MQTT").Add(timeTrapped)
	case "CoAP":
		metrics.activeClients.WithLabelValues("CoAP").Dec()
		metrics.totalTrappedTime.WithLabelValues("CoAP").Add(timeTrapped)
	case "SSH":
		metrics.activeClients.WithLabelValues("SSH").Dec()
		metrics.totalTrappedTime.WithLabelValues("SSH").Add(timeTrapped)
	}
}

func parseTimeMs(s string) int64 {
	var ms int64
	_, _ = fmt.Sscanf(s, "%d", &ms)
	return ms
}

func geoLookup(ipStr string) string {
	ip := netip.MustParseAddr(ipStr)

	var record struct {
		Country struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
	}
	err := db.Lookup(ip).Decode(&record)
	if err != nil {
		log.Panic(err)
	}
	fmt.Print(record.Country.ISOCode)

	return record.Country.ISOCode
}