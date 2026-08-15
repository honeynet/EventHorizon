package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

const metricEventDatagramLimit = 512
const maxMetricEventInteger uint64 = 9007199254740991

type malformedReason string

const (
	malformedEmptyMessage     malformedReason = "empty_message"
	malformedMissingFields    malformedReason = "missing_fields"
	malformedUnknownFormat    malformedReason = "unknown_format"
	malformedUnknownServer    malformedReason = "unknown_server"
	malformedInvalidNumber    malformedReason = "invalid_number"
	malformedUnsupportedEvent malformedReason = "unsupported_event"
)

var malformedReasons = [...]malformedReason{
	malformedEmptyMessage,
	malformedMissingFields,
	malformedUnknownFormat,
	malformedUnknownServer,
	malformedInvalidNumber,
	malformedUnsupportedEvent,
}

type metricEventMutationKind uint8

const (
	metricEventMutationInvalid metricEventMutationKind = iota
	metricEventMutationTelnetConnectionAccepted
	metricEventMutationTelnetPositiveRead
	metricEventMutationTelnetFirstPositiveWrite
	metricEventMutationTelnetSubsequentPositiveWrite
	metricEventMutationTelnetConnectionFinalized
	metricEventMutationMQTTConnectionAccepted
	metricEventMutationMQTTPositiveRead
	metricEventMutationMQTTPositiveWrite
	metricEventMutationMQTTProtocolAction
	metricEventMutationMQTTConnackSent
	metricEventMutationMQTTSecondaryWriteError
	metricEventMutationMQTTConnectionFinalized
	metricEventMutationCoAPRequestReceived
	metricEventMutationCoAPRequestFinalized
	metricEventMutationCoAPCONResponseSent
	metricEventMutationCoAPCONResponseRetransmitted
	metricEventMutationCoAPCONResponseFinalized
	metricEventMutationCoAPWriteError
	metricEventMutationUPnPProtocolAction
	metricEventMutationUPnPDescriptionResponseStarted
	metricEventMutationUPnPDescriptionResponseFinalized
	metricEventMutationUPnPWriteError
	metricEventMutationSSHConnectionAccepted
	metricEventMutationSSHTrackedClientFinalized
)

type telnetFinalizationReason uint8

const (
	telnetFinalizationReasonInvalid telnetFinalizationReason = iota
	telnetFinalizationReasonPeerClosed
	telnetFinalizationReasonReadError
	telnetFinalizationReasonWriteError
	telnetFinalizationReasonServerShutdown
	telnetFinalizationReasonBoundedPolicy
)

var telnetFinalizationReasons = [...]telnetFinalizationReason{
	telnetFinalizationReasonPeerClosed,
	telnetFinalizationReasonReadError,
	telnetFinalizationReasonWriteError,
	telnetFinalizationReasonServerShutdown,
	telnetFinalizationReasonBoundedPolicy,
}

type ioReason uint8

const (
	ioReasonInvalid ioReason = iota
	ioReasonNone
	ioReasonTimeout
	ioReasonReset
	ioReasonClosed
	ioReasonOther
)

var metricIOReasons = [...]ioReason{
	ioReasonTimeout,
	ioReasonReset,
	ioReasonClosed,
	ioReasonOther,
}

type mqttFinalizationReason uint8

const (
	mqttFinalizationReasonInvalid mqttFinalizationReason = iota
	mqttFinalizationReasonPeerClosed
	mqttFinalizationReasonDisconnectReceived
	mqttFinalizationReasonConnectRefused
	mqttFinalizationReasonOperationRefused
	mqttFinalizationReasonProtocolError
	mqttFinalizationReasonKeepAliveTimeout
	mqttFinalizationReasonReadError
	mqttFinalizationReasonWriteError
	mqttFinalizationReasonServerShutdown
	mqttFinalizationReasonBoundedPolicy
)

var mqttFinalizationReasons = [...]mqttFinalizationReason{
	mqttFinalizationReasonPeerClosed,
	mqttFinalizationReasonDisconnectReceived,
	mqttFinalizationReasonConnectRefused,
	mqttFinalizationReasonOperationRefused,
	mqttFinalizationReasonProtocolError,
	mqttFinalizationReasonKeepAliveTimeout,
	mqttFinalizationReasonReadError,
	mqttFinalizationReasonWriteError,
	mqttFinalizationReasonServerShutdown,
	mqttFinalizationReasonBoundedPolicy,
}

type coapRequestOutcome uint8

const (
	coapRequestOutcomeInvalid coapRequestOutcome = iota
	coapRequestOutcomeResponseSent
	coapRequestOutcomeTerminated
)

var coapRequestOutcomes = [...]coapRequestOutcome{
	coapRequestOutcomeResponseSent,
	coapRequestOutcomeTerminated,
}

type coapCONOutcome uint8

const (
	coapCONOutcomeInvalid coapCONOutcome = iota
	coapCONOutcomeACKReceived
	coapCONOutcomeRSTReceived
	coapCONOutcomeRetryExhausted
)

var coapCONOutcomes = [...]coapCONOutcome{
	coapCONOutcomeACKReceived,
	coapCONOutcomeRSTReceived,
	coapCONOutcomeRetryExhausted,
}

type upnpDescriptionOutcome uint8

const (
	upnpDescriptionOutcomeInvalid upnpDescriptionOutcome = iota
	upnpDescriptionOutcomeCompleted
	upnpDescriptionOutcomeTerminated
)

type sshObservationEndReason uint8

const (
	sshObservationEndReasonInvalid sshObservationEndReason = iota
	sshObservationEndReasonWriteFailed
	sshObservationEndReasonServerShutdown
)

var upnpDescriptionOutcomes = [...]upnpDescriptionOutcome{
	upnpDescriptionOutcomeCompleted,
	upnpDescriptionOutcomeTerminated,
}

var sshObservationEndReasons = [...]sshObservationEndReason{
	sshObservationEndReasonWriteFailed,
	sshObservationEndReasonServerShutdown,
}

type protocolAction uint8

const (
	protocolActionInvalid protocolAction = iota
	protocolActionUPnPSSDPMSearchReceived
	protocolActionUPnPSSDPDiscoveryResponseSent
	protocolActionUPnPDescriptionGetReceived
	protocolActionUPnPDescriptionResponseStarted
	protocolActionUPnPDescriptionResponseCompleted
	protocolActionUPnPDescriptionResponseTerminated
	protocolActionCoAPGetRequestReceived
	protocolActionCoAPGetResponseSent
	protocolActionCoAPGetRequestTerminated
	protocolActionCoAPCONResponseSent
	protocolActionCoAPCONResponseRetransmitted
	protocolActionCoAPCONResponseACKReceived
	protocolActionCoAPCONResponseRSTReceived
	protocolActionCoAPCONResponseRetryExhausted
	protocolActionMQTTConnectAccepted
	protocolActionMQTTConnackSent
	protocolActionMQTTPublishReceived
	protocolActionMQTTSubscribeReceived
	protocolActionMQTTUnsubscribeReceived
	protocolActionMQTTSubackSent
	protocolActionMQTTUnsubackSent
	protocolActionMQTTPubackSent
	protocolActionMQTTPubrecSent
	protocolActionMQTTPubrelReceived
	protocolActionMQTTPubcompSent
	protocolActionMQTTSubscriptionPublishSent
)

type protocolActionDefinition struct {
	protocol string
	action   protocolAction
}

var protocolActionDefinitions = [...]protocolActionDefinition{
	{"upnp", protocolActionUPnPSSDPMSearchReceived},
	{"upnp", protocolActionUPnPSSDPDiscoveryResponseSent},
	{"upnp", protocolActionUPnPDescriptionGetReceived},
	{"upnp", protocolActionUPnPDescriptionResponseStarted},
	{"upnp", protocolActionUPnPDescriptionResponseCompleted},
	{"upnp", protocolActionUPnPDescriptionResponseTerminated},
	{"coap", protocolActionCoAPGetRequestReceived},
	{"coap", protocolActionCoAPGetResponseSent},
	{"coap", protocolActionCoAPGetRequestTerminated},
	{"coap", protocolActionCoAPCONResponseSent},
	{"coap", protocolActionCoAPCONResponseRetransmitted},
	{"coap", protocolActionCoAPCONResponseACKReceived},
	{"coap", protocolActionCoAPCONResponseRSTReceived},
	{"coap", protocolActionCoAPCONResponseRetryExhausted},
	{"mqtt", protocolActionMQTTConnectAccepted},
	{"mqtt", protocolActionMQTTConnackSent},
	{"mqtt", protocolActionMQTTPublishReceived},
	{"mqtt", protocolActionMQTTSubscribeReceived},
	{"mqtt", protocolActionMQTTUnsubscribeReceived},
	{"mqtt", protocolActionMQTTSubackSent},
	{"mqtt", protocolActionMQTTUnsubackSent},
	{"mqtt", protocolActionMQTTPubackSent},
	{"mqtt", protocolActionMQTTPubrecSent},
	{"mqtt", protocolActionMQTTPubrelReceived},
	{"mqtt", protocolActionMQTTPubcompSent},
	{"mqtt", protocolActionMQTTSubscriptionPublishSent},
}

type metricEventMutation struct {
	kind                      metricEventMutationKind
	activeCountAfter          uint32
	secondaryActiveCountAfter uint32
	durationMS                uint64
	value                     uint64
	depthLevel                uint8
	telnetFinalizationReason  telnetFinalizationReason
	ioReason                  ioReason
	protocolAction            protocolAction
	mqttFinalizationReason    mqttFinalizationReason
	coapRequestOutcome        coapRequestOutcome
	coapCONOutcome            coapCONOutcome
	upnpDescriptionOutcome    upnpDescriptionOutcome
	sshObservationEndReason   sshObservationEndReason
}

type metricEventMetrics struct {
	mutationMutex sync.Mutex

	totalConnects             *prometheus.CounterVec
	activeClients             *prometheus.GaugeVec
	exporterMalformedMessages *prometheus.CounterVec
	bytesReceived             *prometheus.CounterVec
	bytesSent                 *prometheus.CounterVec
	completedSessions         *prometheus.CounterVec
	sessionDuration           *prometheus.HistogramVec
	sessionInteractionDepth   *prometheus.CounterVec
	readErrors                *prometheus.CounterVec
	writeErrors               *prometheus.CounterVec
	protocolActions           *prometheus.CounterVec
	mqttConnectToConnack      prometheus.Histogram
	mqttFinalizations         *prometheus.CounterVec
	mqttConnectionDuration    prometheus.Histogram
	mqttInteractionDepth      *prometheus.CounterVec
	coapActiveRequests        prometheus.Gauge
	coapRequestDuration       *prometheus.HistogramVec
	coapActiveCONResponses    prometheus.Gauge
	coapCONDuration           *prometheus.HistogramVec
	upnpActiveDescriptions    prometheus.Gauge
	upnpDescriptionDuration   *prometheus.HistogramVec
	telnetFirstWriteDelay     prometheus.Histogram
	telnetInterWriteInterval  prometheus.Histogram
	sshTrackedClientLifetime  *prometheus.HistogramVec
}

func newMetricEventMetrics(registerer prometheus.Registerer) *metricEventMetrics {
	metricState := &metricEventMetrics{
		totalConnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "total_connects",
			Help: "Total accepted TCP connections tracked by the EventHorizon Telnet and MQTT tarpits and by the integrated Endlessh SSH tarpit.",
		}, []string{"server"}),
		activeClients: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "current_connected_clients",
			Help: "Current accepted TCP connections tracked by the EventHorizon Telnet and MQTT servers.",
		}, []string{"server"}),
		exporterMalformedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_exporter_malformed_messages_total",
			Help: "Total malformed or unsupported metric datagrams observed and rejected by the EventHorizon exporter.",
		}, []string{"reason"}),
		bytesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_bytes_received_total",
			Help: "Positive bytes returned by Telnet and MQTT client-facing reads; includes protocol framing and incomplete input, and excludes EOF, failed, retryable, or zero-byte I/O and internal telemetry.",
		}, []string{"protocol"}),
		bytesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_bytes_sent_total",
			Help: "Positive bytes returned by Telnet and MQTT client-facing writes; includes partial writes and protocol framing, and excludes failed, retryable, or zero-byte I/O and internal telemetry.",
		}, []string{"protocol"}),
		completedSessions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_completed_sessions_total",
			Help: "Total finalized tracked Telnet TCP sessions by finalization reason.",
		}, []string{"protocol", "disconnect_reason"}),
		sessionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventhorizon_session_duration_ms",
			Help:    "Duration in milliseconds from accepted Telnet TCP connection to exactly-once session finalization.",
			Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000},
		}, []string{"protocol"}),
		sessionInteractionDepth: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_session_interaction_depth_total",
			Help: "Total finalized tracked Telnet sessions by bounded meaningful application-interaction depth.",
		}, []string{"protocol", "depth_level"}),
		readErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_read_errors_total",
			Help: "Total primary unrecoverable read-side I/O failures that finalize tracked Telnet or MQTT TCP connections.",
		}, []string{"protocol", "reason"}),
		writeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_write_errors_total",
			Help: "Total write-side I/O failures crossing frozen EventHorizon reliability boundaries.",
		}, []string{"protocol", "reason"}),
		protocolActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_protocol_actions_total",
			Help: "Total protocol actions crossing their frozen EventHorizon server-side observation boundaries.",
		}, []string{"protocol", "action"}),
		mqttConnectToConnack: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "eventhorizon_mqtt_connect_to_connack_duration_ms",
			Help:    "Duration in milliseconds from accepted supported MQTT CONNECT to complete successful CONNACK transmission.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000},
		}),
		mqttFinalizations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_mqtt_network_connection_finalizations_total",
			Help: "Total finalized tracked MQTT Network Connections by finalization reason.",
		}, []string{"finalization_reason"}),
		mqttConnectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "eventhorizon_mqtt_network_connection_duration_ms",
			Help:    "Duration in milliseconds from accepted MQTT TCP connection to exactly-once Network Connection finalization.",
			Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 300000, 600000, 1800000, 3600000, 21600000, 86400000, 604800000},
		}),
		mqttInteractionDepth: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventhorizon_mqtt_network_connection_interaction_depth_total",
			Help: "Total finalized MQTT Network Connections by bounded meaningful application-interaction depth.",
		}, []string{"depth_level"}),
		coapActiveRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "eventhorizon_coap_active_request_exchanges",
			Help: "Current accepted CoAP GET request exchanges awaiting response transmission or termination.",
		}),
		coapRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventhorizon_coap_request_exchange_duration_ms",
			Help:    "Duration in milliseconds from accepted CoAP GET request to response transmission or termination.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 300000, 600000},
		}, []string{"outcome"}),
		coapActiveCONResponses: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "eventhorizon_coap_active_con_response_exchanges",
			Help: "Current transmitted CoAP Confirmable response exchanges awaiting ACK, RST, or retry exhaustion.",
		}),
		coapCONDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventhorizon_coap_con_response_exchange_duration_ms",
			Help:    "Duration in milliseconds from first CoAP Confirmable response transmission to ACK, RST, or retry exhaustion.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 6000, 12000, 24000, 45000, 60000},
		}, []string{"outcome"}),
		upnpActiveDescriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "eventhorizon_upnp_active_description_responses",
			Help: "Current UPnP device-description responses that have started but not completed or terminated.",
		}),
		upnpDescriptionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventhorizon_upnp_description_stream_duration_ms",
			Help:    "Duration in milliseconds from the first positive device-description response write to completion or termination.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000, 20000, 25000, 30000, 60000},
		}, []string{"outcome"}),
		telnetFirstWriteDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "eventhorizon_telnet_first_write_delay_ms",
			Help:    "Duration in milliseconds from accepted Telnet TCP connection to its first positive server write.",
			Buckets: []float64{10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 2500, 5000, 10000},
		}),
		sshTrackedClientLifetime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventhorizon_ssh_tracked_client_lifetime_ms",
			Help:    "Observed lifetime in milliseconds of an Endlessh-tracked SSH client, from accepted TCP connection until removal from the tracked-client set.",
			Buckets: []float64{1000, 5000, 10000, 20000, 30000, 60000, 120000, 300000, 600000, 1800000, 3600000, 21600000, 86400000, 604800000},
		}, []string{"observation_end_reason"}),
		telnetInterWriteInterval: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "eventhorizon_telnet_inter_write_interval_ms",
			Help:    "Duration in milliseconds between consecutive positive server writes on a tracked Telnet session.",
			Buckets: []float64{10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 2500, 5000, 10000},
		}),
	}

	for _, server := range []string{"Telnet", "MQTT"} {
		metricState.totalConnects.WithLabelValues(server).Add(0)
		metricState.activeClients.WithLabelValues(server).Set(0)
	}
	metricState.totalConnects.WithLabelValues("SSH").Add(0)
	for _, protocol := range []string{"telnet", "mqtt"} {
		metricState.bytesReceived.WithLabelValues(protocol).Add(0)
		metricState.bytesSent.WithLabelValues(protocol).Add(0)
	}
	for _, reason := range telnetFinalizationReasons {
		metricState.completedSessions.WithLabelValues("telnet", reason.label()).Add(0)
	}
	metricState.sessionDuration.WithLabelValues("telnet")
	for depthLevel := 0; depthLevel <= 3; depthLevel++ {
		metricState.sessionInteractionDepth.WithLabelValues("telnet", strconv.Itoa(depthLevel)).Add(0)
	}
	for _, protocol := range []string{"telnet", "mqtt"} {
		for _, reason := range metricIOReasons {
			metricState.readErrors.WithLabelValues(protocol, reason.label()).Add(0)
		}
	}
	for _, protocol := range []string{"upnp", "coap", "telnet", "mqtt"} {
		for _, reason := range metricIOReasons {
			metricState.writeErrors.WithLabelValues(protocol, reason.label()).Add(0)
		}
	}
	for _, definition := range protocolActionDefinitions {
		metricState.protocolActions.WithLabelValues(
			definition.protocol,
			definition.action.label(),
		).Add(0)
	}
	for _, reason := range mqttFinalizationReasons {
		metricState.mqttFinalizations.WithLabelValues(reason.label()).Add(0)
	}
	for depthLevel := 0; depthLevel <= 3; depthLevel++ {
		metricState.mqttInteractionDepth.WithLabelValues(strconv.Itoa(depthLevel)).Add(0)
	}
	for _, outcome := range coapRequestOutcomes {
		metricState.coapRequestDuration.WithLabelValues(outcome.label())
	}
	for _, outcome := range coapCONOutcomes {
		metricState.coapCONDuration.WithLabelValues(outcome.label())
	}
	for _, outcome := range upnpDescriptionOutcomes {
		metricState.upnpDescriptionDuration.WithLabelValues(outcome.label())
	}
	for _, reason := range sshObservationEndReasons {
		metricState.sshTrackedClientLifetime.WithLabelValues(reason.label())
	}
	for _, reason := range malformedReasons {
		metricState.exporterMalformedMessages.WithLabelValues(string(reason)).Add(0)
	}

	registerer.MustRegister(
		metricState.totalConnects,
		metricState.activeClients,
		metricState.exporterMalformedMessages,
		metricState.bytesReceived,
		metricState.bytesSent,
		metricState.completedSessions,
		metricState.sessionDuration,
		metricState.sessionInteractionDepth,
		metricState.readErrors,
		metricState.writeErrors,
		metricState.protocolActions,
		metricState.mqttConnectToConnack,
		metricState.mqttFinalizations,
		metricState.mqttConnectionDuration,
		metricState.mqttInteractionDepth,
		metricState.coapActiveRequests,
		metricState.coapRequestDuration,
		metricState.coapActiveCONResponses,
		metricState.coapCONDuration,
		metricState.upnpActiveDescriptions,
		metricState.upnpDescriptionDuration,
		metricState.telnetFirstWriteDelay,
		metricState.telnetInterWriteInterval,
		metricState.sshTrackedClientLifetime,
	)
	return metricState
}

func (metricState *metricEventMetrics) applyMetricEvent(
	mutation metricEventMutation,
	rejection malformedReason,
) {
	metricState.mutationMutex.Lock()
	defer metricState.mutationMutex.Unlock()

	if rejection != "" {
		metricState.exporterMalformedMessages.WithLabelValues(string(rejection)).Inc()
		return
	}

	switch mutation.kind {
	case metricEventMutationTelnetConnectionAccepted:
		metricState.totalConnects.WithLabelValues("Telnet").Inc()
		metricState.activeClients.WithLabelValues("Telnet").Set(float64(mutation.activeCountAfter))
	case metricEventMutationTelnetPositiveRead:
		metricState.bytesReceived.WithLabelValues("telnet").Add(float64(mutation.value))
	case metricEventMutationTelnetFirstPositiveWrite:
		metricState.bytesSent.WithLabelValues("telnet").Add(float64(mutation.value))
		metricState.telnetFirstWriteDelay.Observe(float64(mutation.durationMS))
	case metricEventMutationTelnetSubsequentPositiveWrite:
		metricState.bytesSent.WithLabelValues("telnet").Add(float64(mutation.value))
		metricState.telnetInterWriteInterval.Observe(float64(mutation.durationMS))
	case metricEventMutationTelnetConnectionFinalized:
		metricState.completedSessions.WithLabelValues(
			"telnet",
			mutation.telnetFinalizationReason.label(),
		).Inc()
		metricState.sessionDuration.WithLabelValues("telnet").Observe(float64(mutation.durationMS))
		metricState.sessionInteractionDepth.WithLabelValues(
			"telnet",
			strconv.FormatUint(uint64(mutation.depthLevel), 10),
		).Inc()
		metricState.activeClients.WithLabelValues("Telnet").Set(float64(mutation.activeCountAfter))
		switch mutation.telnetFinalizationReason {
		case telnetFinalizationReasonReadError:
			metricState.readErrors.WithLabelValues("telnet", mutation.ioReason.label()).Inc()
		case telnetFinalizationReasonWriteError:
			metricState.writeErrors.WithLabelValues("telnet", mutation.ioReason.label()).Inc()
		}
	case metricEventMutationMQTTConnectionAccepted:
		metricState.totalConnects.WithLabelValues("MQTT").Inc()
		metricState.activeClients.WithLabelValues("MQTT").Set(float64(mutation.activeCountAfter))
	case metricEventMutationMQTTPositiveRead:
		metricState.bytesReceived.WithLabelValues("mqtt").Add(float64(mutation.value))
	case metricEventMutationMQTTPositiveWrite:
		metricState.bytesSent.WithLabelValues("mqtt").Add(float64(mutation.value))
	case metricEventMutationMQTTProtocolAction:
		metricState.protocolActions.WithLabelValues("mqtt", mutation.protocolAction.label()).Inc()
	case metricEventMutationMQTTConnackSent:
		metricState.protocolActions.WithLabelValues("mqtt", protocolActionMQTTConnackSent.label()).Inc()
		metricState.mqttConnectToConnack.Observe(float64(mutation.durationMS))
	case metricEventMutationMQTTSecondaryWriteError:
		metricState.writeErrors.WithLabelValues("mqtt", mutation.ioReason.label()).Inc()
	case metricEventMutationMQTTConnectionFinalized:
		metricState.mqttFinalizations.WithLabelValues(mutation.mqttFinalizationReason.label()).Inc()
		metricState.mqttConnectionDuration.Observe(float64(mutation.durationMS))
		metricState.mqttInteractionDepth.WithLabelValues(
			strconv.FormatUint(uint64(mutation.depthLevel), 10),
		).Inc()
		metricState.activeClients.WithLabelValues("MQTT").Set(float64(mutation.activeCountAfter))
		switch mutation.mqttFinalizationReason {
		case mqttFinalizationReasonReadError:
			metricState.readErrors.WithLabelValues("mqtt", mutation.ioReason.label()).Inc()
		case mqttFinalizationReasonWriteError:
			metricState.writeErrors.WithLabelValues("mqtt", mutation.ioReason.label()).Inc()
		}
	case metricEventMutationCoAPRequestReceived:
		metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPGetRequestReceived.label()).Inc()
		metricState.coapActiveRequests.Set(float64(mutation.activeCountAfter))
	case metricEventMutationCoAPRequestFinalized:
		switch mutation.coapRequestOutcome {
		case coapRequestOutcomeResponseSent:
			metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPGetResponseSent.label()).Inc()
		case coapRequestOutcomeTerminated:
			metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPGetRequestTerminated.label()).Inc()
		}
		metricState.coapRequestDuration.WithLabelValues(mutation.coapRequestOutcome.label()).Observe(float64(mutation.durationMS))
		metricState.coapActiveRequests.Set(float64(mutation.activeCountAfter))
	case metricEventMutationCoAPCONResponseSent:
		metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPGetResponseSent.label()).Inc()
		metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPCONResponseSent.label()).Inc()
		metricState.coapRequestDuration.WithLabelValues(coapRequestOutcomeResponseSent.label()).Observe(float64(mutation.durationMS))
		metricState.coapActiveRequests.Set(float64(mutation.activeCountAfter))
		metricState.coapActiveCONResponses.Set(float64(mutation.secondaryActiveCountAfter))
	case metricEventMutationCoAPCONResponseRetransmitted:
		metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPCONResponseRetransmitted.label()).Inc()
	case metricEventMutationCoAPCONResponseFinalized:
		switch mutation.coapCONOutcome {
		case coapCONOutcomeACKReceived:
			metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPCONResponseACKReceived.label()).Inc()
		case coapCONOutcomeRSTReceived:
			metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPCONResponseRSTReceived.label()).Inc()
		case coapCONOutcomeRetryExhausted:
			metricState.protocolActions.WithLabelValues("coap", protocolActionCoAPCONResponseRetryExhausted.label()).Inc()
		}
		metricState.coapCONDuration.WithLabelValues(mutation.coapCONOutcome.label()).Observe(float64(mutation.durationMS))
		metricState.coapActiveCONResponses.Set(float64(mutation.activeCountAfter))
	case metricEventMutationCoAPWriteError:
		metricState.writeErrors.WithLabelValues("coap", mutation.ioReason.label()).Inc()
	case metricEventMutationUPnPProtocolAction:
		metricState.protocolActions.WithLabelValues("upnp", mutation.protocolAction.label()).Inc()
	case metricEventMutationUPnPDescriptionResponseStarted:
		metricState.protocolActions.WithLabelValues("upnp", protocolActionUPnPDescriptionResponseStarted.label()).Inc()
		metricState.upnpActiveDescriptions.Set(float64(mutation.activeCountAfter))
	case metricEventMutationUPnPDescriptionResponseFinalized:
		switch mutation.upnpDescriptionOutcome {
		case upnpDescriptionOutcomeCompleted:
			metricState.protocolActions.WithLabelValues("upnp", protocolActionUPnPDescriptionResponseCompleted.label()).Inc()
		case upnpDescriptionOutcomeTerminated:
			metricState.protocolActions.WithLabelValues("upnp", protocolActionUPnPDescriptionResponseTerminated.label()).Inc()
		}
		metricState.upnpDescriptionDuration.WithLabelValues(mutation.upnpDescriptionOutcome.label()).Observe(float64(mutation.durationMS))
		metricState.upnpActiveDescriptions.Set(float64(mutation.activeCountAfter))
	case metricEventMutationUPnPWriteError:
		metricState.writeErrors.WithLabelValues("upnp", mutation.ioReason.label()).Inc()
	case metricEventMutationSSHConnectionAccepted:
		metricState.totalConnects.WithLabelValues("SSH").Inc()
	case metricEventMutationSSHTrackedClientFinalized:
		metricState.sshTrackedClientLifetime.WithLabelValues(mutation.sshObservationEndReason.label()).Observe(float64(mutation.durationMS))
	}
}

type metricEventServer struct {
	connection  *net.UnixConn
	metricState *metricEventMetrics
	socketPath  string
	done        chan struct{}
	closeOnce   sync.Once
}

func startMetricEventServer(socketPath string, metricState *metricEventMetrics) (*metricEventServer, error) {
	if metricState == nil {
		return nil, errors.New("metric event state is required")
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	connection, err := net.ListenUnixgram("unixgram", &net.UnixAddr{
		Name: socketPath,
		Net:  "unixgram",
	})
	if err != nil {
		return nil, err
	}
	server := &metricEventServer{
		connection:  connection,
		metricState: metricState,
		socketPath:  socketPath,
		done:        make(chan struct{}),
	}
	go server.serve()
	return server, nil
}

func (server *metricEventServer) serve() {
	defer close(server.done)
	buffer := make([]byte, metricEventDatagramLimit+1)
	for {
		n, _, flags, _, err := server.connection.ReadMsgUnix(buffer, nil)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if flags&unix.MSG_TRUNC != 0 || n > metricEventDatagramLimit {
			server.metricState.applyMetricEvent(metricEventMutation{}, malformedUnknownFormat)
			continue
		}

		mutation, rejection := decodeMetricEvent(buffer[:n])
		server.metricState.applyMetricEvent(mutation, rejection)
	}
}

func (server *metricEventServer) Close() error {
	var closeErr error
	server.closeOnce.Do(func() {
		closeErr = server.connection.Close()
		<-server.done
		if err := os.Remove(server.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func decodeMetricEvent(datagram []byte) (metricEventMutation, malformedReason) {
	if len(bytes.TrimSpace(datagram)) == 0 {
		return metricEventMutation{}, malformedEmptyMessage
	}
	if !utf8.Valid(datagram) {
		return metricEventMutation{}, malformedUnknownFormat
	}
	fields, err := decodeTopLevelObject(datagram)
	if err != nil {
		return metricEventMutation{}, malformedUnknownFormat
	}
	if missingAnyField(fields, "v", "protocol", "event") {
		return metricEventMutation{}, malformedMissingFields
	}
	if string(bytes.TrimSpace(fields["v"])) != "1" {
		return metricEventMutation{}, malformedUnknownFormat
	}

	var protocol string
	if err := json.Unmarshal(fields["protocol"], &protocol); err != nil || !supportedMetricProtocol(protocol) {
		return metricEventMutation{}, malformedUnknownServer
	}
	var event string
	if err := json.Unmarshal(fields["event"], &event); err != nil {
		return metricEventMutation{}, malformedUnsupportedEvent
	}
	switch protocol {
	case "telnet":
		return decodeTelnetMetricEvent(fields, event)
	case "mqtt":
		return decodeMQTTMetricEvent(fields, event)
	case "coap":
		return decodeCoAPMetricEvent(fields, event)
	case "upnp":
		return decodeUPnPMetricEvent(fields, event)
	case "ssh":
		return decodeSSHMetricEvent(fields, event)
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeTelnetMetricEvent(
	fields map[string]json.RawMessage,
	event string,
) (metricEventMutation, malformedReason) {
	switch event {
	case "connection_accepted":
		if reason := validateExactFields(fields, "active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		activeCount, ok := parseMetricUint(fields["active_count_after"], 1, uint64(^uint32(0)))
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:             metricEventMutationTelnetConnectionAccepted,
			activeCountAfter: uint32(activeCount),
		}, ""
	case "positive_read":
		if reason := validateExactFields(fields, "bytes"); reason != "" {
			return metricEventMutation{}, reason
		}
		byteCount, ok := parseMetricUint(fields["bytes"], 1, maxMetricEventInteger)
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:  metricEventMutationTelnetPositiveRead,
			value: byteCount,
		}, ""
	case "first_positive_write":
		if reason := validateExactFields(fields, "duration_ms", "bytes"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		byteCount, bytesOK := parseMetricUint(fields["bytes"], 1, maxMetricEventInteger)
		if !durationOK || !bytesOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:       metricEventMutationTelnetFirstPositiveWrite,
			durationMS: duration,
			value:      byteCount,
		}, ""
	case "subsequent_positive_write":
		if reason := validateExactFields(fields, "duration_ms", "bytes"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		byteCount, bytesOK := parseMetricUint(fields["bytes"], 1, maxMetricEventInteger)
		if !durationOK || !bytesOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:       metricEventMutationTelnetSubsequentPositiveWrite,
			durationMS: duration,
			value:      byteCount,
		}, ""
	case "connection_finalized":
		if reason := validateExactFields(
			fields,
			"finalization_reason",
			"duration_ms",
			"depth_level",
			"active_count_after",
			"io_reason",
		); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		depthLevel, depthOK := parseMetricUint(fields["depth_level"], 0, 3)
		activeCount, activeOK := parseMetricUint(fields["active_count_after"], 0, uint64(^uint32(0)))
		if !durationOK || !depthOK || !activeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		finalizationReason, finalizationOK := parseTelnetFinalizationReason(fields["finalization_reason"])
		ioFailureReason, ioOK := parseIOReason(fields["io_reason"], true)
		if !finalizationOK || !ioOK || !validTelnetFinalizationIO(finalizationReason, ioFailureReason) {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:                     metricEventMutationTelnetConnectionFinalized,
			activeCountAfter:         uint32(activeCount),
			durationMS:               duration,
			depthLevel:               uint8(depthLevel),
			telnetFinalizationReason: finalizationReason,
			ioReason:                 ioFailureReason,
		}, ""
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeMQTTMetricEvent(
	fields map[string]json.RawMessage,
	event string,
) (metricEventMutation, malformedReason) {
	switch event {
	case "connection_accepted":
		if reason := validateExactFields(fields, "active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		activeCount, ok := parseMetricUint(fields["active_count_after"], 1, uint64(^uint32(0)))
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:             metricEventMutationMQTTConnectionAccepted,
			activeCountAfter: uint32(activeCount),
		}, ""
	case "positive_read":
		if reason := validateExactFields(fields, "bytes"); reason != "" {
			return metricEventMutation{}, reason
		}
		byteCount, ok := parseMetricUint(fields["bytes"], 1, maxMetricEventInteger)
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:  metricEventMutationMQTTPositiveRead,
			value: byteCount,
		}, ""
	case "positive_write":
		if reason := validateExactFields(fields, "bytes"); reason != "" {
			return metricEventMutation{}, reason
		}
		byteCount, ok := parseMetricUint(fields["bytes"], 1, maxMetricEventInteger)
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:  metricEventMutationMQTTPositiveWrite,
			value: byteCount,
		}, ""
	case "protocol_action":
		if reason := validateExactFields(fields, "action"); reason != "" {
			return metricEventMutation{}, reason
		}
		action, ok := parseMQTTProtocolAction(fields["action"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:           metricEventMutationMQTTProtocolAction,
			protocolAction: action,
		}, ""
	case "connack_sent":
		if reason := validateExactFields(fields, "duration_ms"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, ok := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:       metricEventMutationMQTTConnackSent,
			durationMS: duration,
		}, ""
	case "secondary_write_error":
		if reason := validateExactFields(fields, "io_reason"); reason != "" {
			return metricEventMutation{}, reason
		}
		failureReason, ok := parseIOReason(fields["io_reason"], false)
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:     metricEventMutationMQTTSecondaryWriteError,
			ioReason: failureReason,
		}, ""
	case "connection_finalized":
		if reason := validateExactFields(
			fields,
			"finalization_reason",
			"duration_ms",
			"depth_level",
			"active_count_after",
			"io_reason",
		); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		depthLevel, depthOK := parseMetricUint(fields["depth_level"], 0, 3)
		activeCount, activeOK := parseMetricUint(fields["active_count_after"], 0, uint64(^uint32(0)))
		if !durationOK || !depthOK || !activeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		finalizationReason, finalizationOK := parseMQTTFinalizationReason(fields["finalization_reason"])
		failureReason, ioOK := parseIOReason(fields["io_reason"], true)
		if !finalizationOK || !ioOK || !validMQTTFinalizationIO(finalizationReason, failureReason) {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:                   metricEventMutationMQTTConnectionFinalized,
			activeCountAfter:       uint32(activeCount),
			durationMS:             duration,
			depthLevel:             uint8(depthLevel),
			mqttFinalizationReason: finalizationReason,
			ioReason:               failureReason,
		}, ""
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeCoAPMetricEvent(
	fields map[string]json.RawMessage,
	event string,
) (metricEventMutation, malformedReason) {
	switch event {
	case "request_received":
		if reason := validateExactFields(fields, "request_active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		activeCount, ok := parseMetricUint(fields["request_active_count_after"], 1, uint64(^uint32(0)))
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:             metricEventMutationCoAPRequestReceived,
			activeCountAfter: uint32(activeCount),
		}, ""
	case "request_finalized":
		if reason := validateExactFields(fields, "outcome", "duration_ms", "request_active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		activeCount, activeOK := parseMetricUint(fields["request_active_count_after"], 0, uint64(^uint32(0)))
		if !durationOK || !activeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		outcome, ok := parseCoAPRequestOutcome(fields["outcome"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:               metricEventMutationCoAPRequestFinalized,
			activeCountAfter:   uint32(activeCount),
			durationMS:         duration,
			coapRequestOutcome: outcome,
		}, ""
	case "con_response_sent":
		if reason := validateExactFields(
			fields,
			"request_duration_ms",
			"request_active_count_after",
			"con_active_count_after",
		); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["request_duration_ms"], 0, maxMetricEventInteger)
		requestActiveCount, requestActiveOK := parseMetricUint(fields["request_active_count_after"], 0, uint64(^uint32(0)))
		conActiveCount, conActiveOK := parseMetricUint(fields["con_active_count_after"], 1, uint64(^uint32(0)))
		if !durationOK || !requestActiveOK || !conActiveOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:                      metricEventMutationCoAPCONResponseSent,
			activeCountAfter:          uint32(requestActiveCount),
			secondaryActiveCountAfter: uint32(conActiveCount),
			durationMS:                duration,
		}, ""
	case "con_response_retransmitted":
		if reason := validateExactFields(fields); reason != "" {
			return metricEventMutation{}, reason
		}
		return metricEventMutation{kind: metricEventMutationCoAPCONResponseRetransmitted}, ""
	case "con_response_finalized":
		if reason := validateExactFields(fields, "outcome", "duration_ms", "con_active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		activeCount, activeOK := parseMetricUint(fields["con_active_count_after"], 0, uint64(^uint32(0)))
		if !durationOK || !activeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		outcome, ok := parseCoAPCONOutcome(fields["outcome"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:             metricEventMutationCoAPCONResponseFinalized,
			activeCountAfter: uint32(activeCount),
			durationMS:       duration,
			coapCONOutcome:   outcome,
		}, ""
	case "write_error":
		if reason := validateExactFields(fields, "io_reason"); reason != "" {
			return metricEventMutation{}, reason
		}
		failureReason, ok := parseIOReason(fields["io_reason"], false)
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:     metricEventMutationCoAPWriteError,
			ioReason: failureReason,
		}, ""
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeSSHMetricEvent(
	fields map[string]json.RawMessage,
	event string,
) (metricEventMutation, malformedReason) {
	switch event {
	case "connection_accepted":
		if reason := validateExactFields(fields); reason != "" {
			return metricEventMutation{}, reason
		}
		return metricEventMutation{kind: metricEventMutationSSHConnectionAccepted}, ""
	case "tracked_client_finalized":
		if reason := validateExactFields(fields, "observation_end_reason", "lifetime_ms"); reason != "" {
			return metricEventMutation{}, reason
		}
		lifetime, lifetimeOK := parseMetricUint(fields["lifetime_ms"], 0, maxMetricEventInteger)
		if !lifetimeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		reason, ok := parseSSHObservationEndReason(fields["observation_end_reason"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:                    metricEventMutationSSHTrackedClientFinalized,
			durationMS:              lifetime,
			sshObservationEndReason: reason,
		}, ""
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeUPnPMetricEvent(
	fields map[string]json.RawMessage,
	event string,
) (metricEventMutation, malformedReason) {
	switch event {
	case "protocol_action":
		if reason := validateExactFields(fields, "action"); reason != "" {
			return metricEventMutation{}, reason
		}
		action, ok := parseUPnPProtocolAction(fields["action"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:           metricEventMutationUPnPProtocolAction,
			protocolAction: action,
		}, ""
	case "description_response_started":
		if reason := validateExactFields(fields, "active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		activeCount, ok := parseMetricUint(fields["active_count_after"], 1, uint64(^uint32(0)))
		if !ok {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:             metricEventMutationUPnPDescriptionResponseStarted,
			activeCountAfter: uint32(activeCount),
		}, ""
	case "description_response_finalized":
		if reason := validateExactFields(fields, "outcome", "duration_ms", "active_count_after"); reason != "" {
			return metricEventMutation{}, reason
		}
		duration, durationOK := parseMetricUint(fields["duration_ms"], 0, maxMetricEventInteger)
		activeCount, activeOK := parseMetricUint(fields["active_count_after"], 0, uint64(^uint32(0)))
		if !durationOK || !activeOK {
			return metricEventMutation{}, malformedInvalidNumber
		}
		outcome, ok := parseUPnPDescriptionOutcome(fields["outcome"])
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		if outcome == upnpDescriptionOutcomeCompleted && duration > 30000 {
			return metricEventMutation{}, malformedInvalidNumber
		}
		return metricEventMutation{
			kind:                   metricEventMutationUPnPDescriptionResponseFinalized,
			activeCountAfter:       uint32(activeCount),
			durationMS:             duration,
			upnpDescriptionOutcome: outcome,
		}, ""
	case "write_error":
		if reason := validateExactFields(fields, "io_reason"); reason != "" {
			return metricEventMutation{}, reason
		}
		failureReason, ok := parseIOReason(fields["io_reason"], false)
		if !ok {
			return metricEventMutation{}, malformedUnsupportedEvent
		}
		return metricEventMutation{
			kind:     metricEventMutationUPnPWriteError,
			ioReason: failureReason,
		}, ""
	default:
		return metricEventMutation{}, malformedUnsupportedEvent
	}
}

func decodeTopLevelObject(datagram []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(datagram))
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := opening.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, errors.New("metric event root is not an object")
	}

	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := nameToken.(string)
		if !ok {
			return nil, errors.New("metric event property name is not a string")
		}
		if _, exists := fields[name]; exists {
			return nil, errors.New("metric event property is duplicated")
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok = closing.(json.Delim)
	if !ok || delimiter != '}' {
		return nil, errors.New("metric event object is not closed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("metric event has trailing JSON content")
	}
	return fields, nil
}

func supportedMetricProtocol(protocol string) bool {
	switch protocol {
	case "upnp", "coap", "telnet", "mqtt", "ssh":
		return true
	default:
		return false
	}
}

func missingAnyField(fields map[string]json.RawMessage, names ...string) bool {
	for _, name := range names {
		if _, exists := fields[name]; !exists {
			return true
		}
	}
	return false
}

func validateExactFields(fields map[string]json.RawMessage, eventFields ...string) malformedReason {
	if missingAnyField(fields, eventFields...) {
		return malformedMissingFields
	}
	allowed := map[string]struct{}{
		"v": {}, "protocol": {}, "event": {},
	}
	for _, name := range eventFields {
		allowed[name] = struct{}{}
	}
	for name := range fields {
		if _, exists := allowed[name]; !exists {
			return malformedUnknownFormat
		}
	}
	return ""
}

func parseMetricUint(raw json.RawMessage, minimum, maximum uint64) (uint64, bool) {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 {
		return 0, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, false
	}
	return parsed, true
}

func parseTelnetFinalizationReason(raw json.RawMessage) (telnetFinalizationReason, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return telnetFinalizationReasonInvalid, false
	}
	switch value {
	case "peer_closed":
		return telnetFinalizationReasonPeerClosed, true
	case "read_error":
		return telnetFinalizationReasonReadError, true
	case "write_error":
		return telnetFinalizationReasonWriteError, true
	case "server_shutdown":
		return telnetFinalizationReasonServerShutdown, true
	case "bounded_policy":
		return telnetFinalizationReasonBoundedPolicy, true
	default:
		return telnetFinalizationReasonInvalid, false
	}
}

func (reason telnetFinalizationReason) label() string {
	switch reason {
	case telnetFinalizationReasonPeerClosed:
		return "peer_closed"
	case telnetFinalizationReasonReadError:
		return "read_error"
	case telnetFinalizationReasonWriteError:
		return "write_error"
	case telnetFinalizationReasonServerShutdown:
		return "server_shutdown"
	case telnetFinalizationReasonBoundedPolicy:
		return "bounded_policy"
	default:
		panic("invalid Telnet finalization reason")
	}
}

func parseIOReason(raw json.RawMessage, allowNone bool) (ioReason, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ioReasonInvalid, false
	}
	switch value {
	case "none":
		return ioReasonNone, allowNone
	case "timeout":
		return ioReasonTimeout, true
	case "reset":
		return ioReasonReset, true
	case "closed":
		return ioReasonClosed, true
	case "other":
		return ioReasonOther, true
	default:
		return ioReasonInvalid, false
	}
}

func (reason ioReason) label() string {
	switch reason {
	case ioReasonTimeout:
		return "timeout"
	case ioReasonReset:
		return "reset"
	case ioReasonClosed:
		return "closed"
	case ioReasonOther:
		return "other"
	default:
		panic("invalid I/O reason")
	}
}

func validTelnetFinalizationIO(reason telnetFinalizationReason, failureReason ioReason) bool {
	switch reason {
	case telnetFinalizationReasonReadError, telnetFinalizationReasonWriteError:
		return failureReason != ioReasonNone
	case telnetFinalizationReasonPeerClosed,
		telnetFinalizationReasonServerShutdown,
		telnetFinalizationReasonBoundedPolicy:
		return failureReason == ioReasonNone
	default:
		return false
	}
}

func parseMQTTProtocolAction(raw json.RawMessage) (protocolAction, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return protocolActionInvalid, false
	}
	switch value {
	case "connect_accepted":
		return protocolActionMQTTConnectAccepted, true
	case "publish_received":
		return protocolActionMQTTPublishReceived, true
	case "subscribe_received":
		return protocolActionMQTTSubscribeReceived, true
	case "unsubscribe_received":
		return protocolActionMQTTUnsubscribeReceived, true
	case "suback_sent":
		return protocolActionMQTTSubackSent, true
	case "unsuback_sent":
		return protocolActionMQTTUnsubackSent, true
	case "puback_sent":
		return protocolActionMQTTPubackSent, true
	case "pubrec_sent":
		return protocolActionMQTTPubrecSent, true
	case "pubrel_received":
		return protocolActionMQTTPubrelReceived, true
	case "pubcomp_sent":
		return protocolActionMQTTPubcompSent, true
	case "subscription_publish_sent":
		return protocolActionMQTTSubscriptionPublishSent, true
	default:
		return protocolActionInvalid, false
	}
}

func parseUPnPProtocolAction(raw json.RawMessage) (protocolAction, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return protocolActionInvalid, false
	}
	switch value {
	case "ssdp_msearch_received":
		return protocolActionUPnPSSDPMSearchReceived, true
	case "ssdp_discovery_response_sent":
		return protocolActionUPnPSSDPDiscoveryResponseSent, true
	case "description_get_received":
		return protocolActionUPnPDescriptionGetReceived, true
	default:
		return protocolActionInvalid, false
	}
}

func (action protocolAction) label() string {
	switch action {
	case protocolActionUPnPSSDPMSearchReceived:
		return "ssdp_msearch_received"
	case protocolActionUPnPSSDPDiscoveryResponseSent:
		return "ssdp_discovery_response_sent"
	case protocolActionUPnPDescriptionGetReceived:
		return "description_get_received"
	case protocolActionUPnPDescriptionResponseStarted:
		return "description_response_started"
	case protocolActionUPnPDescriptionResponseCompleted:
		return "description_response_completed"
	case protocolActionUPnPDescriptionResponseTerminated:
		return "description_response_terminated"
	case protocolActionCoAPGetRequestReceived:
		return "get_request_received"
	case protocolActionCoAPGetResponseSent:
		return "get_response_sent"
	case protocolActionCoAPGetRequestTerminated:
		return "get_request_terminated"
	case protocolActionCoAPCONResponseSent:
		return "con_response_sent"
	case protocolActionCoAPCONResponseRetransmitted:
		return "con_response_retransmitted"
	case protocolActionCoAPCONResponseACKReceived:
		return "con_response_ack_received"
	case protocolActionCoAPCONResponseRSTReceived:
		return "con_response_rst_received"
	case protocolActionCoAPCONResponseRetryExhausted:
		return "con_response_retry_exhausted"
	case protocolActionMQTTConnectAccepted:
		return "connect_accepted"
	case protocolActionMQTTConnackSent:
		return "connack_sent"
	case protocolActionMQTTPublishReceived:
		return "publish_received"
	case protocolActionMQTTSubscribeReceived:
		return "subscribe_received"
	case protocolActionMQTTUnsubscribeReceived:
		return "unsubscribe_received"
	case protocolActionMQTTSubackSent:
		return "suback_sent"
	case protocolActionMQTTUnsubackSent:
		return "unsuback_sent"
	case protocolActionMQTTPubackSent:
		return "puback_sent"
	case protocolActionMQTTPubrecSent:
		return "pubrec_sent"
	case protocolActionMQTTPubrelReceived:
		return "pubrel_received"
	case protocolActionMQTTPubcompSent:
		return "pubcomp_sent"
	case protocolActionMQTTSubscriptionPublishSent:
		return "subscription_publish_sent"
	default:
		panic("invalid protocol action")
	}
}

func parseMQTTFinalizationReason(raw json.RawMessage) (mqttFinalizationReason, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return mqttFinalizationReasonInvalid, false
	}
	switch value {
	case "peer_closed":
		return mqttFinalizationReasonPeerClosed, true
	case "disconnect_received":
		return mqttFinalizationReasonDisconnectReceived, true
	case "connect_refused":
		return mqttFinalizationReasonConnectRefused, true
	case "operation_refused":
		return mqttFinalizationReasonOperationRefused, true
	case "protocol_error":
		return mqttFinalizationReasonProtocolError, true
	case "keep_alive_timeout":
		return mqttFinalizationReasonKeepAliveTimeout, true
	case "read_error":
		return mqttFinalizationReasonReadError, true
	case "write_error":
		return mqttFinalizationReasonWriteError, true
	case "server_shutdown":
		return mqttFinalizationReasonServerShutdown, true
	case "bounded_policy":
		return mqttFinalizationReasonBoundedPolicy, true
	default:
		return mqttFinalizationReasonInvalid, false
	}
}

func (reason mqttFinalizationReason) label() string {
	switch reason {
	case mqttFinalizationReasonPeerClosed:
		return "peer_closed"
	case mqttFinalizationReasonDisconnectReceived:
		return "disconnect_received"
	case mqttFinalizationReasonConnectRefused:
		return "connect_refused"
	case mqttFinalizationReasonOperationRefused:
		return "operation_refused"
	case mqttFinalizationReasonProtocolError:
		return "protocol_error"
	case mqttFinalizationReasonKeepAliveTimeout:
		return "keep_alive_timeout"
	case mqttFinalizationReasonReadError:
		return "read_error"
	case mqttFinalizationReasonWriteError:
		return "write_error"
	case mqttFinalizationReasonServerShutdown:
		return "server_shutdown"
	case mqttFinalizationReasonBoundedPolicy:
		return "bounded_policy"
	default:
		panic("invalid MQTT finalization reason")
	}
}

func validMQTTFinalizationIO(reason mqttFinalizationReason, failureReason ioReason) bool {
	switch reason {
	case mqttFinalizationReasonReadError, mqttFinalizationReasonWriteError:
		return failureReason != ioReasonNone
	case mqttFinalizationReasonPeerClosed,
		mqttFinalizationReasonDisconnectReceived,
		mqttFinalizationReasonConnectRefused,
		mqttFinalizationReasonOperationRefused,
		mqttFinalizationReasonProtocolError,
		mqttFinalizationReasonKeepAliveTimeout,
		mqttFinalizationReasonServerShutdown,
		mqttFinalizationReasonBoundedPolicy:
		return failureReason == ioReasonNone
	default:
		return false
	}
}

func parseCoAPRequestOutcome(raw json.RawMessage) (coapRequestOutcome, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return coapRequestOutcomeInvalid, false
	}
	switch value {
	case "response_sent":
		return coapRequestOutcomeResponseSent, true
	case "terminated":
		return coapRequestOutcomeTerminated, true
	default:
		return coapRequestOutcomeInvalid, false
	}
}

func (outcome coapRequestOutcome) label() string {
	switch outcome {
	case coapRequestOutcomeResponseSent:
		return "response_sent"
	case coapRequestOutcomeTerminated:
		return "terminated"
	default:
		panic("invalid CoAP request outcome")
	}
}

func parseCoAPCONOutcome(raw json.RawMessage) (coapCONOutcome, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return coapCONOutcomeInvalid, false
	}
	switch value {
	case "ack_received":
		return coapCONOutcomeACKReceived, true
	case "rst_received":
		return coapCONOutcomeRSTReceived, true
	case "retry_exhausted":
		return coapCONOutcomeRetryExhausted, true
	default:
		return coapCONOutcomeInvalid, false
	}
}

func (outcome coapCONOutcome) label() string {
	switch outcome {
	case coapCONOutcomeACKReceived:
		return "ack_received"
	case coapCONOutcomeRSTReceived:
		return "rst_received"
	case coapCONOutcomeRetryExhausted:
		return "retry_exhausted"
	default:
		panic("invalid CoAP CON outcome")
	}
}

func parseUPnPDescriptionOutcome(raw json.RawMessage) (upnpDescriptionOutcome, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return upnpDescriptionOutcomeInvalid, false
	}
	switch value {
	case "completed":
		return upnpDescriptionOutcomeCompleted, true
	case "terminated":
		return upnpDescriptionOutcomeTerminated, true
	default:
		return upnpDescriptionOutcomeInvalid, false
	}
}

func parseSSHObservationEndReason(raw json.RawMessage) (sshObservationEndReason, bool) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return sshObservationEndReasonInvalid, false
	}
	switch value {
	case "write_failed":
		return sshObservationEndReasonWriteFailed, true
	case "server_shutdown":
		return sshObservationEndReasonServerShutdown, true
	default:
		return sshObservationEndReasonInvalid, false
	}
}

func (reason sshObservationEndReason) label() string {
	switch reason {
	case sshObservationEndReasonWriteFailed:
		return "write_failed"
	case sshObservationEndReasonServerShutdown:
		return "server_shutdown"
	default:
		panic("invalid SSH observation end reason")
	}
}

func (outcome upnpDescriptionOutcome) label() string {
	switch outcome {
	case upnpDescriptionOutcomeCompleted:
		return "completed"
	case upnpDescriptionOutcomeTerminated:
		return "terminated"
	default:
		panic("invalid UPnP description outcome")
	}
}
