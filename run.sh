#!/bin/bash
# set -x  # Debug

BIN_DIR="/usr/local/bin"
PID_DIR="/tmp/tarpits"

mkdir -p "$PID_DIR"

allArgsAreNumbers() {
    for arg in "$@"; do
        if ! [[ "$arg" =~ ^[0-9]+$ ]]; then
            echo "Error: '$arg' is not a valid number."
            exit 1
        fi
    done
}

function showHelp() {
    echo "Usage:"
    echo "  $0 start <protocol> [args...]"
    echo "  $0 stop <protocol>"
    echo "  $0 status"
    echo
    echo "Servers and required arguments:"
    echo " "
    echo "  telnet <port> <delay> <max-clients>"
    echo "      - port: The local port to listen for incoming Telnet connections."
    echo "      - delay: Time in ms to wait before sending data to trap the client."
    echo "      - max-clients: Maximum number of concurrent connections allowed."
    echo " "
    echo "  upnp <http-port> <ssdp-port> <delay> <max-clients>"
    echo "      - http-port: Port for HTTP-based UPnP emulation."
    echo "      - ssdp-port: Port for SSDP discovery traffic (UDP 1900)."
    echo "      - delay: Response delay in ms to slow down automated scanners."
    echo "      - max-clients: Limit for simultaneous UPnP connection states."
    echo " "
    echo "  mqtt <port> <max-events> <epoll-interval> <pubrel-interval> <max-packets> <max-clients>"
    echo "      - port: The local port for the MQTT broker emulator."
    echo "      - max-events: Max events to process in a single epoll_wait cycle."
    echo "      - epoll-interval: Time in ms to wait between I/O polling cycles."
    echo "      - pubrel-interval: Delay in ms for the QoS 2 'Publish Release' handshake."
    echo "      - max-packets: Maximum number of MQTT control packets per session."
    echo "      - max-clients: Total concurrent IoT clients the engine will trap."
    echo " "
    echo "  coap <port> <delay> <ack-timeout> <max-retransmit> <max-clients>"
    echo "      - port: The local port for the CoAP server emulator."
    echo "      - delay: Delay in ms between CoAP processing cycles."
    echo "      - ack-timeout: Initial timeout in ms before retransmitting a confirmable message."
    echo "      - max-retransmit: Maximum retransmission attempts for confirmable CoAP messages."
    echo "      - max-clients: Total concurrent IoT clients the engine will trap."

}

function invalidAmountOfArgs() {
    echo "Error: Invalid amount of arguments for '$1'"
    echo
    showHelp
    exit 1
}

function startTelnet() {
    [ $# -ne 3 ] && invalidAmountOfArgs "telnet_pit"
    allArgsAreNumbers "$@"

    local port=$1
    local delay=$2
    local maxNoClients=$3
    echo "Starting telnet_pit with port=$port, delay=$delay, max-no-clients=$maxNoClients"

    exec "$BIN_DIR/telnet_pit" "$port" "$delay" "$maxNoClients"
}

function startUpnp() {
    [ $# -ne 4 ] && invalidAmountOfArgs "upnp_pit"
    allArgsAreNumbers "$@"

    local httpPort=$1
    local ssdpPort=$2
    local delay=$3
    local maxNoClients=$4
    echo "Starting upnp_pit with http-port=$httpPort ssdp-port=$ssdpPort delay=$delay max-no-clients=$maxNoClients"

    exec "$BIN_DIR/upnp_pit" "$httpPort" "$ssdpPort" "$delay" "$maxNoClients"
}

function startMqtt() {
    [ $# -ne 6 ] && invalidAmountOfArgs "mqtt_pit"
    allArgsAreNumbers "$@"
    local port=$1
    local maxEvents=$2
    local epollTimeoutInterval=$3
    local pubrelInterval=$4
    local maxPacketsPerClient=$5
    local maxNoClients=$6
    echo "Starting mqtt_pit with port=$port maxEvents=$maxEvents epollTimeoutInterval=$epollTimeoutInterval pubrelInterval=$pubrelInterval maxPacketsPerClient=$maxPacketsPerClient maxNoClients=$maxNoClients"
    
    exec "$BIN_DIR/mqtt_pit" "$port" "$maxEvents" "$epollTimeoutInterval" "$pubrelInterval" "$maxPacketsPerClient" "$maxNoClients"
}

function startCoap() {
    [ $# -ne 5 ] && invalidAmountOfArgs "coap_pit"
    allArgsAreNumbers "$@"

    local port=$1
    local delay=$2
    local ACK_TIMEOUT=$3
    local MAX_RETRANSMIT=$4
    local maxNoClients=$5
    echo "Starting coap_pit with port=$port, delay=$delay, ACK_TIMEOUT=$ACK_TIMEOUT, MAX_RETRANSMIT=$MAX_RETRANSMIT max-no-clients=$maxNoClients"
    exec "$BIN_DIR/coap_pit" "$port" "$delay" "$ACK_TIMEOUT" "$MAX_RETRANSMIT" "$maxNoClients"
}

function stopServer() {
    # TODO
    :
}

function status() {
    # TODO: Only check for single server
    :
}

case "$1" in
    start)
        shift
        protocol="$1"
        shift
        case "$protocol" in
            telnet)
                startTelnet "$@"
                ;;
            upnp)
                startUpnp "$@"
                ;;
            mqtt)
                startMqtt "$@"
                ;;
            coap)
                startCoap "$@"
                ;;
            *)
                echo "Unknown protocol: $protocol"
                showHelp
                exit 1
                ;;
        esac
        ;;
    stop)
        shift
        stopServer "$1"
        ;;
    status)
        status
        ;;
    --help|-h|help)
        showHelp
        ;;
    *)
        echo "Invalid command: $1"
        showHelp
        exit 1
        ;;
esac
