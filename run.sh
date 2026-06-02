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

# TODO: Describe args
function showHelp() {
    echo "Usage:"
    echo "  $0 start <protocol> [args...]"
    echo "  $0 stop <protocol>"
    echo "  $0 status"
    echo
    echo "Servers and required arguments:"
    echo "  telnet <port> <delay> <max-clients>"
    echo "    - port: "
    echo "    - delay: "
    echo "    - max-no-clients: "
    echo
    echo "  upnp <http-port> <ssdp-port> <delay> <max-clients>" 
    echo "    - http-port: "
    echo "    - ssdp-port: "
    echo "    - delay: "
    echo "    - max-clients: "
    echo
    echo "  mqtt <port> <max-events> <epoll-interval> <pubrel-interval> <max-packets> <max-clients>"
    echo "    - port: "
    echo "    - max-events: "
    echo "    - epoll-interval: "
    echo "    - pubrel-interval: "
    echo "    - max_packets: "
    echo "    - max-clients: "
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
