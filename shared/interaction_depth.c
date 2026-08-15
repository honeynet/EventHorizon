#include "interaction_depth.h"

#include <string.h>

#define TELNET_IAC 255
#define TELNET_SE 240
#define TELNET_SB 250
#define TELNET_WILL 251
#define TELNET_WONT 252
#define TELNET_DO 253
#define TELNET_DONT 254

static void raiseDepth(struct interactionDepthState *state, unsigned int level) {
    if (state && level > state->level) {
        state->level = level > 3 ? 3 : level;
    }
}

void interactionDepthInit(struct interactionDepthState *state) {
    if (!state) {
        return;
    }
    memset(state, 0, sizeof(*state));
    state->telnetParseState = TELNET_DEPTH_DATA;
}

static bool isLineWhitespace(uint8_t byte) {
    return byte == ' ' || byte == '\t' || byte == '\v' || byte == '\f';
}

static void observeTelnetDataByte(struct interactionDepthState *state, uint8_t byte) {
    if (byte == '\r') {
        if (state->telnetLineHasMeaningfulData) {
            raiseDepth(state, state->level < 2 ? 2 : 3);
        }
        state->telnetLineHasMeaningfulData = false;
        state->telnetPreviousWasCr = true;
        return;
    }

    if (byte == '\n') {
        if (!state->telnetPreviousWasCr && state->telnetLineHasMeaningfulData) {
            raiseDepth(state, state->level < 2 ? 2 : 3);
        }
        state->telnetLineHasMeaningfulData = false;
        state->telnetPreviousWasCr = false;
        return;
    }

    state->telnetPreviousWasCr = false;
    if (isLineWhitespace(byte)) {
        return;
    }

    if (state->level >= 2) {
        raiseDepth(state, 3);
    } else {
        raiseDepth(state, 1);
    }
    state->telnetLineHasMeaningfulData = true;
}

void interactionDepthObserveTelnet(struct interactionDepthState *state,
                                   const uint8_t *data, size_t length) {
    if (!state || !data || state->finalized) {
        return;
    }

    for (size_t index = 0; index < length; index++) {
        uint8_t byte = data[index];
        switch (state->telnetParseState) {
            case TELNET_DEPTH_DATA:
                if (byte == TELNET_IAC) {
                    state->telnetPreviousWasCr = false;
                    state->telnetParseState = TELNET_DEPTH_IAC;
                } else {
                    observeTelnetDataByte(state, byte);
                }
                break;
            case TELNET_DEPTH_IAC:
                if (byte == TELNET_WILL || byte == TELNET_WONT ||
                    byte == TELNET_DO || byte == TELNET_DONT) {
                    state->telnetParseState = TELNET_DEPTH_IAC_OPTION;
                } else if (byte == TELNET_SB) {
                    state->telnetParseState = TELNET_DEPTH_SUBNEGOTIATION;
                } else {
                    state->telnetParseState = TELNET_DEPTH_DATA;
                }
                break;
            case TELNET_DEPTH_IAC_OPTION:
                state->telnetParseState = TELNET_DEPTH_DATA;
                break;
            case TELNET_DEPTH_SUBNEGOTIATION:
                if (byte == TELNET_IAC) {
                    state->telnetParseState = TELNET_DEPTH_SUBNEGOTIATION_IAC;
                }
                break;
            case TELNET_DEPTH_SUBNEGOTIATION_IAC:
                state->telnetParseState = byte == TELNET_SE
                    ? TELNET_DEPTH_DATA
                    : TELNET_DEPTH_SUBNEGOTIATION;
                break;
        }
    }
}

void interactionDepthObserveMqttConnect(struct interactionDepthState *state) {
    if (!state || state->finalized) {
        return;
    }
    state->mqttConnectAccepted = true;
    raiseDepth(state, 1);
}

void interactionDepthObserveMqttOperation(struct interactionDepthState *state) {
    if (!state || state->finalized || !state->mqttConnectAccepted) {
        return;
    }

    if (state->mqttMeaningfulOperations < 2) {
        state->mqttMeaningfulOperations++;
    }
    raiseDepth(state, state->mqttMeaningfulOperations == 1 ? 2 : 3);
}

unsigned int interactionDepthLevel(const struct interactionDepthState *state) {
    return state ? state->level : 0;
}

bool interactionDepthFinalize(struct interactionDepthState *state,
                              unsigned int *finalLevel) {
    if (!state || !finalLevel || state->finalized) {
        return false;
    }
    state->finalized = true;
    *finalLevel = state->level;
    return true;
}
