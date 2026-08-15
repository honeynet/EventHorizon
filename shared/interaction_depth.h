#ifndef INTERACTION_DEPTH_H
#define INTERACTION_DEPTH_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum telnetDepthParseState {
    TELNET_DEPTH_DATA,
    TELNET_DEPTH_IAC,
    TELNET_DEPTH_IAC_OPTION,
    TELNET_DEPTH_SUBNEGOTIATION,
    TELNET_DEPTH_SUBNEGOTIATION_IAC
};

struct interactionDepthState {
    unsigned int level;
    bool finalized;

    bool telnetLineHasMeaningfulData;
    bool telnetPreviousWasCr;
    enum telnetDepthParseState telnetParseState;

    bool mqttConnectAccepted;
    unsigned int mqttMeaningfulOperations;
};

void interactionDepthInit(struct interactionDepthState *state);
void interactionDepthObserveTelnet(struct interactionDepthState *state,
                                   const uint8_t *data, size_t length);
void interactionDepthObserveMqttConnect(struct interactionDepthState *state);
void interactionDepthObserveMqttOperation(struct interactionDepthState *state);
unsigned int interactionDepthLevel(const struct interactionDepthState *state);
bool interactionDepthFinalize(struct interactionDepthState *state,
                              unsigned int *finalLevel);

#endif
