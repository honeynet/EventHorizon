#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../shared/interaction_depth.h"

static void observe_telnet(struct interactionDepthState *state, const char *input) {
    interactionDepthObserveTelnet(state, (const uint8_t *)input,
                                  strlen(input));
}

static void assert_final_level(struct interactionDepthState *state,
                               unsigned int expected) {
    unsigned int final_level = 99;
    assert(interactionDepthFinalize(state, &final_level));
    assert(final_level == expected);
    assert(!interactionDepthFinalize(state, &final_level));
}

static void test_telnet_levels(void) {
    struct interactionDepthState state;

    interactionDepthInit(&state);
    assert(interactionDepthLevel(&state) == 0);
    assert_final_level(&state, 0);

    interactionDepthInit(&state);
    observe_telnet(&state, "partial");
    assert(interactionDepthLevel(&state) == 1);
    assert_final_level(&state, 1);

    interactionDepthInit(&state);
    observe_telnet(&state, "one line");
    observe_telnet(&state, "\r");
    assert(interactionDepthLevel(&state) == 2);
    observe_telnet(&state, "\n");
    assert(interactionDepthLevel(&state) == 2);
    assert_final_level(&state, 2);

    interactionDepthInit(&state);
    observe_telnet(&state, "one line\n");
    observe_telnet(&state, "continued");
    assert(interactionDepthLevel(&state) == 3);
    observe_telnet(&state, "\nmore\n");
    assert(interactionDepthLevel(&state) == 3);
    assert_final_level(&state, 3);
}

static void test_telnet_segmentation_and_negotiation(void) {
    struct interactionDepthState state;
    const uint8_t negotiation_part_one[] = {255, 253};
    const uint8_t negotiation_part_two[] = {1};
    const uint8_t subnegotiation_one[] = {255, 250, 24, 1};
    const uint8_t subnegotiation_two[] = {255, 240};

    interactionDepthInit(&state);
    interactionDepthObserveTelnet(&state, negotiation_part_one,
                                  sizeof(negotiation_part_one));
    interactionDepthObserveTelnet(&state, negotiation_part_two,
                                  sizeof(negotiation_part_two));
    interactionDepthObserveTelnet(&state, subnegotiation_one,
                                  sizeof(subnegotiation_one));
    interactionDepthObserveTelnet(&state, subnegotiation_two,
                                  sizeof(subnegotiation_two));
    observe_telnet(&state, " \t\r\n");
    assert(interactionDepthLevel(&state) == 0);

    observe_telnet(&state, "he");
    observe_telnet(&state, "lp\r");
    observe_telnet(&state, "\n");
    assert(interactionDepthLevel(&state) == 2);
}

static void test_mqtt_levels_and_invalid_order(void) {
    struct interactionDepthState state;

    interactionDepthInit(&state);
    interactionDepthObserveMqttOperation(&state);
    assert(interactionDepthLevel(&state) == 0);
    assert_final_level(&state, 0);

    interactionDepthInit(&state);
    interactionDepthObserveMqttConnect(&state);
    assert(interactionDepthLevel(&state) == 1);
    assert_final_level(&state, 1);

    interactionDepthInit(&state);
    interactionDepthObserveMqttConnect(&state);
    interactionDepthObserveMqttOperation(&state);
    assert(interactionDepthLevel(&state) == 2);
    assert_final_level(&state, 2);

    interactionDepthInit(&state);
    interactionDepthObserveMqttConnect(&state);
    interactionDepthObserveMqttOperation(&state);
    interactionDepthObserveMqttOperation(&state);
    assert(interactionDepthLevel(&state) == 3);

    interactionDepthObserveMqttConnect(&state);
    assert(interactionDepthLevel(&state) == 3);
    assert_final_level(&state, 3);
}

static void test_finalize_once(void) {
    struct interactionDepthState state;
    unsigned int final_level = 99;

    interactionDepthInit(&state);
    observe_telnet(&state, "one line\n");
    assert(interactionDepthFinalize(&state, &final_level));
    assert(final_level == 2);
    assert(!interactionDepthFinalize(&state, &final_level));

    observe_telnet(&state, "ignored after finalization\n");
    assert(interactionDepthLevel(&state) == 2);
}

int main(void) {
    test_telnet_levels();
    test_telnet_segmentation_and_negotiation();
    test_mqtt_levels_and_invalid_order();
    test_finalize_once();
    puts("interaction depth helper tests passed");
    return 0;
}
