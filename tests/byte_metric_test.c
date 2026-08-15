#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../shared/structs.h"

static void assert_single_event(const char *message) {
    const char *newline = strchr(message, '\n');
    assert(newline != NULL);
    assert(newline[1] == '\0');
}

int main(void) {
    char message[128] = {0};

    assert(formatByteMetric(message, sizeof(message), "Telnet", "received", 17));
    assert(strcmp(message, "Telnet bytes_received 17\n") == 0);
    assert_single_event(message);

    /* A partial write reports its actual return value, not the requested size. */
    memset(message, 0, sizeof(message));
    assert(formatByteMetric(message, sizeof(message), "MQTT", "sent", 2));
    assert(strcmp(message, "MQTT bytes_sent 2\n") == 0);
    assert_single_event(message);

    assert(!formatByteMetric(message, sizeof(message), "Telnet", "sent", 0));
    assert(!formatByteMetric(message, sizeof(message), "MQTT", "received", -1));
    assert(!formatByteMetric(message, sizeof(message), "CoAP", "received", 4));
    assert(!formatByteMetric(message, sizeof(message), "Telnet", "sideways", 4));

    puts("byte metric helper tests passed");
    return 0;
}
