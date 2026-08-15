.POSIX:
CC       = cc
CFLAGS   = -std=c99 -Wall -Wextra -Wno-missing-field-initializers -Os -pthread
CPPFLAGS =
LDFLAGS  = -ggdb3
LDLIBS   =
PREFIX   = /usr/local
SHARED   = /metric_events.c

all: endlessh

endlessh: endlessh.c $(SHARED)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) -o $@ endlessh.c $(SHARED) $(LDLIBS)

install: endlessh
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 endlessh $(DESTDIR)$(PREFIX)/bin/
	install -d $(DESTDIR)$(PREFIX)/share/man/man1
	install -m 644 endlessh.1 $(DESTDIR)$(PREFIX)/share/man/man1/

clean:
	rm -rf endlessh
