CC=g++
CFLAGS=-c -g -O2 -Wall -Wextra -I /usr/include/mysql-connector/
LDFLAGS=-lmysqlcppconn -lstdc++ -lrhash
EXECUTABLE=fscrawl

GIT_VERSION := $(shell git describe --abbrev=4 --dirty --always --tags 2>/dev/null)
ifdef GIT_VERSION
  CFLAGS += -DVERSION=\"$(GIT_VERSION)\"
endif

SRCS = fscrawl.cpp logger.cpp worker.cpp hasher.cpp
OBJS = $(SRCS:%.cpp=%.o)

.PHONY: all release clean

all: fscrawl

release: $(OBJS)
	$(CC) $(LDFLAGS) -s -o $(EXECUTABLE)-release $(OBJS)

fscrawl: $(OBJS)
	$(CC) $(LDFLAGS) -g -o $(EXECUTABLE) $(OBJS)

%.o: %.cpp
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(OBJS) fscrawl fscrawl-release
