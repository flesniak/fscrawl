CC=g++
CFLAGS=-c -g -O2 -Wall -Wextra -I /usr/include/mysql-connector/
LDFLAGS=-lmysqlcppconn -lstdc++ -lrhash
EXECUTABLE=fscrawl

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
