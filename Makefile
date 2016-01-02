EXECUTABLE = fscrawl
CFLAGS = -c -Wall -Wextra
release: CFLAGS += -s -O2
debug:   CFLAGS += -g -O0
LDFLAGS = -lmysqlcppconn -lstdc++ -lrhash

GIT_VERSION := $(shell git describe --abbrev=4 --dirty --always --tags 2>/dev/null)
ifdef GIT_VERSION
  CFLAGS += -DVERSION=\"$(GIT_VERSION)\"
endif

SRCS = fscrawl.cpp logger.cpp worker.cpp hasher.cpp prepared_statement_wrapper.cpp
OBJS = $(SRCS:%.cpp=%.o)

.PHONY: all release debug clean

all: release

debug: $(EXECUTABLE)
release: $(EXECUTABLE)

$(EXECUTABLE): $(OBJS)
	$(CXX) $(LDFLAGS) -o $(EXECUTABLE) $(OBJS)

%.o: %.cpp
	$(CXX) $(CFLAGS) -o $@ $<

clean:
	rm -f $(OBJS) $(EXECUTABLE)
