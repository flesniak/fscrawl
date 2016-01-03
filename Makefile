EXECUTABLE = fscrawl
CFLAGS = -Wall -Wextra -std=c++11
release: CFLAGS += -s -O2
debug:   CFLAGS += -g -O0
LDFLAGS = -lmysqlcppconn -lstdc++ -lrhash -lboost_program_options

GIT_VERSION := $(shell git describe --abbrev=4 --dirty --always --tags 2>/dev/null)
ifdef GIT_VERSION
  CFLAGS += -DVERSION=\"$(GIT_VERSION)\"
endif

SRCS = fscrawl.cpp logger.cpp worker.cpp hasher.cpp prepared_statement_wrapper.cpp options.cpp
OBJS = $(SRCS:%.cpp=%.o)

.PHONY: all release debug clean

all: release

debug: $(EXECUTABLE)
release: $(EXECUTABLE)

$(EXECUTABLE): $(OBJS)
	$(CXX) $(LDFLAGS) $(CFLAGS) -o $(EXECUTABLE) $(OBJS)

%.o: %.cpp
	$(CXX) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(EXECUTABLE)
