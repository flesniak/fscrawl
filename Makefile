CC=g++
CFLAGS=-c -g -Wall -Wextra -I /usr/include/mysql-connector/
LDFLAGS=-lmysqlcppconn -lstdc++
EXECUTABLE=fscrawl

all: fscrawl

release: fscrawl.o logger.o worker.o
	$(CC) $(LDFLAGS) -s -o $(EXECUTABLE)-release fscrawl.o worker.o

fscrawl: fscrawl.o logger.o worker.o
	$(CC) $(LDFLAGS) -o $(EXECUTABLE) fscrawl.o logger.o worker.o

fscrawl.o: fscrawl.cpp worker.o
	$(CC) $(CFLAGS) fscrawl.cpp

logger.o: logger.cpp
	$(CC) $(CFLAGS) logger.cpp

worker.o: worker.cpp logger.o
	$(CC) $(CFLAGS) worker.cpp
