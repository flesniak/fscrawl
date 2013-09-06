CC=g++
CFLAGS=-c -g -Wall -Wextra -I /usr/include/mysql-connector/
LDFLAGS=-lmysqlcppconn -lstdc++
EXECUTABLE=fscrawl

all: fscrawl

release: fscrawl
	$(CC) $(LDFLAGS) -s -o $(EXECUTABLE)-release fscrawl.o worker.o

fscrawl: fscrawl.o worker.o
	$(CC) $(LDFLAGS) -o $(EXECUTABLE) fscrawl.o worker.o

fscrawl.o: fscrawl.cpp worker.o
	$(CC) $(CFLAGS) fscrawl.cpp

worker.o: worker.cpp
	$(CC) $(CFLAGS) worker.cpp

