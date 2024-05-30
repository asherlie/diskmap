CC=gcc
CFLAGS= -Wall -Wextra -Wpedantic -Werror -Wno-unused-result -g3

all: tst

tst: dm.o tst.c
dm.o: dm.c dm.h


clean:
	rm -f tst *.o
