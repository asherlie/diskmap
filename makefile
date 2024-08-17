CC=gcc
#CC=clang
CFLAGS= -Wall -Wextra -Wpedantic -Werror -Wno-unused-result -O3

all: tst

tst: dm.o tst.c
dm.o: dm.c dm.h


clean:
	rm -f tst *.o
