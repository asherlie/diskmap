CC=gcc
#CC=clang
CFLAGS= -Wall -Wextra -Wpedantic -Werror -Wno-unused-result -latomic -pthread -O3

all: tst

tst: dm.o tst.c
dm.o: dm.c dm.h

libdm.a: dm.c dm.h
	$(CC) -c dm.c -o dm.o
	ar rcs libdm.a dm.o

.PHONY:
install: libdm.a dm.h
	install libdm.a /usr/lib/libdm.a
	install dm.h /usr/include/dm.h

clean:
	rm -f tst *.o
