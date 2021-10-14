
.PHONY: all clean run

all: a.out

run: all
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$(shell pwd -P)"
	LD_PRELOAD="$(shell pwd -P)/libruntime.so" ./a.out -p 2 10 20

clean:
	rm a.out

a.out: pa23.c
	$(CC) -o $@ -std=c99 -Wall -pedantic -g -O0 $^ $(CFLAGS) -L. -lruntime # -fsanitize=address

%.c:
