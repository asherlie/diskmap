/*
 * okay, this works
 * now think about a lock free persistant hash map
 * allocate n files, read phash code
 * possibly add to that existing project actually
 * the only missing piece is a mmap'd lock free threadsafety mechanism
 * can't remember why this is needed, but this missing piece will allow safety for specific files actually!
 * oh wait NVM!! i think this just makes it so that i can have multiple processes using the same phash!!!
*/
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

int main(){
    void* ret;
    int* val;
    int fd = open("MM", O_CREAT | O_RDWR, S_IRWXU);
    ftruncate(fd, sizeof(int));
    ret = mmap(0, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ret == MAP_FAILED) {
        puts("failed");
        perror("mmap");
    }

    val = ret;
    *val = 0;
    while (1) {
        printf("%p: %i\n", ret, *val);
        ++(*val);
        usleep(1000000);
    }
}
