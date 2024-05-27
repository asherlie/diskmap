/*
 * okay, this works
 * now think about a lock free persistant hash map
 * allocate n files, read phash code
 * possibly add to that existing project actually
 * the only missing piece is a mmap'd lock free threadsafety mechanism
 * can't remember why this is needed, but this missing piece will allow safety for specific files actually!
 * oh wait NVM!! i think this just makes it so that i can have multiple processes using the same phash!!!
 *
 * should it be possible to insert variable size values? strings for example?
 * yes. it should be. both k and v should optionally be variable length
 * this means each bucket should have the following structure:
 *  bucket:
 *      {INT_key,INT_val,KEYTYPE_key,VALTYPE_val}, // entry 0
 *      {INT_key,INT_val,KEYTYPE_key,VALTYPE_val}, // entry 1
 *      {INT_key,INT_val,KEYTYPE_key,VALTYPE_val}  // entry 2
 * implementation details:
 *  diskmap:
 *      int n_buckets;
 *      char* buckets[n_buckets] // file names
 *      int bucket_sizes[n_buckets]
 *      int bucket_locks[n_buckets]
 *
 *
 * upon insertion we hash the key, find the bucket file for that key
 * insert(key, val):
 *  idx = hash(key)
 *  bucket = buckets[idx]
 *
 *  // check bucket for duplicates while ensuring thread/process safety
 *  // keysize and valsize will speed this up in some cases
 *  // TODO: do we have to lock on the whole bucket? can i just iterate over it?
 *  // if keys never get removed we may not need to actually lock, 
 *  we could block new insertion index assignments until nobody is checking for duplicates anymore
 *  but this allows multiple identical insertions to have the same key actually
 *  the only way is likely to "lock" for duplicate checks
 *  we honestly also probably need to lock for full insertion. if we only get insertion idx assignments,
 *  one thread could lock, find no duplicates, get an ins index, 
 *  another thread could lock after, find no duplicates, get ins index,
 *  then they could both insert their new identical key and corrupt the map
 *  for this reason, we really do need to lock entire buckets from start to finish each insertion
 *  this is a shame but we can just have a huge number of buckets to "solve" this problem
 *
 *  to avoid this limitation we could guarantee no overwrites, but this would not be a true hashmap
 *  
 *  for (int i = 0; i < 
 *
 *
*/
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

struct diskmap{
    char name[12];
    uint32_t n_buckets;
    char** bucket_fns;
    uint16_t* bucket_sizes;
    pthread_mutex_t* bucket_locks;
};

// creates an mmap()'d file or opens one if it exists
// and updates dm->bucket_locks
void mmap_locks(struct diskmap* dm) {
    char lock_fn[20] = {0};
    int fd;
    int target_sz; 
    _Bool exists;

    snprintf(lock_fn, sizeof(lock_fn), "%s.LOCK", dm->name);
    fd = open(lock_fn, O_CREAT | O_RDWR, S_IRWXU);
    target_sz = (sizeof(pthread_mutex_t) * dm->n_buckets);
    exists = lseek(fd, 0, SEEK_END) == target_sz;
    if (!exists) {
        ftruncate(fd, target_sz);
    }

    dm->bucket_locks = mmap(0, target_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (dm->bucket_locks == MAP_FAILED) {
        perror("mmap()");
    }

    if (!exists) {
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            pthread_mutex_init(dm->bucket_locks + i, NULL);
        }
    }
}

void init_diskmap(struct diskmap* dm, uint32_t n_buckets, char* map_name) {
    strcpy(dm->name, map_name);
    dm->n_buckets = n_buckets;
    dm->bucket_sizes = malloc(sizeof(uint16_t) * dm->n_buckets);
    dm->bucket_fns = malloc(sizeof(char*) * dm->n_buckets);
    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        dm->bucket_fns[i] = calloc(sizeof(dm->name) + 11, 1);
        snprintf(dm->bucket_fns[i], sizeof(dm->name) + 11, "%s_%u", dm->name, i);
    }
    mmap_locks(dm);
    /*dm->bucket_locks = malloc();*/
    /*for (int i = 0; i < n_buckets*/
    /*pthread_mutex_init();*/
}
void acquire_bucket_lock();
void release_bucket_lock();

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
        usleep(1000);
    }
}
