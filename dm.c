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
 * TODO: write a #define wrapper so we can have strongly typed maps
 * TODO: munmap
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

struct entry_hdr{
    uint32_t ksz, vsz;
};

struct diskmap{
    char name[12];
    // hash_func(key, keysz, n_buckets)
    /*int entries_in_mem;*/
    int (*hash_func)(void*, uint32_t, uint32_t);
    uint32_t n_buckets;
    char** bucket_fns;
    uint16_t* bucket_sizes;
    uint16_t* bucket_caps;
    off_t* bytes_in_use, * bytes_cap;
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
    lseek(fd, 0, SEEK_SET);
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

void init_diskmap(struct diskmap* dm, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t)) {
    strcpy(dm->name, map_name);
    /*dm->entries_in_mem = 19;*/
    dm->hash_func = hash_func;
    dm->n_buckets = n_buckets;
    /* TODO: consolidate these into a struct */
    dm->bucket_sizes = calloc(dm->n_buckets, sizeof(uint16_t));
    dm->bucket_caps = calloc(dm->n_buckets, sizeof(uint16_t));
    dm->bytes_in_use = calloc(dm->n_buckets, sizeof(off_t));
    dm->bytes_cap = calloc(dm->n_buckets, sizeof(off_t));
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

void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], O_CREAT | O_RDWR, S_IRWXU);
    /* TODO: need to truncate file to correct size if file does not exist, maybe like 2x needed size */
    off_t off = 0;
    off_t insertion_offset = -1;
    // TODO: remove the dm->* that are just these, no need to record state?
    off_t fsz;
    struct entry_hdr* e;
    uint8_t* data;
    pthread_mutex_lock(dm->bucket_locks + idx);
    if ((fsz = lseek(fd, 0, SEEK_END)) == 0) {
        ftruncate(fd, 5 * (keysz + valsz));
    }
    dm->bytes_cap[idx] = fsz;
    lseek(fd, 0, SEEK_SET);
    for (uint16_t i = 0; i < dm->bucket_sizes[idx]; ++i) {
        e = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, off);

        /* if we've found a fragmented entry that will fit our new k/v pair */
        if (e->vsz == 0 && e->ksz >= (keysz + valsz)) {
            insertion_offset = off;
            printf("found internal fragmented offset at %li\n", insertion_offset);
        }

        // seek forward to read actual data
        off += sizeof(struct entry_hdr);
        /* if keysizes are !=, we don't need to compare keys */
        // TODO:!!! when i'm iterating through the list, keep track of an empty section that can fit our new
        // k/v pair!! this way we can fill deleted portions!
        if (e->ksz == keysz) {
            // TODO: do i need to munmap() before i re-mmap()?
            data = mmap(0, e->ksz + e->vsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, off);
            if (!memcmp(data, key, keysz)) {
                /* overwrite entry and exit if new val fits in old val allocation
                 * otherwise, we have to fragment the bucket and erase this whole entry
                 */
                if (valsz <= e->vsz) {
                    memcpy(data + keysz, val, valsz);
                    goto cleanup;
                }
                // how do i mark this section as invalid without removing info about where next entry begins?
                // easy - if value is set to 0, we will understand key as meaning where next chunk starts
                // this is a great solution because is still allows us to increment off by ksz + vsz
                /*memset(e, 0, sizeof(struct entry_hdr));*/
                e->ksz += e->vsz;
                e->vsz = 0;
                // hmm, maybe i shouldn't decrement size because size still is taken up
                --dm->bucket_sizes[idx];
                break;
            }
        }
        // TODO: not sure if mmap increments filepos or if i need to use off
        off += e->ksz + e->vsz;
    }
    dm->bytes_in_use[idx] = off;

    ++dm->bucket_sizes[idx];
    /* this is reached if no duplicates are found OR a k/v pair now requires more space */
    /*TODO:need to truncate file to fit new entry*/
    /*if ()*/
    /*first, we check if we have an insertion_offset that will fit this. this will allow us to defragment a portion of our bucket*/
    /*if (fsz <)*/
    /*okay, we can calculate bytes_in_use from the loop above. we shouldn't rely on any state anyway!*/
    if (insertion_offset != -1) {
    }
    mmap(0, sizeof(struct entry_hdr) + keysz + valsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, );

    cleanup:
    pthread_mutex_unlock(dm->bucket_locks + idx);
}

int hash(void* key, uint32_t keysz, uint32_t n_buckets) {
    if (keysz < sizeof(int)) {
        return 9;
    }
    return *((int*)key) & n_buckets;
}

int main() {
    struct diskmap dm;
    int val = 5;
    init_diskmap(&dm, 10, "TESTMAP", hash);
    insert_diskmap(&dm, 4, 4, &val, &val);
}
