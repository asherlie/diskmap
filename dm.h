#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

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

void init_diskmap(struct diskmap* dm, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t));
void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val);
void remove_key_diskmap();
void* lookup_diskmap();
