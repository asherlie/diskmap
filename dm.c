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
#include <sys/param.h>
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

/*
 * void* mmap_fine(int fd, off_t offset) {
 *     return fd + offset;
 * }
*/

// TODO: all mmap() calls must use an offset divisible by page size ...
// this complicates things because if the bucket is smaller than a page, we might as well just load it all into memory
// we need to pick which page to load - we'll get page size and find which page we need to find the offset of
// for now i'll write an abstracted function to just get the right page, mmap to that offset, return that pointer + fine tuned
// offset. this is slower than just handling this all from insert_diskmap() because we could just grab a page
// at a time and only mmap a new page once we need to
// UGHHH, i should just do this from the outset. this is the only function where mmap()s have offsets anyway
// just keep track of offset like i am, but mmap() a page at a time UNLESS i need more than one page
// for a large keysz + valsz, hmm
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
    long pgsz = sysconf(_SC_PAGE_SIZE);
    uint32_t pages_needed;

/*
 * dupe checking:
 *     grab page
 *     iterate over the buffer looking at ksz and key comparisons
 *     each iteration, check if we're at the end of our page and if we need to grab a new one
 *     we will still record offsets in the same manner, but will have to be more careful when navigating to this offset
 *     NOTE: an entry_hdr or kv pair may be on a page boundary, in this case i'll have to be careful
 * 
 * insertion:
 *     grow file if needed
 *     if a new page is needed, grab it
 *     use offset + page number to find where to insert
 *     write ksz, vsz, k, v to offset
 * 
 * commit before implementing this!
 * i'll probably need to add 
 * int current_page
 * 
 * offsets will be global offsets from 0, so we'll calculate pages with that in mind
 * and then add offset % pagesize as our new offset
 * (4096*2 + 3) % 4096 == 3, so just divide for the page number
 * use modulus to get offset in page
 * 
*/

    pthread_mutex_lock(dm->bucket_locks + idx);
    if ((fsz = lseek(fd, 0, SEEK_END)) == 0) {
        ftruncate(fd, 5 * (keysz + valsz));
        fsz = 5 * (keysz + valsz);
    }
    dm->bytes_cap[idx] = fsz;
    lseek(fd, 0, SEEK_SET);
    for (uint16_t i = 0; i < dm->bucket_sizes[idx]; ++i) {
        // TODO: all mmap() calls must use an offset divisible by page size ...
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
            // TODO: all mmap() calls must use an offset divisible by page size ...
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
    if (insertion_offset == -1 && (off + sizeof(struct entry_hdr) + keysz + valsz >= (uint64_t)fsz)) {
        ftruncate(fd, MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz));
    }
    /*if (insertion_offset != -1) {*/
    /*}*/
    if (insertion_offset == -1) {
        insertion_offset = off;
    }
    // TODO: all mmap() calls must use an offset divisible by page size ...
    e = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, insertion_offset);
    e->vsz = valsz;
    e->ksz = keysz;
    insertion_offset += sizeof(struct entry_hdr);
    munmap(e, sizeof(struct entry_hdr));
    /*lseek(fd, 0, SEEK_SET);*/
    // hmm, seems that insertion_offset makes this fail. this is probably somethign weird with fseek
    // TODO: all mmap() calls must use an offset divisible by page size ...
    data = mmap(0, keysz + valsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, insertion_offset);
    printf("mmap(0, %u, PROT_READ | PROT_WRITE, MAP_SHARED, %i, %li) == %p\n", keysz + valsz, fd, insertion_offset, data);
    perror("mm");
    memcpy(data, key, keysz);
    memcpy(data+keysz, val, valsz);

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
    int val = 45;
    init_diskmap(&dm, 10, "TESTMAP", hash);
    insert_diskmap(&dm, 4, 4, &val, &val);
}
