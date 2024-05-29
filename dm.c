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

/* this allows you to access portions of an mmap()d region between pages. if a new page is needed
 * this function will mmap() it
 */
/*
 * void* mmap_fine(int fd, off_t offset, uint32_t size, uint8_t* cur_page, uint32_t cur_page_idx, void* write, uint32_t write_sz) {
 *     long pgsz = sysconf(_SC_PAGE_SIZE);
 *     off_t adj_offset = offset / pgsz;
 *     return mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
 * }
*/

void* mmap_fine(int fd, off_t offset, uint32_t size, off_t* fine_off, size_t* adj_sz) {
    long pgsz = sysconf(_SC_PAGE_SIZE);
    uint32_t pgno = offset / pgsz;
    /* increase size by the amount of extra bytes we're reading due to aligning with a page */
    off_t adj_offset = pgno * pgsz;
    *adj_sz = size + (offset % pgsz);
    *fine_off = offset % pgsz;
    /*
     * printf("%li size %u -> pageno %u, adj_offset: %li\n", offset, size, pgno, pgno * pgsz);
     * printf("reading %li bytes from offset %lu. a user offset of %li must be used\n", *adj_sz, adj_offset, *fine_off);
    */

    return mmap(0, *adj_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, adj_offset);
}

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
    off_t fsz, adtnl_offset;
    size_t munmap_sz;
    struct entry_hdr* e;
    uint8_t* data;
    /*long pgsz = sysconf(_SC_PAGE_SIZE);*/
    /*uint32_t pages_needed, pg_idx = 0;*/

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
 * ugh, it may just be easier to grab as many pages as we need at a given time
 * this will let us avoid messing with page boundaries
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
        /*e = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, off);*/
        data = mmap_fine(fd, off, sizeof(struct entry_hdr) + keysz + valsz, &adtnl_offset, &munmap_sz);
        e = (struct entry_hdr*)(data + adtnl_offset);

        /*page = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, pgsz * pg_idx);*/

        /* if we've found a fragmented entry that will fit our new k/v pair */
        if (e->vsz == 0 && e->ksz >= (keysz + valsz)) {
            insertion_offset = off;
            printf("found internal fragmented offset at %li\n", insertion_offset);
        }

        // seek forward to read actual data
        /*off += sizeof(struct entry_hdr);*/
        /* if keysizes are !=, we don't need to compare keys */
        // TODO:!!! when i'm iterating through the list, keep track of an empty section that can fit our new
        // k/v pair!! this way we can fill deleted portions!
        if (e->ksz == keysz) {
            // TODO: do i need to munmap() before i re-mmap()?
            // TODO: all mmap() calls must use an offset divisible by page size ...
            /*data = mmap(0, e->ksz + e->vsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, off);*/

            /*ah, diff valsz makes this wrong instantly. with diff valsz, we need to fragment*/
            /*nvm already handled*/

            printf("found identical keysz of %i\n", keysz);
            if (!memcmp(data + adtnl_offset + sizeof(struct entry_hdr), key, keysz)) {
                printf("found identical KEY of %s!\n", (char*)key);
                /* overwrite entry and exit if new val fits in old val allocation
                 * otherwise, we have to fragment the bucket and erase this whole entry
                 */
                if (valsz <= e->vsz) {
                    puts("found a region to fit new val");
                    memcpy(data + sizeof(struct entry_hdr) + adtnl_offset + keysz, val, valsz);
                    munmap(data, munmap_sz);
                    goto cleanup;
                }
                // how do i mark this section as invalid without removing info about where next entry begins?
                // easy - if value is set to 0, we will understand key as meaning where next chunk starts
                // this is a great solution because is still allows us to increment off by ksz + vsz
                /*memset(e, 0, sizeof(struct entry_hdr));*/
                e->ksz += e->vsz;
                e->vsz = 0;
                printf("marked region at off %li for overwriting due to lack of space for new value\n", off);
                // hmm, maybe i shouldn't decrement size because size still is taken up
                --dm->bucket_sizes[idx];
                // TODO: make this more elegant, no reason to have two separate incrementations of off
                off += sizeof(struct entry_hdr) + e->ksz + e->vsz;
                break;
                // instead of breaking, we can just ensure that this is the last iteration and continue
                // this way we don't have to explicitly increment off again
                /*i = dm->bucket_sizes[idx];*/
            }
        }
        // TODO: not sure if mmap increments filepos or if i need to use off
        off += sizeof(struct entry_hdr) + e->ksz + e->vsz;
        puts("incremented off");
        munmap(data, munmap_sz);
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
        puts("growing file...");
        ftruncate(fd, MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz));
    }
    /*if (insertion_offset != -1) {*/
    /*}*/
    if (insertion_offset == -1) {
        insertion_offset = off;
    }
    // TODO: all mmap() calls must use an offset divisible by page size ...
    /*e = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, insertion_offset);*/



    /*
     * e = mmap_fine(fd, insertion_offset, sizeof(struct entry_hdr), &adtnl_offset);
     * (e+adtnl_offset)->vsz = valsz;
     * (e+adtnl_offset)->ksz = keysz;
     * insertion_offset += sizeof(struct entry_hdr);
     * munmap(e, sizeof(struct entry_hdr));
     * [>lseek(fd, 0, SEEK_SET);<]
     * // hmm, seems that insertion_offset makes this fail. this is probably somethign weird with fseek
     * // TODO: all mmap() calls must use an offset divisible by page size ...
     * data = mmap(0, keysz + valsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, insertion_offset);
     * mmap_fine(fd, insertion_offset, keysz + valsz, &adtnl_offset);
     * printf("mmap(0, %u, PROT_READ | PROT_WRITE, MAP_SHARED, %i, %li) == %p\n", keysz + valsz, fd, insertion_offset, data);
     * perror("mm");
     * memcpy(data, key, keysz);
     * memcpy(data+keysz, val, valsz);
    */

    data = mmap_fine(fd, insertion_offset, sizeof(struct entry_hdr) + keysz + valsz, &adtnl_offset, &munmap_sz);
    e = (struct entry_hdr*)(data + adtnl_offset);
    e->vsz = valsz;
    e->ksz = keysz;
    printf("writing new entry to offset %li\n", insertion_offset);
    memcpy((data + adtnl_offset + sizeof(struct entry_hdr)), key, keysz);
    memcpy((data + adtnl_offset + sizeof(struct entry_hdr) + keysz), val, valsz);
    munmap(data, munmap_sz);


    cleanup:
    pthread_mutex_unlock(dm->bucket_locks + idx);
}

/* remove_key_diskmap() sets e->vsz to 0, marking the region as reclaimable */
void remove_key_diskmap() {
}

int hash(void* key, uint32_t keysz, uint32_t n_buckets) {
    if (keysz < sizeof(int)) {
        return 9;
    }
    return *((int*)key) % n_buckets;
}

int main() {
    struct diskmap dm;
    /*
     * int val = 'z';
     * int key = '9';
    */
    init_diskmap(&dm, 10, "TESTMAP", hash);
    /*insert_diskmap(&dm, 4, 4, &key, &val);*/
    insert_diskmap(&dm, 2, 5, "BA", "ASHER");
    // hmm, this should be showing up at the end but instead hexdump shows that it's directly overwriting ASHER
    insert_diskmap(&dm, 2, 10, "BA", "**********");

    // this should take the space that "ASHER" previously took up
    insert_diskmap(&dm, 2, 4, "bs", "news");
}
