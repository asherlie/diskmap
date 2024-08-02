/*
 * TODO: write a #define wrapper so we can have strongly typed maps
 *
*/
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>

#include "dm.h"

/* there's no need to have a page tracker between threads
 * most of the benefit is from checking from duplicates, and
 * since insertion is always in a region that has just been 
 * checked, we'll never need to store pages more intelligently
 *
 * tracking pages between calls to insert()/lookup() would only
 * improve performance marginally and add a lot of complexity
 */
struct page_tracker{
    uint32_t n_pages;
    uint32_t byte_offset_start, n_bytes;

    uint8_t* mapped;
};


/* creates an mmap()'d file or opens one if it exists
 * and updates dm->bucket_locks
 */
#if 0
void mmap_activity_counter(struct diskmap* dm) {
    char lock_fn[30] = {0};
    int fd;
    int target_sz; 
    _Bool exists;
    pthread_mutexattr_t attr;

    snprintf(lock_fn, sizeof(lock_fn), "%s/%s.AC", dm->name, dm->name);
    fd = open(lock_fn, O_CREAT | O_RDWR, S_IRWXU);
    target_sz = (sizeof(_Atomic int) * dm->n_buckets);
    exists = lseek(fd, 0, SEEK_END) >= target_sz;
    lseek(fd, 0, SEEK_SET);
    if (!exists) {
        ftruncate(fd, target_sz);
        pthread_mutexattr_setpshared(&attr, 1);
    }

    dm->activity_counter = mmap(0, target_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (dm->activity_counter == MAP_FAILED) {
        perror("mmap()");
    }

    if (!exists) {
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            atomic_init(dm->activity_counter + i, 0);
        }
    }
    /* it's okay to close file descriptors that are mmap()d */
    close(fd);
}

void mmap_lookup_counter(struct diskmap* dm) {
    char fn[30] = {0};
    int fd;
    int target_sz; 
    _Bool exists;
    pthread_mutexattr_t attr;

    snprintf(fn, sizeof(fn), "%s/%s.LC", dm->name, dm->name);
    fd = open(fn, O_CREAT | O_RDWR, S_IRWXU);
    target_sz = (sizeof(_Atomic int) * dm->n_buckets);
    exists = lseek(fd, 0, SEEK_END) >= target_sz;
    lseek(fd, 0, SEEK_SET);
    if (!exists) {
        ftruncate(fd, target_sz);
        pthread_mutexattr_setpshared(&attr, 1);
    }

    dm->lookup_counter = mmap(0, target_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (dm->activity_counter == MAP_FAILED) {
        perror("mmap()");
    }

    if (!exists) {
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            atomic_init(dm->lookup_counter + i, 0);
        }
    }
    /* it's okay to close file descriptors that are mmap()d */
    close(fd);
}
#endif

void mmap_counter_struct(struct diskmap* dm) {
    char fn[30] = {0};
    int fd;
    int target_sz; 
    _Bool exists;

    snprintf(fn, sizeof(fn), "%s/%s.COU", dm->name, dm->name);
    fd = open(fn, O_CREAT | O_RDWR, S_IRWXU);
    target_sz = (sizeof(struct counters) * dm->n_buckets);
    exists = lseek(fd, 0, SEEK_END) >= target_sz;
    lseek(fd, 0, SEEK_SET);
    if (!exists) {
        ftruncate(fd, target_sz);
    }

    dm->counter = mmap(0, target_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (dm->counter == MAP_FAILED) {
        perror("mmap()");
    }

    if (!exists) {
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            atomic_init(&dm->counter[i].lookup_counter, 0);
            atomic_init(&dm->counter[i].insertion_counter, 0);
        }
    }
    /* it's okay to close file descriptors that are mmap()d */
    close(fd);
}

void init_diskmap(struct diskmap* dm, uint32_t n_pages, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t)) {
    strcpy(dm->name, map_name);
    /* TODO: fix perms */
    mkdir(dm->name, 0777);
    dm->hash_func = hash_func;
    dm->n_buckets = n_buckets;
    dm->bucket_fns = malloc(sizeof(char*) * dm->n_buckets);
    dm->pages_in_memory = n_pages;

    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        dm->bucket_fns[i] = calloc(sizeof(dm->name)*2 + 31, 1);
        snprintf(dm->bucket_fns[i], sizeof(dm->name)  + 31 + sizeof(dm->name), "%s/%s_%u", dm->name, dm->name, i);
    }

    mmap_counter_struct(dm);
    /*
     * mmap_activity_counter(dm);
     * mmap_lookup_counter(dm);
    */
}

void munmap_fine(struct page_tracker* pt) {
    munmap(pt->mapped, pt->n_bytes);
}

void* mmap_fine_optimized(struct page_tracker* pt, int fd, _Bool rdonly, off_t offset, uint32_t size) {
    long pgsz = sysconf(_SC_PAGE_SIZE);
    /* calculate starting page */
    uint32_t pgno = offset / pgsz;

    /*printf("got a request for offset %li -> %lu\n", offset, offset + size);*/
    if (pt->n_bytes) {
        if (offset >= pt->byte_offset_start && offset + size <= pt->byte_offset_start + pt->n_bytes) {
            /*printf("this is contained within %i -> %i, we're good.\n", pt->byte_offset_start, pt->byte_offset_start + pt->n_bytes);*/
            return pt->mapped + offset - pt->byte_offset_start;
        }
        munmap(pt->mapped, pt->n_bytes);
    }

    pt->byte_offset_start = pgno * pgsz;
    /*printf("nbytes = MAX(%li, %li)\n", pgsz * pt->n_pages, size + offset - pt->byte_offset_start);*/
    /* we may need to mmap() more than pt->n_pages if our data is between page boundaries */
    pt->n_bytes = MAX(pgsz * pt->n_pages, size + offset - pt->byte_offset_start);


    pt->mapped = mmap(0, pt->n_bytes, rdonly ? PROT_READ : PROT_READ | PROT_WRITE, MAP_SHARED, fd, pt->byte_offset_start);
    /*printf("re-MMAP required. %i -> %i\n", pt->byte_offset_start, pt->byte_offset_start + pt->n_bytes);*/
    assert(!(offset + size > pt->byte_offset_start + pt->n_bytes));
    return pt->mapped + (offset - pt->byte_offset_start);
}


void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], O_CREAT | O_RDWR, S_IRWXU);
    if (fd == -1)
        perror("OPEN");
    off_t off = 0;
    off_t insertion_offset = -1;
    off_t fsz;
    struct entry_hdr* e;
    uint8_t* data;
    _Bool first = 0;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};

    struct counters updated_cnt = {.lookup_counter = 0, .insertion_counter = 1};
    struct counters target_cnt;

    int attempts = 0;

    
    while (1) {
        memset(&target_cnt, 0, sizeof(struct counters));
        if (atomic_compare_exchange_strong(&dm->counter[idx], &target_cnt, updated_cnt)) {
            break;
        }
        ++attempts;
        printf("%i %i\n", dm->counter[idx].lookup_counter, dm->counter[idx].insertion_counter);
    }

    if (attempts > 0) {
        printf("acquired target insertion state in %i attempts\n", attempts);
    }

    /*pthread_mutex_lock(dm->bucket_locks + idx);*/
    // need to update n_insertions

    if ((fsz = lseek(fd, 0, SEEK_END)) == 0) {
        ftruncate(fd, 5 * (sizeof(struct entry_hdr) + keysz + valsz));
        fsz = 5 * sizeof(struct entry_hdr) + (keysz + valsz);
        first = 1;
    }
    lseek(fd, 0, SEEK_SET);
    while (!first && off < fsz) {
        data = mmap_fine_optimized(&pt, fd, 0, off, sizeof(struct entry_hdr) + keysz + valsz);
        e = (struct entry_hdr*)data;
        if (e->cap + e->ksz + e->vsz == 0) {
            break;
        }
        data = mmap_fine_optimized(&pt, fd, 0, off, sizeof(struct entry_hdr) + e->cap);
        e = (struct entry_hdr*)data;
        data += sizeof(struct entry_hdr);

        /* if we've found a fragmented entry that will fit our new k/v pair */
        if (e->vsz == 0 && e->cap >= (keysz + valsz)) {
            insertion_offset = off;
            break;
        }

        /* if keysizes are !=, we don't need to compare keys */
        if (e->ksz == keysz) {
            if (!memcmp(data, key, keysz)) {
                /* overwrite entry and exit if new val fits in old val allocation
                 * otherwise, we have to fragment the bucket and erase this whole entry
                 */
                if (valsz <= e->cap - e->ksz) {
                    e->vsz = valsz;
                    memcpy(data + keysz, val, valsz);
                    goto cleanup;
                }
                /* setting e->vsz to 0 to indicate that this is a deleted entry */
                e->ksz += e->vsz;
                e->vsz = 0;
                break;
            }
        }
        off += sizeof(struct entry_hdr) + e->cap;
    }

    /* this is reached if no duplicates are found OR a k/v pair now requires more space
     * first, we check if we have an insertion_offset that will fit this.
     * this will allow us to defragment a portion of our bucket
     */
    if (insertion_offset == -1) {
        insertion_offset = off;
    }

    if (insertion_offset + sizeof(struct entry_hdr) + keysz + valsz >= (uint64_t)fsz) {
        ftruncate(fd, (fsz = MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz)));
    }

    data = mmap_fine_optimized(&pt, fd, 0, insertion_offset, sizeof(struct entry_hdr) + keysz + valsz);
    e = (struct entry_hdr*)data;
    e->vsz = valsz;
    e->ksz = keysz;
    if (!e->cap) {
        e->cap = e->vsz + e->ksz;
    }
    memcpy((data + sizeof(struct entry_hdr)), key, keysz);
    memcpy((data + sizeof(struct entry_hdr) + keysz), val, valsz);

    cleanup:
    atomic_fetch_sub(&dm->counter[idx].insertion_counter, 1);
    munmap_fine(&pt);
    close(fd);
    /*pthread_mutex_unlock(dm->bucket_locks + idx);*/
}

_Bool lookup_diskmap_internal(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val, _Bool delete, _Bool check_vsz_only) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], delete ? O_RDWR : O_RDONLY);
    _Bool ret = 0;
    off_t fsz;
    off_t off = 0;
    struct entry_hdr* e;
    uint8_t* data;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};
    /*
     * we should maybe have both read and write locks, nvm just need write lock and need to make it so that
     * multiple lookups can run without locks
     * how do i do this?
     * when did i become retarded?
     *
     * write some code that runs in multi processes 
     * a test that looks up values!
     * i'll speed it up greatly if i remove the requirement to lock during lookups
     *
     * maybe use atomic ints, 
     * or atomic_cas()
     * readers writers
     *
     * maybe some combo of CAS and mutex, if !(CAS), mutex
     * idk...
     *
     * simultaneous lookups are totally fine, the second an insertion starts, however, we need to be careful
     * so we can just wait to insert until all lookups are done, adding an extra locking layer to insertion
     * lookup() will simply increment an atomic counter to keep track of n_lookups
     * if this is > 0, insert() must wait BEFORE acquiring a lock
     *
     *  ugh, nvm. this doesn't work. we need to NOT begin a lookup if we're already inserting as well
     *  maybe i can use two atomics and no locks - one for insert and one for lookup
     *
     *  insert():
     *      // if anything is going on, wait
     *      if (activity_counter) wait
     *      ++ins_counter; ++activity_counter
     *      maybe set both together in a struct
     *
     *  lookup()
     *      // if insertions are going on, wait
     *      if (ins_counter) wait
     *      ++activity_counter
     *
     *      we can have an activity counter and an insertion counter!
     *      but won't they both need to be incremented simultaneously?
     *      UGH. so hard to think about
     *
     * this needs to be mmap()d just like the mutex lock file
     * it can be put at the end of the smae file, or alternatively in  a separate one actually
     * will be simpler - KISS
     *
     * lookup()
     *  ++lookups[idx]
     *  --lookups[idx]
     *
     *  CAS(lookups[idx]);
     *
     * insert()
     *  ++insertions
     *
     *
     * maybe not such a big deal since we have a lock for each bucket thouuuugh
     * could still cause problems with low n_buckets or with a bad hashing func / uniform data
    */

    // TODO: test to make sure this even improves performance
    // there's a chance it degrades insertion performance
    /*
     * omg wait i can only enter this if there are no insertions, maybe scrap activity counter, just have insertion and lookup counter
     * and we can check for double zero for insertions and for single zero for lookups!! this is hypothetically an improvement to locking because
     * it allows us to have simultaneous lookups
    */
    /*
     * ugh, maybe i can actually make this good by atomically incrementing just lookups first, THEN, checking only for insertions
     * similarly, for insertions, i can ONLY check for the activity ... damn this is hard
     * for onw i think i should just update the entire struct at a time, this seems like it's the only way i caan get true thread safety
     *
     * just think through this
     * does it make sense? 
     *
     * there's no reason i shouldn't be able to atomically manipulate the member of an atomic struct
     *      
     *      lookup(): // need to increment lookups, need to check insertions!! don't need to
     *          CAS(counter[idx]->lookups, )
    */
    int attempts = 0;
    #if 0
    struct counters updated_cnt;
    /*struct counters tmp_cnt = {.lookup_counter = 0, .insertion_counter = 0};*/
    struct counters expected = atomic_load(&dm->counter[idx]);
    while (1) {
        // expected is auto-refilled each call
        memcpy(&updated_cnt, &expected, sizeof(struct counters));
        ++updated_cnt.lookup_counter;
        /*
         * this is a bad approach because it requires that lookups stay constant, this ideally shouldn't matter
        */
        /*atomic_fetch_add(dm->lookup_counter + idx, 1);*/
        /*atomic_fetch_add(dm->counters[idx], 1);*/
        /*check if any insertions are occurring*/
        if (atomic_compare_exchange_strong(&dm->counter[idx], &expected, updated_cnt)) {
            break;
        }
        ++attempts;
    }

    #endif
    // need to also fetch add action counter
    /*pthread_mutex_lock(dm->bucket_locks + idx);*/


    uint32_t n_lookups;
    n_lookups = 1 + atomic_fetch_add(&dm->counter[idx].lookup_counter, 1);
    /* spin until all insertions are complete, we can guarantee
     * that no new insertions will begin because lookup_counter is nonzero
     */
    while (atomic_load(&dm->counter[idx].insertion_counter)) {
        ++attempts;
    }
    /*atomic_compare_exchange_strong(&dm->counter[idx].insertion_counter, 1);*/

    if (attempts > 0) {
        printf("took %i attempts to safely begin a lookup\n", attempts);
    }
    if (n_lookups > 1) {
        printf("%i concurrent lookups!\n", n_lookups);
    }



    if ((fsz = lseek(fd, 0, SEEK_END)) <= 0) {
        ret = NULL;
        *valsz = 0;
        goto cleanup;
    }
    lseek(fd, 0, SEEK_SET);

    while (off < fsz) {
        e = mmap_fine_optimized(&pt, fd, 1, off, sizeof(struct entry_hdr) + (keysz * 2));
        if (!(e->ksz || e->vsz)) {
            goto cleanup;
        }
        off += sizeof(struct entry_hdr);
        if (e->vsz == 0 || e->ksz != keysz) {
            off += e->cap;
            continue;
        }
        /* we don't need to mmap() e->cap here, only grab relevant bytes */
        data = mmap_fine_optimized(&pt, fd, 1, off, e->ksz + e->vsz);
        if (memcmp(data, key, keysz)) {
            off += e->cap;
            continue;
        }

        ret = 1;
        *valsz = e->vsz;

        if (check_vsz_only) {
            goto cleanup;
        }

        if (delete) {
            e->ksz += e->vsz;
            e->vsz = 0;
            break;
        }

        memcpy(val, data + keysz, *valsz);
        break;
    }

    cleanup:
    /*expected = atomic_load(&dm->counter[idx]);*/
    // TODO: is this truly atomic or should i do a load first?
    atomic_fetch_sub(&dm->counter[idx].lookup_counter, 1);
    close(fd);
    munmap_fine(&pt);
    /*pthread_mutex_unlock(dm->bucket_locks + idx);*/
    /*atomic_fetch_sub(dm->lookup_counter + idx, 1);*/
    return ret;
}

_Bool lookup_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val) {
    return lookup_diskmap_internal(dm, keysz, key, valsz, val, 0, 0);
}

/* remove_key_diskmap() sets e->vsz to 0, marking the region as reclaimable */
_Bool remove_key_diskmap(struct diskmap* dm, uint32_t keysz, void* key) {
    uint32_t valsz;
    return lookup_diskmap_internal(dm, keysz, key, &valsz, NULL, 1, 0);
}

_Bool check_valsz_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz) {
    return lookup_diskmap_internal(dm, keysz, key, valsz, NULL, 0, 1);
}
