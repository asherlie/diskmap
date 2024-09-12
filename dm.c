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

/* if *_exp_lock is set, this will be used as our expected value before swapping */
struct counters update_counter(_Atomic struct counters* ctr, int8_t lookup_delta, int8_t insert_delta, const uint32_t* lookup_exp_lock, 
                               const uint32_t* insert_exp_lock, int32_t max_attempts, uint32_t* attempts_required, _Bool* success) {
    int32_t attempts = 0;
    struct counters c, target;

    if (success) {
        *success = 0;
    }

    for (; attempts != max_attempts; ++attempts) {
        if (!lookup_exp_lock || !insert_exp_lock) {
            c = atomic_load(ctr);
        }
        if (lookup_exp_lock) {
            c.lookup_counter = *lookup_exp_lock;
        }
        if (insert_exp_lock) {
            c.insertion_counter = *insert_exp_lock;
        }
        target.lookup_counter = c.lookup_counter + lookup_delta;
        target.insertion_counter = c.insertion_counter + insert_delta;
        if (target.insertion_counter == INT32_MAX) {
            puts("reached error");
        }
        if (atomic_compare_exchange_strong(ctr, &c, target)) {
            if (success) {
                *success = 1;
            }
            break;
        }
    }
    if (attempts_required) {
        *attempts_required = attempts;
    }
    return target;
}

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
        // TODO: move above
        struct counters c = {.lookup_counter = 0, .insertion_counter = 0};
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            atomic_init(&dm->counter[i], c);
            /*
             * atomic_init(&dm->counter[i].lookup_counter, 0);
             * atomic_init(&dm->counter[i].insertion_counter, 0);
            */
        }
    }
    /* it's okay to close file descriptors that are mmap()d */
    close(fd);
}

_Bool flip_byte(char* str, int idx) {
    if (idx == -1) {
        return 1;
    }
    if (str[idx] == '9') {
        str[idx] = '0'; 
        return flip_byte(str, idx - 1);
    } else {
        ++str[idx];
        return 0;
    }
}

void increment_str_int(char* str, int* len) {
    if (flip_byte(str, *len - 1)) {
        *str = '1';
        str[*len] = '0';
        ++(*len);
        str[*len] = 0;
    }
}

void init_diskmap(struct diskmap* dm, uint32_t n_pages, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t)) {
    char base_str[sizeof(dm->name) * 2 + 31];
    int base_bytes;
    int str_int_offset, intlen = 1;

    strcpy(dm->name, map_name);
    /* TODO: fix perms */
    mkdir(dm->name, 0777);
    dm->hash_func = hash_func;
    dm->n_buckets = n_buckets;
    dm->bucket_fns = malloc(sizeof(char*) * dm->n_buckets);
    dm->pages_in_memory = n_pages;

    base_bytes = snprintf(base_str, sizeof(base_str), "%s/%s_0", dm->name, dm->name);
    str_int_offset = base_bytes - 1;

    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        dm->bucket_fns[i] = calloc(sizeof(dm->name)*2 + 31, 1);
        memcpy(dm->bucket_fns[i], base_str, base_bytes + intlen - 1);
        increment_str_int(base_str + str_int_offset, &intlen);
    }

    mmap_counter_struct(dm);
}

void free_diskmap(struct diskmap* dm) {
    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        free(dm->bucket_fns[i]);
    }
    free(dm->bucket_fns);
}

void munmap_fine(struct page_tracker* pt) {
    munmap(pt->mapped, pt->n_bytes);
}

/*
 * opportunistically munmap()s memory if no other lookup or insertion is occurring
 * otherwise, leaves it to be free()d at exit
 *
 *   1. increment n_insertions
 *   2. check if n_lookups == 1
 *   3. munmap if 2. is true
 *   4. decrement n_insertions
 *   5. decrement n_lookups // handled by caller
 */
/* this is only to be called from within a lookup() with the guarantee that no insertion is underway */
// this is causing "deadlocks" for concurrent lookups
_Bool _internal_lookup_maybe_munmap(struct page_tracker* pt, _Atomic struct counters* counters) {
    _Bool ret;
    const uint32_t no_insertions = 0;
    int32_t n_attempts = 1;
    struct counters c_ret;

    /* guarantees that no other lookups will begin by spoofing an increase in n_insertions */
    c_ret = update_counter(counters, 0, 1, NULL, &no_insertions, n_attempts, NULL, &ret);
    if (ret) {
        /* munmap() only if we're in the only current lookup thread */
        if (c_ret.lookup_counter == 1) {
            munmap_fine(pt);
        }
        /* no need to delcare ins number lock, at this point it's guaranteed to be 1 */
        /* back to business as usual */

        // looking for a -1 INSERTION, one of these is 

        // OPTION A

        /*this is the problem statement. something's happening here where we maybe don't increment properly first*/
/*
 *         before the below call, c_ret == {1, 1} as expected
 *         but once we enter update_counter(), c == {1, 0}, another lookup() thread may be decrementing
 *         could this be an ABA problem? prob not
 *         also problem isn't solely caused by this, it occurs eventually even if i remove all calls to this function
 * 
 *         hmm, is it possible that we're just not being careful enough when entering critical sections? 
 *         we should maybe be using a lock exp when we're not with update_counter()
 *         as a starter, just make all locks use const like they should
*/


        update_counter(counters, 0, -1, NULL, NULL, -1, NULL, NULL);
    }

    return ret;
}

/* ctrs is set if caller is lookup(), in which case it is not always safe to munmap() due to concurrent lookups
 * if this is set, mmap_fine_optimized() will opportunistically munmap() only if a guarantee of no double mmap()s can be made
 */
void* mmap_fine_optimized(struct page_tracker* pt, _Atomic struct counters* ctrs, int fd, _Bool rdonly, uint32_t offset, uint32_t size) {
    long pgsz = sysconf(_SC_PAGE_SIZE);
    /* calculate starting page */
    uint32_t pgno = offset / pgsz;

    if (pt->n_bytes) {
        if (offset >= pt->byte_offset_start && offset + size <= pt->byte_offset_start + pt->n_bytes) {
            return pt->mapped + offset - pt->byte_offset_start;
        }
        /* it's always safe to munmap() if the caller is insert() */
        if (ctrs) {
            _internal_lookup_maybe_munmap(pt, ctrs);
        } else {
            munmap(pt->mapped, pt->n_bytes);
        }
    }

    pt->byte_offset_start = pgno * pgsz;
    /* we may need to mmap() more than pt->n_pages if our data is between page boundaries */
    pt->n_bytes = MAX(pgsz * pt->n_pages, size + offset - pt->byte_offset_start);

    pt->mapped = mmap(0, pt->n_bytes, rdonly ? PROT_READ : PROT_READ | PROT_WRITE, MAP_SHARED, fd, pt->byte_offset_start);
    assert(!(offset + size > pt->byte_offset_start + pt->n_bytes));
    return pt->mapped + (offset - pt->byte_offset_start);
}


void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], O_CREAT | O_RDWR, S_IRWXU);
    if (fd == -1)
        perror("OPEN");
    uint64_t off = 0;
    off_t insertion_offset = -1;
    uint64_t fsz;
    struct entry_hdr* e;
    uint8_t* data;
    _Bool first = 0;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};

    uint32_t exp_ins = 0, exp_lk = 0;
    uint32_t attempts;

    // this fails sometimes when concurrent lookups == -1
    update_counter(&dm->counter[idx], 0, 1, &exp_lk, &exp_ins, -1, &attempts, NULL);

    if (attempts > 0) {
        printf("acquired target insertion state in %u attempts\n", attempts);
    }

    if ((fsz = lseek(fd, 0, SEEK_END)) == 0) {
        ftruncate(fd, 5 * (sizeof(struct entry_hdr) + keysz + valsz));
        fsz = 5 * sizeof(struct entry_hdr) + (keysz + valsz);
        first = 1;
    }

    lseek(fd, 0, SEEK_SET);
    while (!first && off < fsz) {
        data = mmap_fine_optimized(&pt, NULL, fd, 0, off, sizeof(struct entry_hdr) + keysz + valsz);
        e = (struct entry_hdr*)data;
        if (e->cap + e->ksz + e->vsz == 0) {
            break;
        }
        data = mmap_fine_optimized(&pt, NULL, fd, 0, off, sizeof(struct entry_hdr) + e->cap);
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

    if (insertion_offset + sizeof(struct entry_hdr) + keysz + valsz >= fsz) {
        ftruncate(fd, (fsz = MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz)));
    }

    data = mmap_fine_optimized(&pt, NULL, fd, 0, insertion_offset, sizeof(struct entry_hdr) + keysz + valsz);
    e = (struct entry_hdr*)data;
    e->vsz = valsz;
    e->ksz = keysz;
    if (!e->cap) {
        e->cap = e->vsz + e->ksz;
    }
    memcpy((data + sizeof(struct entry_hdr)), key, keysz);
    memcpy((data + sizeof(struct entry_hdr) + keysz), val, valsz);

    cleanup:
    /* this explicit munmap() is safe, as no other threads will be active in this section */
    munmap_fine(&pt);
    close(fd);
    /*c = atomic_load(&dm->counter[idx]);*/
    // TODO: does this have the intended behavior? is there any chance that counter[idx] has been ptr swapped? nope
    // wait maybe actually, because of the CAS() calls in insert() that wait for 0 0
    // is there a chance that we get 0 0 here, then insert() replaces with 0 1, THEN, we fetch_sub on the local insertion counter. damn...
    // i need to use CAS here as well potentially, replace them with
    // i shoudl only access using one method or another i believe

    /*
     * while (1) {
     *     c = atomic_load(&dm->counter[idx]);
     *     target.insertion_counter = c.insertion_counter - 1;
     *     target.lookup_counter = c.lookup_counter;
     *     atomic_compare_exchange_strong(&dm->counter[idx], &c, target);
     * }
    */
    // TODO: do i need to explicitly CAS() with (0, 1)? shouldn't have to

    // OPTION B
    update_counter(&dm->counter[idx], 0, -1, NULL, NULL, -1, NULL, NULL);

    /*atomic_fetch_sub(&c.insertion_counter, 1);*/
}

/* TODO: is lock free approach worth the added complexity? run some tests */
_Bool lookup_diskmap_internal(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val,
                              _Bool delete, _Bool check_vsz_only, void (*foreach_func)(uint32_t, void*, uint32_t, uint8_t*), int idx_override) {
    int idx = (foreach_func) ? idx_override : dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], delete ? O_RDWR : O_RDONLY);
    _Bool ret = 0;
    off_t fsz;
    off_t off = 0;
    struct entry_hdr* e;
    uint8_t* data;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};
    uint32_t attempts;
    uint32_t n_lookups, expected_insertions;
    struct counters c_res;

    /*spinning will be built into update_counter()*/
    /*need to increment lookup_counter, wait until there are no further insertions*/

/*
 * this will spin until we can update lookup, need to explicitly spin
 * as well until there are no further lookup
 * maybe i need to use a target state for one or both, 
 * potentially allow this as an arg
*/

/*
 * below we need to:
 *     1: increment lookup counter
 *     2: wait for insertion_counter == 0
 * luckily, this can easily be achieved with the new update_counter()
*/

    /*update_counter(&dm->counter[idx], 1, 0, NULL, NULL, NULL);*/
    /*n_lookups = 1 + atomic_fetch_add(&dm->counter[idx].lookup_counter, 1);*/
    /* spin until all insertions are complete, we can guarantee
     * that no new insertions will begin because lookup_counter is nonzero
     */
    /*while (atomic_load(&dm->counter[idx].insertion_counter)) {*/
        /*++attempts;*/
    /*}*/

    expected_insertions = 0;
    /*puts("attempting lkup");*/
    // we get stuck attempting lookup sometimes, look into why. what's state here?
    // probably has to do with insertion counter, actually definitely does, as this is the only
    // reason we ever do not enter this critical region
    if (!foreach_func) {
        c_res = update_counter(&dm->counter[idx], 1, 0, NULL, &expected_insertions, -1, &attempts, NULL);
        /*puts("done");*/
        n_lookups = c_res.lookup_counter;

        if (attempts > 0) {
            printf("took %u attempts to safely begin a lookup\n", attempts);
        }

        if (n_lookups > 1) {
            printf("%u concurrent lookups!\n", n_lookups);
        }
    }

    if ((fsz = lseek(fd, 0, SEEK_END)) <= 0) {
        ret = NULL;
        *valsz = 0;
        goto cleanup;
    }
    lseek(fd, 0, SEEK_SET);

    while (off < fsz) {
        e = mmap_fine_optimized(&pt, &dm->counter[idx], fd, 1, off, sizeof(struct entry_hdr) + (keysz * 2));
        if (!(e->ksz || e->vsz)) {
            goto cleanup;
        }
        if (e->vsz == 0) {
            puts("found empty vsz");
        }
        if (e->vsz == 0 || e->ksz != keysz) {
            off += sizeof(struct entry_hdr) + e->cap;
            continue;
        }

        /* we don't need to mmap() e->cap here, only grab relevant bytes */
        e = mmap_fine_optimized(&pt, &dm->counter[idx], fd, 1, off, sizeof(struct entry_hdr) + e->ksz + e->vsz);
        data = (uint8_t*)e + sizeof(struct entry_hdr);

        if (foreach_func) {
            /*aha, the trick is that we need to offset keysz from data, this is awesome because it means we can include keysz, key*/
            foreach_func(e->ksz, data, e->vsz, data + e->ksz);
            off += sizeof(struct entry_hdr) + e->cap;
            continue;
        }

        else {
            if (memcmp(data, key, keysz)) {
                off += sizeof(struct entry_hdr) + e->cap;
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
    }

    cleanup:
    if (!foreach_func) {
        /* this could hang if the foreach plans to alter data bc we'll have nonzero insertions */
        _internal_lookup_maybe_munmap(&pt, &dm->counter[idx]);

        /*atomic_fetch_sub(&dm->counter[idx].lookup_counter, 1);*/
        update_counter(&dm->counter[idx], -1, 0, NULL, NULL, -1, NULL, NULL);
    }
    close(fd);

    return ret;
}

_Bool lookup_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val) {
    return lookup_diskmap_internal(dm, keysz, key, valsz, val, 0, 0, NULL, -1);
}

/*
 * void foreach_diskmap_internal(struct diskmap* dm, void* funcptr) {
 *     for (uint32_t i = 0; i < dm->n_buckets; ++i) {
 * #if 0
 *     hmm, i think i need to mmap_fine_optimized() for each bucket fn 
 *     then use internal lookup logic, split this out of the function to reuse code - starting at `while (off < fsz)`
 *     actually we just need this internal logic - get the fd, go from 0->fsz
 * 
 *     int fd = open(dm->bucket_fns[idx], delete ? O_RDWR : O_RDONLY);
 * #endif
 * 
 *     }
 * }
*/

void foreach_diskmap_const(struct diskmap* dm, uint32_t keysz, void (*funcptr)(uint32_t, void*, uint32_t, uint8_t*)) {
    uint32_t valsz;
    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        update_counter(&dm->counter[i], 1, 0, NULL, NULL, -1, NULL, NULL);
        lookup_diskmap_internal(dm, keysz, NULL, &valsz, NULL, 0, 1, funcptr, i);
        update_counter(&dm->counter[i], -1, 0, NULL, NULL, -1, NULL, NULL);
    }
}

void foreach_diskmap(struct diskmap* dm, uint32_t keysz, void (*funcptr)(uint32_t, void*, uint32_t, uint8_t*)) {
    uint32_t valsz;
    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        update_counter(&dm->counter[i], 0, 1, NULL, NULL, -1, NULL, NULL);
        lookup_diskmap_internal(dm, keysz, NULL, &valsz, NULL, 0, 1, funcptr, (int)i);
        update_counter(&dm->counter[i], 0, -1, NULL, NULL, -1, NULL, NULL);
    }
}

/* remove_key_diskmap() sets e->vsz to 0, marking the region as reclaimable */
_Bool remove_key_diskmap(struct diskmap* dm, uint32_t keysz, void* key) {
    uint32_t valsz;
    return lookup_diskmap_internal(dm, keysz, key, &valsz, NULL, 1, 0, NULL, -1);
}

_Bool check_valsz_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz) {
    return lookup_diskmap_internal(dm, keysz, key, valsz, NULL, 0, 1, NULL, -1);
}
