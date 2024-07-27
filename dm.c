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

#include "dm.h"

struct page_tracker{
    uint32_t n_pages;
    uint32_t byte_offset_start, n_bytes;

    uint8_t* mapped;
};

/* creates an mmap()'d file or opens one if it exists
 * and updates dm->bucket_locks
 */
void mmap_locks(struct diskmap* dm) {
    char lock_fn[30] = {0};
    int fd;
    int target_sz; 
    _Bool exists;
    pthread_mutexattr_t attr;

    snprintf(lock_fn, sizeof(lock_fn), "%s/%s.LOCK", dm->name, dm->name);
    fd = open(lock_fn, O_CREAT | O_RDWR, S_IRWXU);
    target_sz = (sizeof(pthread_mutex_t) * dm->n_buckets);
    exists = lseek(fd, 0, SEEK_END) >= target_sz;
    lseek(fd, 0, SEEK_SET);
    if (!exists) {
        ftruncate(fd, target_sz);
        pthread_mutexattr_setpshared(&attr, 1);
    }

    dm->bucket_locks = mmap(0, target_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (dm->bucket_locks == MAP_FAILED) {
        perror("mmap()");
    }

    if (!exists) {
        for (uint32_t i = 0; i < dm->n_buckets; ++i) {
            pthread_mutex_init(dm->bucket_locks + i, &attr);
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

    mmap_locks(dm);
}

void munmap_fine(struct page_tracker* pt) {
    munmap(pt->mapped, pt->n_bytes);
}

void* mmap_fine_optimized(struct page_tracker* pt, int fd, off_t offset, uint32_t size) {
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


    pt->mapped = mmap(0, pt->n_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, pt->byte_offset_start);
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

    pthread_mutex_lock(dm->bucket_locks + idx);
    if ((fsz = lseek(fd, 0, SEEK_END)) == 0) {
        ftruncate(fd, 5 * (sizeof(struct entry_hdr) + keysz + valsz));
        fsz = 5 * sizeof(struct entry_hdr) + (keysz + valsz);
        first = 1;
    }
    lseek(fd, 0, SEEK_SET);
    while (!first && off < fsz) {
        data = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + keysz + valsz);
        e = (struct entry_hdr*)data;
        if (e->cap + e->ksz + e->vsz == 0) {
            break;
        }
        data = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + e->cap);
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

    data = mmap_fine_optimized(&pt, fd, insertion_offset, sizeof(struct entry_hdr) + keysz + valsz);
    e = (struct entry_hdr*)data;
    e->vsz = valsz;
    e->ksz = keysz;
    if (!e->cap) {
        e->cap = e->vsz + e->ksz;
    }
    memcpy((data + sizeof(struct entry_hdr)), key, keysz);
    memcpy((data + sizeof(struct entry_hdr) + keysz), val, valsz);

    cleanup:
    munmap_fine(&pt);
    close(fd);
    pthread_mutex_unlock(dm->bucket_locks + idx);
}

_Bool lookup_diskmap_internal(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val, _Bool delete) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    /* TODO why doesn't O_RDONLY work for lookups */ 
    /*int fd = open(dm->bucket_fns[idx], O_RDONLY);*/
    int fd = open(dm->bucket_fns[idx], O_RDWR);
    _Bool ret = 0;
    off_t fsz;
    off_t off = 0;
    struct entry_hdr* e;
    uint8_t* data;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};
    pthread_mutex_lock(dm->bucket_locks + idx);
    if ((fsz = lseek(fd, 0, SEEK_END)) <= 0) {
        ret = NULL;
        *valsz = 0;
        goto cleanup;
    }
    lseek(fd, 0, SEEK_SET);

    while (off < fsz) {
        e = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + (keysz * 2));
        if (!(e->ksz || e->vsz)) {
            goto cleanup;
        }
        off += sizeof(struct entry_hdr);
        if (e->vsz == 0 || e->ksz != keysz) {
            off += e->cap;
            continue;
        }
        /* we don't need to mmap() e->cap here, only grab relevant bytes */
        data = mmap_fine_optimized(&pt, fd, off, e->ksz + e->vsz);
        if (memcmp(data, key, keysz)) {
            off += e->cap;
            continue;
        }

        ret = 1;
        *valsz = e->vsz;

        if (delete) {
            e->ksz += e->vsz;
            e->vsz = 0;
            break;
        }

        memcpy(val, data + keysz, *valsz);
        break;
    }

    cleanup:
    close(fd);
    munmap_fine(&pt);
    pthread_mutex_unlock(dm->bucket_locks + idx);
    return ret;
}

_Bool lookup_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val) {
    return lookup_diskmap_internal(dm, keysz, key, valsz, val, 0);
}

/* remove_key_diskmap() sets e->vsz to 0, marking the region as reclaimable */
_Bool remove_key_diskmap(struct diskmap* dm, uint32_t keysz, void* key) {
    uint32_t valsz;
    return lookup_diskmap_internal(dm, keysz, key, &valsz, NULL, 1);
}
