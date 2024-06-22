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
    /*int offset_range;*/
    uint32_t byte_offset_start, n_bytes;

    uint8_t* mapped;
};

// creates an mmap()'d file or opens one if it exists
// and updates dm->bucket_locks
void mmap_locks(struct diskmap* dm) {
    char lock_fn[30] = {0};
    int fd;
    int target_sz; 
    _Bool exists;

    snprintf(lock_fn, sizeof(lock_fn), "%s/%s.LOCK", dm->name, dm->name);
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

void init_diskmap(struct diskmap* dm, uint32_t n_pages, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t)) {
    strcpy(dm->name, map_name);
    /* TODO: fix perms */
    mkdir(dm->name, 0777);
    /*dm->entries_in_mem = 19;*/
    dm->hash_func = hash_func;
    dm->n_buckets = n_buckets;
    /* TODO: consolidate these into a struct */
    dm->bucket_fns = malloc(sizeof(char*) * dm->n_buckets);
    dm->pages_in_memory = n_pages;
    /*
     * memset(&dm->pt, 0, sizeof(struct page_tracker));
     * dm->pt.n_pages = n_pages;
    */
    for (uint32_t i = 0; i < dm->n_buckets; ++i) {
        dm->bucket_fns[i] = calloc(sizeof(dm->name)*2 + 31, 1);
        snprintf(dm->bucket_fns[i], sizeof(dm->name)  + 31 + sizeof(dm->name), "%s/%s_%u", dm->name, dm->name, i);
    }
    mmap_locks(dm);
}

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

void munmap_fine(struct page_tracker* pt) {
    munmap(pt->mapped, pt->n_bytes);
}

// TODO: this breaks when the number of pages that include (offset + size) > pt->n_pages
// AH! in this case, we can just return mmap_fine()!
// nvm... we can just add to n_bytes!!! we don't have to mmap() an exact number of pages
void* mmap_fine_optimized(struct page_tracker* pt, int fd, off_t offset, uint32_t size) {
    long pgsz = sysconf(_SC_PAGE_SIZE);
    // starting page
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
    // mmap()ing size bytes if our data spans more pages than is "allowed"
    /*printf("nbytes = MAX(%li, %li)\n", pgsz * pt->n_pages, size + offset - pt->byte_offset_start);*/
    /* we may need to mmap() more than pt->n_pages if our data is between page boundaries */
    pt->n_bytes = MAX(pgsz * pt->n_pages, size + offset - pt->byte_offset_start);


    pt->mapped = mmap(0, pt->n_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, pt->byte_offset_start);
    /*printf("re-MMAP required. %i -> %i\n", pt->byte_offset_start, pt->byte_offset_start + pt->n_bytes);*/
    assert(!(offset + size > pt->byte_offset_start + pt->n_bytes));
    /*
     * found the problem, as suspected, it's about boundary offsets i believe
     * sometimes more than n_pages is required
    */
    return pt->mapped + (offset - pt->byte_offset_start);
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
/*insertions get veeeeery slow when we have to check for duplicates*/
void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
    int fd = open(dm->bucket_fns[idx], O_CREAT | O_RDWR, S_IRWXU);
    if (fd == -1)
        perror("OPEN");
    /* TODO: need to truncate file to correct size if file does not exist, maybe like 2x needed size */
    off_t off = 0;
    off_t insertion_offset = -1;
    // TODO: remove the dm->* that are just these, no need to record state?
    off_t fsz;
    /*size_t munmap_sz[2];*/
    struct entry_hdr* e;
    /*uint8_t* data[2];*/
    uint8_t* data;
    _Bool first = 0;
    struct page_tracker pt = {.n_pages = dm->pages_in_memory, .byte_offset_start = 0, .n_bytes = 0};
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
        ftruncate(fd, 5 * (sizeof(struct entry_hdr) + keysz + valsz));
        fsz = 5 * sizeof(struct entry_hdr) + (keysz + valsz);
        first = 1;
    }
    lseek(fd, 0, SEEK_SET);
    /*bucket sizes should not be used*/
    /*for (uint16_t i = 0; i < dm->bucket_sizes[idx]; ++i) {*/
    // TODO something is up here, we're adding zeroed padding to the beginning of the file
    // TODO: clean this all up, we shouldn't be individually munmap()ing
    /*while (!first && off < fsz) {*/
    // TODO: there could still be a circumstance where this fails - if we have enough room for header but not
    // fields
    // need to add a check below as wll
    /*while (!first && (fsz - off) > (long)sizeof(struct entry_hdr)) {*/
    while (!first && off < fsz) {
        /*printf("%li <= %li\n", off, fsz);*/
        // TODO: all mmap() calls must use an offset divisible by page size ...
        /*e = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, off);*/
        /*UGH. i need to separately mmap() for keysz, valsz because we need keysz/valsz for each individual entry*/
        /*printf("reading from offset %li\n", off);*/
        /*printf("adtnl offset: %li\n", adtnl_offset[0]);*/
        // TODO: get rid of munmap()s, these will be handled by *_optimized()

        /*data[0] = mmap_fine(fd, off, sizeof(struct entry_hdr), adtnl_offset, munmap_sz);*/
        /*adding an educated guess for ksz/vsz so we hopefully don't need to re-mmap() for second mmap_fine_optimized() call*/
        /*
         * i honestly bet that our educated guess stuff is causing the seg fault
         * actually maybe not...
         * but maybe we're requesting too much memory
        */
        // there's a chance that this is beyond our file, when does this happen?
        // we need to ftruncate() the file, i found the cause of the crash
        // TODO: why are we reading an invalid number of bytes? when would this happen?
        // TODO: we must be doing some weird arithmetic somewhere
        // we shouldn't need to grow our file while iterating through it
        // TODO: maypbe it's a problem with off +=
        // nvm, i think it could just be because we're doing such a rough estimate above! obviously it's not a perfectly formed
        // file!! we need to just exit once we've reached a point where there can be no more valid entries!
        // look also into how we're ftruncating, 
        //
        // hmmm, it would be convenient to store something like final_offset, which would be the offset
        // of the final entry
        // could store this in entry, just one byte for y/n final entry
        // OR could just be a little smarter about the while condition
        //
        //
        // okay, so we're going along and keep finding empty entries, this is NP. we += off each time
        // in our last iteration, we will have set off to a value where entry_hdr + ... is unavailable
        data = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + keysz + valsz);
        e = (struct entry_hdr*)data;

        /*printf("adtnl offset: %li\n", adtnl_offset[0]);*/
        /*e = (struct entry_hdr*)(data[0] + adtnl_offset[0]);*/
        /*off += sizeof(struct entry_hdr);*/

        /*data[1] = mmap_fine(fd, off + sizeof(struct entry_hdr), e->ksz + e->vsz, adtnl_offset+1, munmap_sz+1);*/
        // TODO: move this after the next check

        /*
         * oof, this is bad. can't have multiple calls to *_optimized() and expect both to be mmap()d
         * there's a chance e may be munmap()d by this call
        */

        // we need to re-mmap() in case e->ksz + e->vsz != keysz + valsz
        /*wow! this is the crash now*/
        /*data = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + e->ksz + e->vsz);*/
        // e->cap must be mmap()d in case we defragment below
        // // crash is here
        // crash is here
        // TODO: is this necessary? I believe it is - we can't have a more elegant check for this because
        // we don't know e->cap in while condition
        /*printf("%li < %i + %li\n", fsz, e->ksz, off);*/
        // if e->cap == 0 || e->vsz == 0 then this is uncharted territory - an empty ftruncate()d region
        if (e->cap + e->ksz + e->vsz == 0) {
            /*this seems too good to be true, this is being triggered early on it seems*/
            /*puts("SAVING SOME CYCLES, exiting early :)");*/
            break;
            /*goto cleanup;*/
        }
        data = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + e->cap);
        e = (struct entry_hdr*)data;
        data += sizeof(struct entry_hdr);
        /*ftruncate(fd, fsz = (fsz * 2));*/

        /*printf("data[0] == %p, data[1] == %p\n", data[0], data[1]);*/
        /*printf("e->ksz: %u, e->vsz: %u\n", e->ksz, e->vsz);*/
        /*e = (struct entry_hdr*)(data + adtnl_offset);*/

        /*page = mmap(0, sizeof(struct entry_hdr), PROT_READ | PROT_WRITE, MAP_SHARED, fd, pgsz * pg_idx);*/

        /* if we've found a fragmented entry that will fit our new k/v pair */
        /*if (e->vsz == 0 && e->ksz >= (keysz + valsz)) {*/
        if (e->vsz == 0 && e->cap >= (keysz + valsz)) {
            insertion_offset = off;
            /*munmap(data[0], munmap_sz[0]);*/
            /*munmap(data[1], munmap_sz[1]);*/
            break;
            /*printf("found internal fragmented offset at %li\n", insertion_offset);*/
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

            /*printf("found identical keysz of %i\n", keysz);*/
            if (!memcmp(data, key, keysz)) {
                /*printf("found identical KEY of %s!\n", (char*)key);*/
                /* overwrite entry and exit if new val fits in old val allocation
                 * otherwise, we have to fragment the bucket and erase this whole entry
                 */
                /*
                 * omg, the actual problem! we're not keeping track of the size difference
                 * it would seem by looking at the updated e->ksz + e->vsz that we've allocated a smaller
                 * region that we actually have for our data
                 * we need a new field - e->padding or e->cap
                 * USE NEW FIELD HERE
                */
                /*if (valsz <= e->vsz) {*/
                if (valsz <= e->cap - e->ksz) {
                    /*puts("found a region to fit new val");*/
                    // this makes insertion offset wild
                    /*hmmm, this still causes issue and it seems it isn't due to weird insertion sizes*/

                    /* does this corrupt some part of scanning? if we keep reading from start, incrementing, etc.i think it should be fine because we
                     *
                     */
                    
                    /*e->vsz -= 5;*/
                    
                    /*printf("e->vsz %i -> %i, cap remains: %i\n", e->vsz, valsz, e->cap);*/
                    e->vsz = valsz;
                    memcpy(data + keysz, val, valsz);
                    /*munmap(data[0], munmap_sz[0]);*/
                    /*munmap(data[1], munmap_sz[1]);*/
                    goto cleanup;
                }
                // how do i mark this section as invalid without removing info about where next entry begins?
                // easy - if value is set to 0, we will understand key as meaning where next chunk starts
                // this is a great solution because is still allows us to increment off by ksz + vsz
                /*memset(e, 0, sizeof(struct entry_hdr));*/
                e->ksz += e->vsz;
                e->vsz = 0;
                /* aha, i think this is the culprit! if valsz > e->vsz! we haven't mmap()d enough */
                /*printf("marked region at off %li for overwriting due to lack of space for new value\n", off);*/
                // hmm, maybe i shouldn't decrement size because size still is taken up
                // TODO: make this more elegant, no reason to have two separate incrementations of off
                /*maybe remove this... we're incrementing twice sometimes!*/
                /*off += sizeof(struct entry_hdr) + e->ksz + e->vsz;*/
                /*munmap(data[0], munmap_sz[0]);*/
                /*munmap(data[1], munmap_sz[1]);*/
                break;
                // instead of breaking, we can just ensure that this is the last iteration and continue
                // this way we don't have to explicitly increment off again
            }
        }
        // TODO: not sure if mmap increments filepos or if i need to use off
        // problem is here, e->ksz is impossibly large! 4160495616
        // we're either reading from the wrong spot or ftruncate() doesn't zero the extended region
        off += sizeof(struct entry_hdr) + e->cap;
        /*puts("incremented off");*/
        /*munmap(data[0], munmap_sz[0]);*/
        /*munmap(data[1], munmap_sz[1]);*/
    }

    /* this is reached if no duplicates are found OR a k/v pair now requires more space */
    /*TODO:need to truncate file to fit new entry*/
    /*if ()*/
    /*first, we check if we have an insertion_offset that will fit this. this will allow us to defragment a portion of our bucket*/
    /*if (fsz <)*/
    /*okay, we can calculate bytes_in_use from the loop above. we shouldn't rely on any state anyway!*/
    #if 0
    if (insertion_offset == -1 && (off + sizeof(struct entry_hdr) + keysz + valsz >= (uint64_t)fsz)) {
        /*puts("growing file...");*/
        puts("RESIZE");
        ftruncate(fd, (fsz = MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz)));
    }
    #endif
    /*if (insertion_offset != -1) {*/
    /*}*/
    if (insertion_offset == -1) {
        insertion_offset = off;
    }

    if (insertion_offset + sizeof(struct entry_hdr) + keysz + valsz >= (uint64_t)fsz) {
        /*printf("resize needed: %li\n", fsz);*/
        ftruncate(fd, (fsz = MAX(fsz * 2, fsz + sizeof(struct entry_hdr) + keysz + valsz)));
        /*printf("resize needed: %li\n", fsz);*/
    }
    /*munmap_fine(&pt);*/
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

    /*printf("setting k/v sz: %i %i %i %i\n", e->vsz, e->ksz, valsz, keysz);*/
    /*uhh, insertion offset is absolutely insane*/
    // there's a chance we need to re-truncate here
    data = mmap_fine_optimized(&pt, fd, insertion_offset, sizeof(struct entry_hdr) + keysz + valsz);
    e = (struct entry_hdr*)data;
    // hmm, this is segfaulting when we set it above. weird.
    /*
     * USE NEW FIELD HERE!
     * we're corrupting our map by disregarding capacity of this entry
    */
    e->vsz = valsz;
    e->ksz = keysz;
    if (!e->cap) {
        e->cap = e->vsz + e->ksz;
    }
    /*e->cap = MAX(e->vsz + e->ksz, e->cap);*/
    /*printf("writing new entry to offset %li\n", insertion_offset);*/
    memcpy((data + sizeof(struct entry_hdr)), key, keysz);
    memcpy((data + sizeof(struct entry_hdr) + keysz), val, valsz);
    /*munmap(data[0], munmap_sz[0]);*/


    cleanup:
    munmap_fine(&pt);
    close(fd);
    pthread_mutex_unlock(dm->bucket_locks + idx);

    /*printf("exiting call with size (%i,%i)\n", keysz, valsz);*/
}

_Bool lookup_diskmap_internal(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val, _Bool delete) {
    int idx = dm->hash_func(key, keysz, dm->n_buckets);
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

    /*int iter = 0;*/
    while (off < fsz) {
        /*printf("iteration %i - off %li, fsz %li\n", iter++, off, fsz);*/
        e = mmap_fine_optimized(&pt, fd, off, sizeof(struct entry_hdr) + (keysz * 2));
        if (!(e->ksz || e->vsz)) {
            // why are we finding NIL entries in the beginning? look into insert
            /*printf("found NIL entry, exiting\n");*/
            /*munmap(data[0], munmap_sz[0]);*/
            goto cleanup;
        }
        /*perror("mm");*/
        // USE NEW FIELD HERE
        off += sizeof(struct entry_hdr);
        /* e->* == 0, hmm */
        if (e->vsz == 0 || e->ksz != keysz) {
            /*printf("found bad ksz or deleted field, incrementing off by %i OR %i\n", e->ksz + e->vsz, e->cap);*/
            off += e->cap;
            /*munmap(data[0], munmap_sz[0]);*/
            continue;
        }
        /* we don't need to mmap() e->cap here, only grab relevant bytes */
        data = mmap_fine_optimized(&pt, fd, off, e->ksz + e->vsz);
        /*something's off with our incrementation of off*/
        if (memcmp(data, key, keysz)) {
            off += e->cap;
            /*munmap(data[0], munmap_sz[0]);*/
            /*munmap(data[1], munmap_sz[1]);*/
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
