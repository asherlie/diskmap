#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

struct entry_hdr{
    /* cap is necessary to keep track of the amount of bytes taken up by an entry
     * this may shift over time as smaller entries are used to de-fragment larger ones
     * that have room
     * cap stores the largest ksz + vsz
     */
    uint32_t ksz, vsz, cap;
};

struct diskmap{
    char name[12];
    int (*hash_func)(void*, uint32_t, uint32_t);
    uint32_t n_buckets;
    char** bucket_fns;
    pthread_mutex_t* bucket_locks;

    /*
     * ah, this can't be used because this must be specific to a file...
     * we really could have one for each file
     * maybe have this be an option, if enabled we have one page_tracker for each bucket
     * each with n_pages of designated memory
     * this way we can leverage the page tracker between calls
     * this could speed up consecutive insertions significantly
     * i'll add
     * struct page_tracker* page_trackers;
     * this is optionally initialized in init_diskmap()
     * if the seting is turned off, we'll just use it within insertions
     * otherwise, we'll use our idx's specific page_tracker
     *
     *
     * honestly not sure how big a benefit this will be because page_tracker is the most helpful for very large
     * files and for these files, we'll need to adjust the window being used anyway - especially because
     * we'll finish an insertion with the window at the end of our file
     * and next call it'll be shifted to the start. yep, probably not worth it.
     *
     * it may help for many consecutive insertions but my hunch is that duplicate checking renders it pretty much
     * useless
    */
    uint32_t pages_in_memory;
};

void init_diskmap(struct diskmap* dm, uint32_t n_pages, uint32_t n_buckets, char* map_name, int (*hash_func)(void*, uint32_t, uint32_t));
void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val);
_Bool remove_key_diskmap(struct diskmap* dm, uint32_t keysz, void* key);
_Bool lookup_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val);
_Bool check_valsz_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz);
