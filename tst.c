#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>

#include <dm.h>
/*#include "easy_dm.h"*/

int hash(void* key, uint32_t keysz, uint32_t n_buckets) {
    if (keysz < sizeof(int)) {
        return 9 % n_buckets;
    }
    return *((int*)key) % n_buckets;
}

struct ins_arg{
    struct diskmap* dm;

    int n_insertions, start_val;
};

void* insert_th(void* v_ins_arg) {
    struct ins_arg* ia = v_ins_arg;
    uint32_t kv;

    for (int i = 0; i < ia->n_insertions; ++i) {
        kv = ia->start_val + i;
        insert_diskmap(ia->dm, 4, 4, &kv, &kv);
        /*printf("inserted %i: %i\n", kv, kv);*/
    }
    free(ia);
    return NULL;
}

/*
 * void insert_diskmap(struct diskmap* dm, uint32_t keysz, uint32_t valsz, void* key, void* val);
 * _Bool remove_key_diskmap(struct diskmap* dm, uint32_t keysz, void* key);
 * _Bool lookup_diskmap(struct diskmap* dm, uint32_t keysz, void* key, uint32_t* valsz, void* val);
 * 
*/

// this will do concurrent lookups to make sure the feature is actually working
// i'll also need a test to atempt concurrent insertions to make sure nothign gets corrupted
// with the new lock free approach

// ah, don't need new tests just yet, running the current code in multiple processes fails
void concurrent_lookup_speed_test(void) {
}

uint32_t persistent_test(void) {
    struct diskmap dm;
    int keysz = 6;
    uint32_t vsz = 2;
    char key[6] = "sher";
    char* lval;

    srandom(time(NULL));

    init_diskmap(&dm, 10, 10000, "PERSISTENT", hash);
    if (check_valsz_diskmap(&dm, keysz, key, &vsz)) {
        /*printf("found k/v pair with vsz: %i\n", vsz);*/
        lval = calloc((vsz * 2) + 1, 1);
        /*printf("lval: %p\n", lval);*/
        lookup_diskmap(&dm, keysz, key, &vsz, lval);
        /*printf("found \"%s\"\n", lval);*/
        lval[vsz] = 'a' + rand() % 26; // this is an invalid write apparently, not enough lval is calloc'd. is this free()d btw?
        // AHA! this exposed an issue with this test!!! there's no guarantee that no other values are inserted between check() and lookup()
        // i've fixed this by allocating twice as much mem as needed
        key[0] = rand();
        key[1] = rand();
        key[2] = rand();
        key[3] = rand();
        insert_diskmap(&dm, keysz, vsz+1, key, lval);
        free(lval);
    } else {
        printf("did not find k/v pair. inserting starter string\n");
        insert_diskmap(&dm, keysz, 1, key, "0");
    }
    /*lookup_diskmap(&dm, keysz, key, &vsz);*/
    free_diskmap(&dm);
    return vsz;
}

void create_n_buckets_test(int n_buckets) {
    struct diskmap dm;
    init_diskmap(&dm, 1, n_buckets, "enby", hash);
    for (int i = 0; i < n_buckets; ++i) {
        insert_diskmap(&dm, sizeof(int), sizeof(int), &i, &i);
    }
}

// TODO: return elapsed time
void large_insertion_test(int n_buckets, int n_threads, int ins_per_thread) {
    pthread_t pth[n_threads];
    struct ins_arg* ia;
    struct diskmap dm;
    uint32_t valsz, lval;
    // next step is to speed up low bucket insertions, checking for duplicates is incredibly slow
    // hmm, this crash seems to have something to do with page numbers
    // maybe we're on a page boundary and don't have logic to grab data from both - do some MAX()
    // to expose this further, use just one page
    /*init_diskmap(&dm, 10, n_buckets, "LIT", hash);*/
    // hmm, if n_pages is too low we get bus errors, maybe we're not munmap()ing properly and too many maps remain
    /*init_diskmap(&dm, 10, n_buckets, "LIT", hash);*/
    init_diskmap(&dm, 90, n_buckets, "LIT", hash);
    /*puts("INITIALIZED");*/

    for (int i = 0; i < n_threads; ++i) {
        ia = malloc(sizeof(struct ins_arg));
        ia->dm = &dm;
        ia->start_val = i * ins_per_thread;
        ia->n_insertions = ins_per_thread;
        pthread_create(pth+i, NULL, insert_th, ia);
    }
    /*puts("CREATED threads");*/
    for (int i = 0; i < n_threads; ++i) {
        pthread_join(pth[i], NULL);
        printf("joined thread %i\n", i);
    }
    if (0 && n_threads == 1) {
        for (uint32_t i = 0; i < (uint32_t)ins_per_thread; ++i) {
            lookup_diskmap(&dm, 4, &i, &valsz, &lval);
            assert(lval == i);
            if (lval != i) {
                printf("%i != %i\n", lval, i);
                /*weird, investigate this!*/
            }
        }
    }
}

void isolate_bug(void) {
    struct diskmap dm;
    uint32_t valsz, lval;

    init_diskmap(&dm, 1, 1, "bug", hash);

    (void)valsz;
    (void)lval;

    for (int i = 0; i < 10021; ++i) {
        insert_diskmap(&dm, 4, 4, &i, &i);
        lookup_diskmap(&dm, 4, &i, &valsz, &lval);
        if (valsz != 4) {
            printf("bad valsz for %u\n", i);
        }
        if (lval != (unsigned)i) {
            printf("bad val for %u, != %u\n", i, lval);
        }
    }
}

#ifdef REGISTER_TST
REGISTER_DISKMAP(intint, int, int, NULL)
#endif

void pagesz_insertion_tst(void) {
    struct diskmap dm;
    long pgsz = sysconf(_SC_PAGE_SIZE);
    uint32_t valsz = pgsz - sizeof(struct entry_hdr) - sizeof(int) + 1;
    uint32_t valsz_lookup;
    uint8_t* data = malloc(valsz);
    int key = 0;
    init_diskmap(&dm, 1, 1, "PSIT", hash);

    insert_diskmap(&dm, sizeof(int), valsz, &key, data);
    check_valsz_diskmap(&dm, sizeof(int), &key, &valsz_lookup);

    if (valsz != valsz_lookup) {
        printf("%i != %i\n", valsz, valsz_lookup);
    } else {
        puts("SUCCESS");
    }
}

// TODO: confirm that insertions are succeeding and can be popped properly
// potentially write this into large_insertion_test()
// OMG! i think that this is the first out of page value that we see!!! let's see if i increase n_pages if it doubles!
/*
 * yep, it roughly doubled. we need to fix some weird page bytes issue. maybe it's a problem with npages loaded in
 * based on size
*/
/*works just fine on thinkpad, seeing state 0 -1 on raspbi though, some bad stuff somewhere, maybe due to lack of memory fence*/

void p_foreach(uint32_t keysz, void* key, uint32_t valsz, uint8_t* data) {
    int k;
    /*printf("%u, %p, %u, \"%s\"\n", keysz, key, valsz, data);*/
    memcpy(&k, key, keysz);
    printf("%u, %i, %u, \"%s\"\n", keysz, k, valsz, data);
    /*printf("%u, \"%s\"\n", valsz, data);*/
}

void foreach_test() {
    struct diskmap dm;
    int key = 3;
    char str[16] = "asher";
    init_diskmap(&dm, 1, 1, "foreach", hash);

    insert_diskmap(&dm, sizeof(int), 6, &key, str);
    key = 2;
    insert_diskmap(&dm, sizeof(int), 6, &key, str);
    key = -1;
    insert_diskmap(&dm, sizeof(int), 6, &key, str);
    key = -331;
    insert_diskmap(&dm, sizeof(int), 6, &key, str);
    key = 991;
    str[0] = 'Z';
    insert_diskmap(&dm, sizeof(int), 6, &key, str);

    key = 1991;
    str[5] = 'a';
    insert_diskmap(&dm, sizeof(int), 6, &key, str);

    puts("foreach_diskmap():");
    foreach_diskmap(&dm, sizeof(int), p_foreach);

    puts("foreach_diskmap_const():");
    foreach_diskmap_const(&dm, sizeof(int), p_foreach);

}

int main(void) {
    foreach_test();
    /*return 0;*/
    /*
     * create_n_buckets_test(1000);
     * return 0;
    */
    /*
     * for (int i = 0; i < 4080; ++i) {
     *     persistent_test();
     * }
     * return 0;
    */
    /*
     * pagesz_insertion_tst();
     * return 1;
    */
    uint32_t prev_p = persistent_test(), p;
    int total = 10000;
    /*while (1) {*/
    for (int i = 0; i < total; ++i) {
        /*printf("%i\n", i);*/
        /*printf("\r%.2lf%%   ", (double)(i*100)/total);*/
        printf("\r%i%%   ", (i*100)/total);
        /*uhh, this stops at 4078, there's some weird issue
         * with page size
         */ 
        p = persistent_test();
        // removing this due to paralel testing, this does not hold true if two processes are inserting simultaneously
        #if 0
        if (p < prev_p) {
            printf("%i < %i\n", p, prev_p);
            break;
        }
        #endif
        prev_p = p;
        (void)prev_p;
    }
    return 0;
    /*struct __internal_registered_dm_intint ii;*/
    /*isolate_bug();*/
    /*return 1;*/
    #ifdef REGISTER_ST
    intint x;
    insert_intint(&x, 4, 22);
    #endif
    clock_t st;
    double elapsed;
    st = clock();
    // this crashes with a very large number of insertions and < 10000 bucket entries
    // why?
    // 13 crashes, 12 stable
    /*large_insertion_test(1, 1, 1021);*/
    /*large_insertion_test(1, 1, 1020);*/
    // TODO: why does this fail when run from two processes concurrently?
    // one process hangs
    large_insertion_test(10000, 10, 50000);
    /*large_insertion_test(10, 10, 100);*/
    elapsed = ((double)(clock()-st))/CLOCKS_PER_SEC;
    printf("%f elapsed\n", elapsed);
    /*pre-optimization this took 1.4-1.9 seconds*/
    return 0;

    /*#endif*/
    struct diskmap dm;
    int val = 0;
    int key = 0;
    uint32_t valsz;
    char valstr[30] = {0};
    init_diskmap(&dm, 10, 10000, "x", hash);
    /*insert_diskmap(&dm, 4, 4, &key, &val);*/
    /*insert_diskmap(&dm, 2, 5, "BA", "ASHER");*/
    // hmm, this should be showing up at the end but instead hexdump shows that it's directly overwriting ASHER
    /*insert_diskmap(&dm, 2, 10, "BA", "**********");*/
    insert_diskmap(&dm, 2, 10, "BA", "****@*$***");
    insert_diskmap(&dm, 2, 2, "BA", "_ ");

    // this should take the space that "ASHER" previously took up
    insert_diskmap(&dm, 2, 4, "bs", "news");
    insert_diskmap(&dm, 2, 4, "bs", "Kews");
    insert_diskmap(&dm, 2, 4, "bs", "neKs");
    insert_diskmap(&dm, 9, 5, "Eteridval", "asher");
    
    // we should never insert from only one thread
    for (int i = 0; i < 9000; ++i) {
        ++key;
        val = key*49;
        insert_diskmap(&dm, 4, 4, &key, &val);
        /*usleep(100);*/
    }
    /*return 1;*/
    key = 59;
    val = 2891;
    /*remove_key_diskmap(&dm, 4, &key);*/
    /*insert_diskmap(&dm, 4, 4, &key, &val);*/

    /*printf("lookup(): %i\n", lookup_diskmap(&dm, 4, &key, &valsz, &val));*/
    /*printf("lookup val: %i\n", val);*/

    printf("lookup(): %i\n", lookup_diskmap(&dm, 2, "BA", &valsz, valstr));
    /*printf("valsz: %i\n", valsz);*/
    /*valstr[valsz] = 0;*/
    printf("lookup val: %s\n", valstr);

    /*TODO: further test remove_key_diskmap()*/
    /*remove_key_diskmap(&dm, 2, "BA");*/

}
