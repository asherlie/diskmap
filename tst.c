#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "dm.h"

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
    init_diskmap(&dm, 1, n_buckets, "LIT", hash);

    for (int i = 0; i < n_threads; ++i) {
        ia = malloc(sizeof(struct ins_arg));
        ia->dm = &dm;
        ia->start_val = i * ins_per_thread;
        ia->n_insertions = ins_per_thread;
        pthread_create(pth+i, NULL, insert_th, ia);
    }
    for (int i = 0; i < n_threads; ++i) {
        pthread_join(pth[i], NULL);
    }
    if (n_threads == 1) {
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

void isolate_bug() {
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

// TODO: confirm that insertions are succeeding and can be popped properly
// potentially write this into large_insertion_test()
int main() {
    /*isolate_bug();*/
    /*return 1;*/
    #if 1
    clock_t st;
    double elapsed;
    st = clock();
    // this crashes with a very large number of insertions and < 10000 bucket entries
    // why?
    // 13 crashes, 12 stable
    /*large_insertion_test(1, 1, 1021);*/
    /*large_insertion_test(1, 1, 1020);*/
    large_insertion_test(1, 1, 4021);
    elapsed = ((double)(clock()-st))/CLOCKS_PER_SEC;
    printf("%f elapsed\n", elapsed);
    /*pre-optimization this took 1.4-1.9 seconds*/
    return 0;
    #endif

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
