#include <stdio.h>
#include <stdlib.h>

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
        /*printf("inserted %i\n", i);*/
    }
    free(ia);
    return NULL;
}

// TODO: return elapsed time
void large_insertion_test(int n_buckets, int n_threads, int ins_per_thread) {
    pthread_t pth[n_threads];
    struct ins_arg* ia;
    struct diskmap dm;
    init_diskmap(&dm, n_buckets, "LIT", hash);

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
}

int main() {
    clock_t st;
    double elapsed;
    st = clock();
    large_insertion_test(1, 1, 16);
    elapsed = ((double)(clock()-st))/CLOCKS_PER_SEC;
    printf("%f elapsed\n", elapsed);

    return 0;

    struct diskmap dm;
    int val = 0;
    int key = 0;
    uint32_t valsz;
    char valstr[30] = {0};
    init_diskmap(&dm, 10000, "x", hash);
    /*insert_diskmap(&dm, 4, 4, &key, &val);*/
    insert_diskmap(&dm, 2, 5, "BA", "ASHER");
    // hmm, this should be showing up at the end but instead hexdump shows that it's directly overwriting ASHER
    insert_diskmap(&dm, 2, 10, "BA", "**********");
    insert_diskmap(&dm, 2, 10, "BA", "****@*$***");
    insert_diskmap(&dm, 2, 2, "BA", "_ ");

    // this should take the space that "ASHER" previously took up
    insert_diskmap(&dm, 2, 4, "bs", "news");
    insert_diskmap(&dm, 2, 4, "bs", "Kews");
    insert_diskmap(&dm, 2, 4, "bs", "neKs");
    insert_diskmap(&dm, 9, 5, "Eteridval", "asher");
    
    for (int i = 0; i < 100000; ++i) {
        ++key;
        val = key*49;
        insert_diskmap(&dm, 4, 4, &key, &val);
        /*usleep(100);*/
    }

    key = 59;
    printf("lookup(): %i\n", lookup_diskmap(&dm, 4, &key, &valsz, &val));
    printf("lookup val: %i\n", val);

    printf("lookup(): %i\n", lookup_diskmap(&dm, 2, "BA", &valsz, valstr));
    printf("valsz: %i\n", valsz);
    printf("lookup val: %s\n", valstr);

    insert_diskmap(&dm, 0, 0, NULL, NULL);
}
