#include <stdio.h>

#include "dm.h"

int hash(void* key, uint32_t keysz, uint32_t n_buckets) {
    if (keysz < sizeof(int)) {
        return 9 % n_buckets;
    }
    return *((int*)key) % n_buckets;
}

int main() {
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

    // this should take the space that "ASHER" previously took up
    insert_diskmap(&dm, 2, 4, "bs", "news");
    insert_diskmap(&dm, 2, 4, "bs", "Kews");
    insert_diskmap(&dm, 2, 4, "bs", "neKs");
    insert_diskmap(&dm, 9, 5, "Eteridval", "asher");
    
    for (int i = 0; i < 1573; ++i) {
        ++key;
        val = key*49;
        insert_diskmap(&dm, 4, 4, &key, &val);
        /*usleep(100);*/
    }
    (void)key;
    (void)val;

    key = 59;
    printf("lookup(): %i\n", lookup_diskmap(&dm, 4, &key, &valsz, &val));
    printf("lookup val: %i\n", val);

    printf("lookup(): %i\n", lookup_diskmap(&dm, 2, "BA", &valsz, valstr));
    printf("valsz: %i\n", valsz);
    printf("lookup val: %s\n", valstr);
}
