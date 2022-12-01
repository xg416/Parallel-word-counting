#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <time.h>

#include "ht.h"

#define HASH_CAPACITY 65536

int main(int argc, char *argv[]){
    int nthreads;
    char *files_dir;
    nthreads = atoi(argv[1]);
    files_dir = argv[2];
    printf("input %s\n", files_dir);
    ht **tables;
    ht *sum_table;
    tables = (ht **)malloc(sizeof(ht *) * nthreads/2);
    sum_table = ht_create(HASH_CAPACITY);
    
    // reduce
    #pragma omp parallel shared(sum_table, tables)
    {
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        int i;

        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (int i = 0; i < num_threads; i++)
        {
            ht_merge(sum_table, tables[i], start, end);
        }
    }

    // write file
    #pragma omp parallel shared(sum_table)
    {
        int i, j;
        item *current;
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        if (end > sum_table->capacity) end = sum_table->capacity;

        char *filename = (char *)malloc(sizeof(char) * 32);
        sprintf(filename, "output/parallel/%d.txt", id_thread);
        FILE *fp = (FILE *)filename;
        for (i = start; i < end; i++)
        {
            current = sum_table->entries[i];
            if (current == NULL)
                continue;
            fprintf(filename, "key: %s, frequency: %d\n", current->key, current->count);
        }
    }
    // write
    
    
}