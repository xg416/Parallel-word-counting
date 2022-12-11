#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>

#include "queue.h"
#include "util.h"
#include "ht.h"

// This file runs readers and mappers serially

int main(int argc, char *argv[]){
    int nThreads, nf_repeat;
    char *files_dir;
    nThreads = atoi(argv[1]); // first argument, number of thread
    files_dir = argv[2];      // second argument, the folder of all input files
    nf_repeat = atoi(argv[3]); // first argument, number of thread
    printf("Input files from %s\n", files_dir);
    int nReaders = nThreads;
    int nMappers = nThreads;
    int nReducers = nThreads;
    int file_count = 0;
    int i;
    double total_time = -omp_get_wtime();
    double temp_timer, temp_timer2;
    
    omp_set_num_threads(nThreads);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    ht **tables = (ht**)malloc(sizeof(struct ht*) * nMappers);
    ht **reduceTables = (ht**)malloc(sizeof(struct ht*) * nReducers);
    struct Queue **queueList = (struct Queue **)malloc(sizeof(struct Queue *) * nMappers);
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        queueList[i] = initQueue();
        tables[i] = ht_create(HASH_CAPACITY);
        reduceTables[i] = ht_create(HASH_CAPACITY/nThreads);
    }
    //create filesQueue
    struct Queue* filesQueue;
    filesQueue = initQueue();

    temp_timer = -omp_get_wtime();
    for (i = 0; i < nf_repeat; i++){
        file_count += createFileQ(filesQueue, files_dir);
    }
    temp_timer += omp_get_wtime();
    printf("Done loading %d files, time taken: %f\n", file_count, temp_timer);

    
    //Queueing content of the files
    temp_timer = -omp_get_wtime();
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        queueList[i] = initQueue();
        char file_name[FILE_NAME_MAX_LENGTH];
        while (filesQueue->front != NULL) {
            omp_set_lock(&filesQlock);
            if (filesQueue->front == NULL) {
                omp_unset_lock(&filesQlock);
                continue;
            }
            strcpy(file_name, filesQueue->front->line);
            removeQ(filesQueue);
            omp_unset_lock(&filesQlock);
            populateQueue(queueList[i], file_name);
        }
    }
    omp_destroy_lock(&filesQlock);
    temp_timer += omp_get_wtime();
    printf("Reader time taken: %f\n", temp_timer);

    /************************* Mapper **********************************/
    temp_timer2 = -omp_get_wtime();
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        populateHashMap(queueList[i], tables[i]);
        freeQueue(queueList[i]);
    }
    temp_timer2 += omp_get_wtime();
    printf("Mapper time taken: %f\n", temp_timer2);
    printf("Reader and Mapper time taken: %f\n", temp_timer+temp_timer2);
    
    /********************** reduction **********************************/
    temp_timer2 = -omp_get_wtime();
    #pragma omp parallel shared(reduceTables, tables) num_threads(nReducers)
    {
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        
        int i;
        for (i = 0; i < num_threads; i++){
            ht_merge_remap(reduceTables[id_thread], tables[i], start, end);
        }
    }
    temp_timer2 += omp_get_wtime();
    printf("Reduction time taken: %f\n", temp_timer2);
    /********************** write file **********************************/
    temp_timer2 = -omp_get_wtime();
    #pragma omp parallel
    {
       int i;
       item* current;
       int id_thread = omp_get_thread_num();
       int num_threads = omp_get_num_threads();

       char filename[FILE_NAME_MAX_LENGTH];
       sprintf(filename, "../output/openmp/%d.txt", id_thread);
       FILE* fp = fopen(filename, "w");
       for (i = 0; i < reduceTables[id_thread]->capacity; i++){
           current = reduceTables[id_thread]->entries[i];
           if (current == NULL)
               continue;
           fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
       }
       fclose(fp);
    }
    temp_timer2 += omp_get_wtime();
    printf("Writing time taken: %f\n", temp_timer2);
    
    #pragma omp parallel for
    for (i = 0; i < nThreads; i++)
    {
        freeHT(tables[i]);
        freeHT(reduceTables[i]);
    }
    free(tables);
    free(reduceTables);
    
    total_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", total_time);
    
    return EXIT_SUCCESS;
}
