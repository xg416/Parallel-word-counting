#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>

#include "queue.h"
#include "util.h"
#include "ht.h"

// This file runs readers and mappers serially
//
//
//
//
#define HASH_CAPACITY 65536

int main(int argc, char *argv[]){
    int nThreads;
    char *files_dir;
    nThreads = atoi(argv[1]); // first argument, number of thread
    files_dir = argv[2];      // second argument, the folder of all input files
    printf("Input files from %s\n", files_dir);
    int nReaders = nThreads/2;
    int nMappers = nThreads/2;
    int nReducers = nThreads/2;
    int file_count = 0;
    int nf_repeat = 1;
    int i;
    double global_time = -omp_get_wtime();
    double local_time;

    ht **tables;
    ht *sum_table;
    tables = (ht**)malloc(sizeof(struct ht*) * nMappers);
    sum_table = ht_create(HASH_CAPACITY);
    
    omp_set_num_threads(nThreads);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    //create filesQueue
    struct Queue* filesQueue;
    filesQueue = initQueue();

    local_time = -omp_get_wtime();
    for (i = 0; i < nf_repeat; i++){
        file_count += createFileQ(filesQueue, files_dir);
    }
    local_time += omp_get_wtime();
    printf("Done loading %d files, time taken: %f\n", file_count, local_time);

    //Queueing content of the files
    local_time = -omp_get_wtime();
    struct Queue **queueList = (struct Queue **)malloc(sizeof(struct Queue *) * nReaders);
    
    omp_lock_t linesQlock;
    omp_init_lock(&linesQlock);
    #pragma omp parallel num_threads(nReaders)
    {
        int i = omp_get_thread_num();
        queueList[i] = initQueue();
        char file_name[FILE_NAME_BUF_SIZE ];
        while (filesQueue->front != NULL) {
            omp_set_lock(&filesQlock);
            if (filesQueue->front == NULL) {
                omp_unset_lock(&filesQlock);
                continue;
            }

            printf("thread: %d, filename: %s\n", i, filesQueue->front->line);
            strcpy(file_name, filesQueue->front->line);
            removeQ(filesQueue);
            omp_unset_lock(&filesQlock);
            populateQueueWL_ML(queueList[i], file_name, &linesQlock);
        }
    }
    omp_destroy_lock(&filesQlock);
    local_time += omp_get_wtime();
    printf("Done Populating lines! Time taken: %f\n", local_time);


    /************************* Mapper **********************************/
    #pragma omp parallel for shared(queueList, tables) num_threads(nMappers)
    for (int i = 0; i < nMappers; i++) {
        tables[i] = ht_create(HASH_CAPACITY);
        populateHashMapWL(queueList[i], tables[i], &linesQlock);
    }

    /********************** reduction **********************************/
    #pragma omp parallel shared(sum_table, tables) num_threads(nReducers)
    {
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        int i;

        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (i = 0; i < num_threads; i++)
        {
            ht_merge(sum_table, tables[i], start, end);
        }
    }
     
   
    /********************** write file **********************************/
    #pragma omp parallel shared(sum_table)
    {
       int i;
       item* current;
       int id_thread = omp_get_thread_num();
       int num_threads = omp_get_num_threads();
       int interval = HASH_CAPACITY / num_threads;
       int start = id_thread * interval;
       int end = start + interval;
       if (end > sum_table->capacity) end = sum_table->capacity;

       char* filename = (char*)malloc(sizeof(char) * 32);
       sprintf(filename, "../output/openmp/%d.txt", id_thread);
       FILE* fp = fopen(filename, "w");
       for (i = start; i < end; i++)
       {
           current = sum_table->entries[i];
           if (current == NULL)
               continue;
           fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
       }
       fclose(fp);
       printf("thread: %d/%d, output file: %s, start: %d, end: %d\n", id_thread, num_threads, filename, start, end); 
    }
    
    #pragma omp parallel for
    for (int i = 0; i < nMappers; i++)
    {
        freeHT(tables[i]);
        freeQueue(queueList[i]);
    }
    freeHT(sum_table);
    free(queueList);
    free(tables);
    
    global_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", global_time);
    
    return EXIT_SUCCESS;
}
