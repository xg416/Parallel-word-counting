#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>

#include "queue.h"
#include "util.h"
#include "ht.h"

// omp with reduce queue
#define HASH_CAPACITY 65536

int main(int argc, char *argv[]){
    int nThreads;
    char *files_dir;
    nThreads = atoi(argv[1]); // first argument, number of thread
    files_dir = argv[2];      // second argument, the folder of all input files
    printf("Input files from %s\n", files_dir);
    int nRM = nThreads/2;     // # of readers and mappers
    int file_count = 0;
    int k;                    // temp variable for loop
    double global_time = -omp_get_wtime();
    double local_time;

    ht **tables, **reduceTables;
    tables = (ht**)malloc(sizeof(struct ht*) * nRM);
    reduceTables = (ht**)malloc(sizeof(struct ht*) * nThreads);

    omp_set_num_threads(nThreads);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    //create filesQueue
    struct Queue* filesQueue;
    filesQueue = initQueue();
    printf("\nQueuing files in Directory: %s\n", files_dir);
    
    local_time = -omp_get_wtime();
    int nfiles = createFileQ(filesQueue, files_dir);
    if (nfiles == -1) printf("Error!! Check input directory and rerun! Exiting!\n");
    file_count += nfiles;
    local_time += omp_get_wtime();
    printf("Done loading %d files, time taken: %f\n", file_count, local_time);

    //Queueing content of the files
    local_time = -omp_get_wtime();
    struct Queue **queueList = (struct Queue **)malloc(sizeof(struct Queue *) * nRM);
    struct Queue **reduceQueues = (struct Queue **)malloc(sizeof(struct Queue *) * nThreads);
    
    omp_lock_t linesQlocks[nRM];
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        reduceTables[i] = ht_create(HASH_CAPACITY/nThreads);
        reduceQueues[i] = initQueue();
        if (i<nRM){
            omp_init_lock(&linesQlocks[i]);
            queueList[i] = initQueue();   
        }
        else{
            tables[i-nRM] = ht_create(HASH_CAPACITY);
        }
    }
    
    /********************** reader and mapper **********************************/
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        if (i < nRM){
            //reader threads
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
                populateQueueDynamic(queueList[i], file_name, &linesQlocks[i]);
            }     
        }
        else{
            //mapper threads
            populateHashMapWL(queueList[i-nRM], tables[i-nRM], &linesQlocks[i-nRM]); 
        }
    }
    omp_destroy_lock(&filesQlock);
    for (k=0; k<nRM; k++) {
        omp_destroy_lock(&linesQlocks[k]);
    }
    local_time += omp_get_wtime();
    printf("Done Populating lines! Time taken: %f\n", local_time);
    
    //while (queue->front != NULL)//verification of the work queue
    //{
    //    printf("%s", queue->front->line);
    //    queue->front = queue->front->next;
    //}

    /********************** reduction **********************************/
    local_time = -omp_get_wtime();
    int id_thread;
    // omp_lock_t reduceQlocks[nThreads];
    
    #pragma omp parallel num_threads(nThreads)
    {
        // omp_init_lock(&reduceQlocks[id_thread]);
        int id_thread = omp_get_thread_num();
        int interval = HASH_CAPACITY / nThreads;
        int start = id_thread * interval;
        int end = start + interval;
        int i;
        // if (id_thread == 0){printTable(tables[0]);}
        
        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (i = 0; i < nRM; i++)
        {
            populateRQ(reduceQueues[id_thread], tables[i], start, end);
        }
        queueToHtWoL(reduceQueues[id_thread], reduceTables[id_thread]);
        free(reduceQueues[id_thread]);
    }
    local_time += omp_get_wtime();
    printf("time for reduction %f \n ", local_time);
    
    /********************** write file **********************************/
    #pragma omp parallel
    {
       int i;
       item* current;
       int id_thread = omp_get_thread_num();
       int num_threads = omp_get_num_threads();

       char* filename = (char*)malloc(sizeof(char) * 32);
       sprintf(filename, "../output/openmp/%d.txt", id_thread);
       FILE* fp = fopen(filename, "w");
       for (i = 0; i < reduceTables[id_thread]->capacity; i++)
       {
           current = reduceTables[id_thread]->entries[i];
           if (current == NULL)
               continue;
           fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
       }
       fclose(fp);
       freeHT(reduceTables[id_thread]);
    }
    
    #pragma omp parallel for
    for (int k = 0; k < nRM; k++)
    {
        freeHT(tables[k]);
        freeQueue(queueList[k]);
    }
    free(queueList);
    free(reduceTables);
    
    global_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", global_time);
    
    return EXIT_SUCCESS;
}
