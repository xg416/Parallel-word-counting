#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>

#include "queue.h"
#include "util.h"
#include "ht.h"


#define HASH_CAPACITY 65536

int main(int argc, char *argv[]){
    int nThreads, nReader, nMapper;
    int nf_repeat;
    char *files_dir;
    nThreads = atoi(argv[1]);      // first argument, # of thread
    nReader = atoi(argv[2]);       // second argument, # of reader thread  
    files_dir = argv[3];           // 3rd argument, the folder of all input files
    nf_repeat = atoi(argv[4]);     // 4th argument, repeat times of all files
    printf("Input files from %s\n", files_dir);
    nMapper = nThreads - nReader;     // # of mappers
    int file_count = 0;
    int i, k;                    // temp variable for loop
    double total_time = -omp_get_wtime();
    double temp_timer;
    
    omp_set_num_threads(nThreads);

    //create filesQueue
    struct Queue* filesQueue;
    filesQueue = initQueue();
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);
    
    temp_timer = -omp_get_wtime();
    for (i = 0; i < nf_repeat; i++){
        file_count += createFileQ(filesQueue, files_dir);
    }
    temp_timer += omp_get_wtime();
    printf("Done loading %d files, time taken: %f\n", file_count, temp_timer);

    ht **tables, **reduceTables;
    tables = (ht**)malloc(sizeof(struct ht*) * nMapper);
    reduceTables = (ht**)malloc(sizeof(struct ht*) * nThreads);
    struct Queue **queueList = (struct Queue **)malloc(sizeof(struct Queue *) * nMapper);
    struct Queue **reduceQueues = (struct Queue **)malloc(sizeof(struct Queue *) * nThreads);
    omp_lock_t linesQlocks[nMapper];
    //Queueing content of the files
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        reduceTables[i] = ht_create(HASH_CAPACITY/nThreads);
        reduceQueues[i] = initQueue();
        if (i<nMapper){
            omp_init_lock(&linesQlocks[i]);
            queueList[i] = initQueue();   
            tables[i] = ht_create(HASH_CAPACITY);
        }
    }
    
    temp_timer = -omp_get_wtime();
    /********************** reader and mapper **********************************/
    int queue_count = -1;
    #pragma omp parallel num_threads(nThreads)
    {
        int tid = omp_get_thread_num();
        // double thread_timer = -omp_get_wtime();
        if (tid < nReader){
            //reader threads
            char file_name[FILE_NAME_MAX_LENGTH];
            int queue_id;
            while (filesQueue->front != NULL) {
                omp_set_lock(&filesQlock);
                queue_count++;
                if (filesQueue->front == NULL) {
                    omp_unset_lock(&filesQlock);
                    continue;
                }
                strcpy(file_name, filesQueue->front->line);
                removeQ(filesQueue);
                omp_unset_lock(&filesQlock);
                queue_id = queue_count % nMapper;
                populateQueueDynamic(queueList[queue_id], file_name, &linesQlocks[queue_id]);
            }
            for (i = 0; i< nMapper; i++){
                queueList[i]->NoMoreNode = 1;
            }
            // thread_timer += omp_get_wtime();
            // printf("Reader thread %d takes time %f \n ", tid, thread_timer);
        }
        else{
            //mapper threads
            int queue_id = tid-nReader;
            populateHashMapWL(queueList[queue_id], tables[queue_id], &linesQlocks[queue_id]); 
            // thread_timer += omp_get_wtime();
            // printf("Mapper thread %d takes time %f \n ", tid, thread_timer);
        }
    }
    omp_destroy_lock(&filesQlock);
    for (k=0; k<nMapper; k++) {
        omp_destroy_lock(&linesQlocks[k]);
    }
    temp_timer += omp_get_wtime();
    printf("Reader and mapper time taken: %f\n", temp_timer);


    /********************** reduction **********************************/
    temp_timer = -omp_get_wtime();
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
        
        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (i = 0; i < nMapper; i++)
        {
            populateRQ(reduceQueues[id_thread], tables[i], start, end);
        }
        queueToHtWoL(reduceQueues[id_thread], reduceTables[id_thread]);
        free(reduceQueues[id_thread]);
    }
    temp_timer += omp_get_wtime();
    printf("time for reduction %f \n ", temp_timer);
    
    /********************** write file **********************************/
    #pragma omp parallel
    {
       int i;
       item* current;
       int id_thread = omp_get_thread_num();
       int num_threads = omp_get_num_threads();

       char filename[FILE_NAME_MAX_LENGTH];
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
    for (k = 0; k < nMapper; k++)
    {
        freeHT(tables[k]);
        freeQueue(queueList[k]);
    }
    free(queueList);
    free(reduceTables);
    
    total_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", total_time);
    
    return EXIT_SUCCESS;
}
