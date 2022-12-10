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
    int nThreads = 1;
    char *files_dir;
    int nf_repeat;
    files_dir = argv[1];      // the folder of all input files
    nf_repeat = atoi(argv[2]); // second argument, number of thread
    printf("Input files from %s\n", files_dir);
    int file_count = 0;
    int i, k;                 // temp variable for loop
    double global_time = -omp_get_wtime();
    double local_time;

    ht *sum_table;
    sum_table = ht_create(HASH_CAPACITY);
    //create filesQueue
    struct Queue* filesQueue;
    struct Queue* wordsQueue;
    filesQueue = initQueue();
    wordsQueue = initQueue(); 
    local_time = -omp_get_wtime();
    for (i = 0; i < nf_repeat; i++){
        file_count += createFileQ(filesQueue, files_dir);
    }
    local_time += omp_get_wtime();
    printf("Done loading %d files, time taken: %f\n", file_count, local_time);

    //Queueing content of the files
    local_time = -omp_get_wtime();
    /********************** reader and mapper **********************************/
    char file_name[FILE_NAME_BUF_SIZE];
    while (filesQueue->front != NULL) {
        printf("thread: %d, filename: %s\n", 0, filesQueue->front->line);
        strcpy(file_name, filesQueue->front->line);
        removeQ(filesQueue);
        populateQueue(wordsQueue, file_name);
    } 
    local_time += omp_get_wtime();
    printf("Reader: %f\n", local_time);
    
    local_time = -omp_get_wtime();
    populateHashMap(wordsQueue, sum_table); 
    local_time += omp_get_wtime();
    printf("Mapper: %f\n", local_time);

    
    /********************** write file **********************************/
    local_time = -omp_get_wtime();
    item* current;
    char* filename = (char*)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
    sprintf(filename, "../output/openmp/serial.txt");
    FILE* fp = fopen(filename, "w");
    for (i = 0; i < HASH_CAPACITY; i++){
        current = sum_table->entries[i];
        if (current == NULL)
            continue; 
        fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
    }
    fclose(fp);
    local_time += omp_get_wtime();
    printf("Write: %f\n", local_time);
    freeHT(sum_table);
    freeQueue(wordsQueue);
    
    
    global_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", global_time);
    
    return EXIT_SUCCESS;
}
