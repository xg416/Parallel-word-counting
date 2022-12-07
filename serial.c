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
    int nThreads;
    char *files_dir;
    nThreads = atoi(argv[1]); // first argument, number of thread
    files_dir = argv[2];      // second argument, the folder of all input files
    printf("input %s\n", files_dir);
    int nRM = nThreads;     // # of readers and mappers
    int file_count = 0;
    int i, k;                    // temp variable for loop
    double global_time = -omp_get_wtime();
    double local_time;
    char csv_out[400] = "";
    char tmp_out[200] = "";  // Buffer was small. sprintf caused a buffer overflow and modified the inputs. 
    
    ht **tables;
    ht *sum_table;
    tables = (ht**)malloc(sizeof(struct ht*) * nRM);
    sum_table = ht_create(HASH_CAPACITY);
    
    omp_set_num_threads(nThreads);
    //create filesQueue
    struct Queue* file_name_queue;
    struct Queue* wordQueue;
    file_name_queue = createQueue();
    wordQueue = createQueue();
    printf("\nQueuing files in Directory: %s\n", files_dir);
    
    local_time = -omp_get_wtime();
    int nfiles = get_file_list(file_name_queue, files_dir);
    if (nfiles == -1) printf("Error!! Check input directory and rerun! Exiting!\n");
    file_count += nfiles;
    local_time += omp_get_wtime();
    sprintf(tmp_out, "%d, %d, %d, %.4f, ", file_count, HASH_CAPACITY, nThreads, local_time);
    strcat(csv_out, tmp_out);
    printf("Done Queuing %d files! Time taken: %f\n", file_count, local_time);

    //Queueing content of the files
    printf("\nQueuing Lines by reading files in the FilesQueue\n");
    local_time = -omp_get_wtime();

    omp_lock_t linesQlock;
    omp_init_lock(&linesQlock);

    /********************** reader and mapper **********************************/
    char file_name[FILE_NAME_BUF_SIZE * 3];
    while (file_name_queue->front != NULL) {
        printf("thread: %d, filename: %s\n", 0, file_name_queue->front->line);
        strcpy(file_name, file_name_queue->front->line);
        deQueue(file_name_queue);
        populateQueueDynamic(wordQueue, file_name, &linesQlock);
    }     
    populateHashMapWL(wordQueue, sum_table, &linesQlock); 

    local_time += omp_get_wtime();
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);
    printf("Done Populating lines! Time taken: %f\n", local_time);

    
    /********************** write file **********************************/
    item* current;
    char* filename = (char*)malloc(sizeof(char) * 32);
    sprintf(filename, "../output/openmp/serial.txt");
    FILE* fp = fopen(filename, "w");
    for (i = 0; i < HASH_CAPACITY; i++){
        current = sum_table->entries[i];
        if (current == NULL)
            continue; 
        fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
    }
    fclose(fp);

    freeHT(sum_table);
    freeQueue(wordQueue);
    
    
    global_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", global_time);
    
    return EXIT_SUCCESS;
}
