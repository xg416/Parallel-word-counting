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
    int nRM = nThreads/2;     // # of readers and mappers
    int file_count = 0;
    int k;                    // temp variable for loop
    double global_time = -omp_get_wtime();
    double local_time;
    char csv_out[400] = "";
    char tmp_out[200] = "";  // Buffer was small. sprintf caused a buffer overflow and modified the inputs. 
    
    ht **tables;
    ht *sum_table;
    tables = (ht**)malloc(sizeof(struct ht*) * nRM);
    sum_table = ht_create(HASH_CAPACITY);
    
    omp_set_num_threads(nThreads);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    //create filesQueue
    struct Queue* file_name_queue;
    file_name_queue = createQueue();
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
    struct Queue **queueList = (struct Queue **)malloc(sizeof(struct Queue *) * nRM);
    
    omp_lock_t linesQlocks[nRM];
    for (k = 0; k < nRM; k++)
    {
        omp_init_lock(&linesQlocks[k]);
        queueList[k] = createQueue();
    }
    
    /********************** reader and mapper **********************************/
    #pragma omp parallel num_threads(nThreads)
    {
        int i = omp_get_thread_num();
        if (i < nRM){
            //reader threads
            char file_name[FILE_NAME_BUF_SIZE * 3];
            while (file_name_queue->front != NULL) {
                omp_set_lock(&filesQlock);
                if (file_name_queue->front == NULL) {
                    omp_unset_lock(&filesQlock);
                    continue;
                }

                printf("thread: %d, filename: %s\n", i, file_name_queue->front->line);
                strcpy(file_name, file_name_queue->front->line);
                deQueue(file_name_queue);
                omp_unset_lock(&filesQlock);
                populateQueueDynamic(queueList[i], file_name, &linesQlocks[i]);
            }     
        }
        else{
            //mapper threads
            tables[i-nRM] = ht_create(HASH_CAPACITY);
            populateHashMapWL(queueList[i-nRM], tables[i-nRM], &linesQlocks[i-nRM]); 
        }
    }
    omp_destroy_lock(&filesQlock);
    for (k=0; k<nRM; k++) {
        omp_destroy_lock(&linesQlocks[k]);
    }
    local_time += omp_get_wtime();
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);
    printf("Done Populating lines! Time taken: %f\n", local_time);
    
    //while (queue->front != NULL)//verification of the work queue
    //{
    //    printf("%s", queue->front->line);
    //    queue->front = queue->front->next;
    //}

    /********************** reduction **********************************/
    local_time = -omp_get_wtime();
    #pragma omp parallel num_threads(nThreads)
    {
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        int i;
        // if (id_thread == 0){printTable(tables[0]);}

        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (i = 0; i < nRM; i++)
        {
            ht_merge(sum_table, tables[i], start, end);
        }
        // if (id_thread == 0){printTable(sum_table);}
    }
    local_time += omp_get_wtime();
    printf("time for reduction %f \n ", local_time);
    
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
        //    printf("i: %d, key: %s, count: %d\n", i, current->key, current->count); 
           fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
       }
       fclose(fp);
    //    printf("thread: %d/%d, output file: %s, start: %d, end: %d\n", id_thread, num_threads, filename, start, end); 
    }
    
    #pragma omp parallel for
    for (int k = 0; k < nRM; k++)
    {
        freeHT(tables[k]);
        freeQueue(queueList[k]);
    }
    freeHT(sum_table);
    free(queueList);
    free(tables);
    
    global_time += omp_get_wtime();
    printf("total time taken for the execution: %f\n", global_time);
    
    return EXIT_SUCCESS;
}
