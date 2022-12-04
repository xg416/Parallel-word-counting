#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>

#include "queue.h"
#include "util.h"
#include "ht.h"


#define HASH_CAPACITY 65536
extern int errno;
//int DEBUG_MODE = 0;
//int PRINT_MODE = 0;


int main(int argc, char** argv)
{
    int NUM_THREADS = 16;
    int NUM_READERS = 8;
    int NUM_MAPPERS = 8;
    int NUM_REDUCERS = 8;
    //int QUEUE_TABLE_COUNT = 1;
    char files_dir[FILE_NAME_BUF_SIZE] = "../files/";
    int file_count = 0;
    double global_time = -omp_get_wtime();
    double local_time;
    char csv_out[400] = "";
    char tmp_out[200] = "";  // Buffer was small. sprintf caused a buffer overflow and modified the inputs. 
    
    omp_set_num_threads(NUM_THREADS);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    //create filesQueue
    struct Queue* file_name_queue;
    file_name_queue = createQueue();

    printf("\nQueuing files in Directory: %s\n", files_dir);
    
    local_time = -omp_get_wtime();
    int files = get_file_list(file_name_queue, files_dir);
    if (files == -1)
    {
        printf("Error!! Check input directory and rerun! Exiting!\n");
    }
    file_count += files;
    local_time += omp_get_wtime();

    sprintf(tmp_out, "%d, %d, %d, %.4f, ", file_count, HASH_CAPACITY, NUM_THREADS, local_time);
    strcat(csv_out, tmp_out);
    printf("Done Queuing %d files! Time taken: %f\n", file_count, local_time);
    


    //Queueing content of the files
    printf("\nQueuing Lines by reading files in the FilesQueue\n");
    local_time = -omp_get_wtime();
    struct Queue* queue;
    omp_lock_t linesQlock;
    omp_init_lock(&linesQlock);
    queue = createQueue();
    #pragma omp parallel num_threads(NUM_THREADS)
    {
        int i = omp_get_thread_num();
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
            populateQueueWL_ML(queue, file_name, &linesQlock);
        }
    }
    omp_destroy_lock(&filesQlock);
    local_time += omp_get_wtime();
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);
    printf("Done Populating lines! Time taken: %f\n", local_time);
    
    //while (queue->front != NULL)//verification of the work queue
    //{
    //    printf("%s", queue->front->line);
    //    queue->front = queue->front->next;
    //}

    //hash words
    ht** tables;
    tables = (ht**)malloc(sizeof(struct ht*) * NUM_THREADS);
    #pragma omp parallel for shared(queue, tables)
    for (int i = 0; i < NUM_THREADS; i++) {
        tables[i] = ht_create(HASH_CAPACITY);
        populateHashMapWL(queue, tables[i], &linesQlock);
    }

    /********************** reduction **********************************/
    ht* sum_table;
    sum_table = ht_create(HASH_CAPACITY);
    #pragma omp parallel shared(sum_table, tables)
    {
        int id_thread = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / num_threads;
        int start = id_thread * interval;
        int end = start + interval;
        int i;
        // if (id_thread == 0){printTable(tables[0]);}

        if (end > HASH_CAPACITY) end = HASH_CAPACITY;
        for (int i = 0; i < num_threads; i++)
        {
            ht_merge(sum_table, tables[i], start, end);
        }
        // if (id_thread == 0){printTable(sum_table);}
    }
     
   
    /********************** write file **********************************/
    #pragma omp parallel shared(sum_table)
    {
       int i, j;
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
       printf("thread: %d/%d, output file: %s, start: %d, end: %d\n", id_thread, num_threads, filename, start, end); 
    }
    
    #pragma omp parallel for
    for (int i = 0; i < NUM_THREADS; i++)
    {
        ht_destroy(tables[i]);
    }
    ht_destroy(sum_table);
    destoryQueue(queue);
}
    