#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <time.h>

#include "queue.h"
#include "ht.h"
#include "util.h"

#define HASH_CAPACITY 65536

int main(int argc, char *argv[]){
    int nthreads;
    char *files_dir;
    nthreads = atoi(argv[1]); // first argument, number of thread
    files_dir = argv[2];      // second argument, the folder of all input files
    printf("input %s\n", files_dir);
    ht **tables;
    ht *sum_table;
    tables = (ht **)malloc(sizeof(ht *) * nthreads/2);
    sum_table = ht_create(HASH_CAPACITY);
    
    omp_set_num_threads(nthreads);
    omp_lock_t filesQlock;
    omp_init_lock(&filesQlock);

    /********************** Creating and populating FilesQueue ************************************************/
    struct Queue* file_name_queue;
    file_name_queue = createQueue();

    printf("\nQueuing files in Directory: %s\n", files_dir);
    
    //read files
    local_time = -omp_get_wtime();
    int files = get_file_list(file_name_queue, files_dir);
    if (files == -1)
    {
        printf("Error!! Check input directory and rerun! Exiting!\n");
    }
    file_count += files;
    local_time += omp_get_wtime();

    sprintf(tmp_out, "%d, %d, %d, %.4f, ", file_count, HASH_SIZE, NUM_THREADS, local_time);
    strcat(csv_out, tmp_out);
    printf("Done Queuing %d files! Time taken: %f\n", file_count, local_time);
    /**********************************************************************************************************/

    /********************** Queuing Lines by reading files in the FilesQueue **********************************/
    printf("\nQueuing Lines by reading files in the FilesQueue\n");
    local_time = -omp_get_wtime();
    struct Queue* queue;
    omp_lock_t linesQlock;
    omp_init_lock(&linesQlock);
    queue = createQueue();
    #pragma omp parallel  num_threads(NUM_THREADS)
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

    printf("%s",queue->);
    
    /********************** reduction **********************************/
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

    /********************** write file **********************************/
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
        sprintf(filename, "../output/openmp/%d.txt", id_thread);
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