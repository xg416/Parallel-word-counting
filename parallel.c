#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <omp.h>
#include "queue.h"
#include "hashTable.h"
#include "util.h"

extern int errno;
//int DEBUG_MODE = 0;
//int PRINT_MODE = 0;

int main(int argc, char** argv)
{
    int NUM_THREADS = 16;
    int NUM_READERS = 16;
    int NUM_MAPPERS = 16;
    int NUM_REDUCERS = 16;
    int HASH_SIZE = 50000;
    //int QUEUE_TABLE_COUNT = 1;
    char files_dir[FILE_NAME_BUF_SIZE] = "./files/";
    int file_count = 0;
    double global_time = -omp_get_wtime();
    double local_time;
    char csv_out[400] = "";
    char tmp_out[200] = "";  // Buffer was small. sprintf caused a buffer overflow and modified the inputs. 
    

    // Parsing User inputs from run command with getopt
    int arg_parse = process_args(argc, argv, files_dir, &HASH_SIZE,
         &NUM_THREADS);
    if (arg_parse == -1)
    {
        printf("Check inputs and rerun! Exiting!\n");
        return 1;
    }

    omp_set_num_threads(NUM_THREADS);
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
  


}
    /****************** ****************************************************************************************/