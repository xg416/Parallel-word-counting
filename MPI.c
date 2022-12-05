#include <dirent.h>
#include <errno.h>
#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <omp.h>

#include "ht.h"
#include "queue.h"
#include "util.h"

#define TAG_COMM_REQ_DATA 0
#define TAG_COMM_FILE_NAME 1
#define TAG_COMM_PAIR_LIST 3
#define WORD_MAX_LENGTH 64
#define HASH_CAPACITY 65536

#define NUM_THREADS 4
#define REPEAT_FILES 10
#define BREAK_RM 0


typedef struct
{
    char word[WORD_MAX_LENGTH];
    int count;
} pair;


int main(int argc, char **argv)
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    if(provided < MPI_THREAD_FUNNELED)
    {
        fprintf(stderr, "Error: the threading support level: %d is lesser than that demanded: %d\n", MPI_THREAD_FUNNELED, provided);
        MPI_Finalize();
        return 0;
    }

    int size, pid, p_name_len;
    char p_name[MPI_MAX_PROCESSOR_NAME];
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Get_processor_name(p_name, &p_name_len);

    int nthreads = 4;
    char files_dir[] = "../files";
    int repeat_files = 1;
    double global_time = 0;
    double local_time = 0;
    char csv_out[400] = "";
    char tmp_out[200] = "";

    MPI_Request request;
    MPI_Status status;
    int recv_pid;
    int recv_len = 0;

    int count = 0;
    int done = 0;
    int file_count = 0;
    int done_sent_p_count = 0;

    struct Queue *file_name_queue;
    file_name_queue = createQueue();
    struct Queue *files_to_read;
    MPI_Barrier(MPI_COMM_WORLD);
    local_time = -omp_get_wtime();

    char *file_names = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE * 1000);

    /*****************************************************************************************
     * Share files among the processes
     * If there are 15 files are 4 processes
     * 00,01,02,03 is sent to process 0 | 04,05,06,07 is sent to process 1
     * 08,09,10,11 is sent to process 2 | 12,13,14    is sent to process 3
     *****************************************************************************************/
    if (pid == 0)
    {
        for (int i = 0; i < repeat_files; i++)
        {
            int files = get_file_list(file_name_queue, files_dir);
            if (files == -1)
            {
                printf("Check input directory and rerun! Exiting!\n");
                return 1;
            }
            file_count += files;
        }

        int num_files_to_send = file_count / size;
        int spare = file_count % size;

        // iterate for all process ids in the comm world
        for (int i = 0; i < size; i++)
        {
            char *concat_files =
                (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE * num_files_to_send);
            int len = 0;
            int send_file_count = num_files_to_send;
            if (spare>0) {
                send_file_count += 1;
                spare--;
            }

            // concat file names to one big char array for ease of sending
            for (int j = 0; j < send_file_count; j++)
            {
                if (j == 0)
                {
                    strcpy(concat_files, file_name_queue->front->line);
                }
                else
                {
                    strcat(concat_files, file_name_queue->front->line);
                }
                len += file_name_queue->front->len + 1;
                if (j != send_file_count - 1)
                {
                    strcat(concat_files, ",");
                }
                deQueue(file_name_queue);
            }
            if (i == 0)
            { // send to other processes
                strcpy(file_names, concat_files);
                recv_len = len;
            }
            else
            {
                MPI_Send(concat_files, len + 1, MPI_CHAR, i, TAG_COMM_FILE_NAME,
                         MPI_COMM_WORLD);
            }
        }
    }
    else
    {
        MPI_Status status;
        MPI_Recv(file_names, FILE_NAME_BUF_SIZE * 1000, MPI_CHAR, 0,
                 TAG_COMM_FILE_NAME, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &recv_len);
    }

    char *file;
    int received_file_count = 0;
    while ((file = strtok_r(file_names, ",", &file_names)))
    {
        if (strlen(file) > 0)
        {
            enQueue(file_name_queue, file, strlen(file));
            received_file_count++;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    local_time += omp_get_wtime();
    global_time += local_time;
    strcat(csv_out, tmp_out);   

    local_time = -omp_get_wtime();
    omp_lock_t readlock;
    omp_init_lock(&readlock);
    omp_lock_t queuelock[nthreads/2];

    struct Queue **queues;
    struct ht **hash_tables;

    // we will divide the number of threads and use half for reading and half for mapping
    // therefore, only half the thread number of queues and hash_tables are required
    queues = (struct Queue **)malloc(sizeof(struct Queue *) * nthreads);
    hash_tables = (struct ht **)malloc(sizeof(struct ht *) * nthreads);

    for (int k=0; k<nthreads; k++) {
        omp_init_lock(&queuelock[k]);
        queues[k] = createQueue();
        hash_tables[k] = ht_create(HASH_CAPACITY);
    }

    /*****************************************************************************************
     * Read and map section
     * if par_read_map is set to 1, half the threads are doing reading while the other half
     * threads are mapping -- this happens in parallel
     * if par_read_map is set to 0, the all the threads run reading and mapping functions 
     * in a sequential manner. But the complete process is run in parallel by multiple threads
     *****************************************************************************************/
    #pragma omp parallel shared(queues, hash_tables, file_name_queue, readlock, queuelock) num_threads(nthreads)
    {
        int threadn = omp_get_thread_num();
        if (threadn < nthreads/2) {
            while (file_name_queue->front != NULL)
            {
                char file_name[30];
                omp_set_lock(&readlock);
                if (file_name_queue->front == NULL) {
                    omp_unset_lock(&readlock);
                    continue;
                }
                strcpy(file_name, file_name_queue->front->line);
                deQueue(file_name_queue);
                omp_unset_lock(&readlock);

                populateQueueDynamic(queues[threadn], file_name, &queuelock[threadn]);
            }
            // queues[threadn]->finished = 1;
        } else {
            int thread = threadn - nthreads/2;
            hash_tables[thread] = ht_create(HASH_CAPACITY);
            populateHashMapWL(queues[thread], hash_tables[thread], &queuelock[thread]);
        }  
    }
    omp_destroy_lock(&readlock);
    for (int k=0; k<nthreads; k++) {
        omp_destroy_lock(&queuelock[k]);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    local_time += omp_get_wtime();
    global_time += local_time;
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);

    /*****************************************************************************************
     * Final sum reduction locally inside the process
     *****************************************************************************************/
    local_time = -omp_get_wtime();
    struct ht *final_table = ht_create(HASH_CAPACITY);
    #pragma omp parallel shared(final_table, hash_tables) num_threads(nthreads)
    {
        int threadn = omp_get_thread_num();
        int tot_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / tot_threads;
        int start = threadn * interval;
        int end = start + interval;

        if (end > final_table->itemcount) end = final_table->itemcount;

        int i;
        ht_merge(final_table, hash_tables[i], start, end);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    local_time += omp_get_wtime();
    global_time += local_time;
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);
    // fprintf(stdout, "reduction inside the process done.. size: %d, rank: %d\n", size, pid);


    /*****************************************************************************************
     * Send/Receive [{word,count}] Array of Structs to/from other processes 
     *****************************************************************************************/

    local_time = -omp_get_wtime();
    /*
    * add reduction - hashtable should be communicated amoung the
    * processes to come up with the final reduction
    */
    int h_space = HASH_CAPACITY / size;
    int h_start = h_space * pid;
    int h_end = h_space * (pid + 1);
    // fprintf(outfile, "start [%d] end [%d]\n", h_start, h_end);
    // [0, HASH_CAPACITY/size] values from all the processes should be sent to 0th
    // process [HASH_CAPACITY/size, HASH_CAPACITY/size*2] values from all the ps should be
    // sent to 1st process likewise all data should be shared among the processes

    // --------- DEFINE THE STRUCT DATA TYPE TO SEND
    const int nfields = 2;
    MPI_Aint disps[nfields];
    int blocklens[] = {WORD_MAX_LENGTH, 1};
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};

    disps[0] = offsetof(pair, word);
    disps[1] = offsetof(pair, count);

    MPI_Datatype istruct;
    MPI_Type_create_struct(nfields, blocklens, disps, types, &istruct);
    MPI_Type_commit(&istruct);

    // ---

    int k = 0;
    for (k = 0; k < size; k++)
    {
        if (pid != k)
        {
            int j = 0;
            pair pairs[HASH_CAPACITY];
            struct item *current = NULL;
            for (int i = h_space * k; i < h_space * (k + 1); i++)
            {
                current = final_table->entries[i];
                if (current == NULL)
                    continue;
                else{
                    pairs[j].count = current->count;
                    strcpy(pairs[j].word, current->key);
                    j++;
                }
            }

            // fprintf(outfile, "total words to send: %d\n", j);
            MPI_Send(pairs, j, istruct, k, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD);
            // fprintf(outfile, "total words sent: %d\n", j);
        }
        else if (pid == k)
        {
            for (int pr = 0; pr < size - 1; pr++)
            {
                int recv_j = 0;
                pair recv_pairs[HASH_CAPACITY];
                MPI_Recv(recv_pairs, HASH_CAPACITY, istruct, MPI_ANY_SOURCE,
                         TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, istruct, &recv_j);
                // fprintf(outfile, "total words to received: %d from source: %d\n",recv_j, status.MPI_SOURCE);

                for (int i = 0; i < recv_j; i++)
                {
                    pair recv_pair = recv_pairs[i];
                    int frequency = recv_pair.count;

                    struct item *node = ht_update(final_table, recv_pair.word, recv_pair.count);
                }
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    local_time += omp_get_wtime();
    global_time += local_time;
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);

    /*****************************************************************************************
     * Write data to tables
     * write function should be only called for the respective section of the
     *****************************************************************************************/

    // local_time = -omp_get_wtime();
    char* filename = (char*)malloc(sizeof(char) * 32);
    sprintf(filename, "../output/mpi/%d.txt", pid);
    FILE* fp = fopen(filename, "w");
    item* current;
    for (int i = h_start; i < h_end; i++)
    {
        current = final_table->entries[i];
        if (current == NULL)
            continue;
        //    printf("i: %d, key: %s, count: %d\n", i, current->key, current->count); 
        fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
    }
    fclose(fp);
    local_time += omp_get_wtime();
    global_time += local_time;
    sprintf(tmp_out, "%.4f, ", local_time);
    strcat(csv_out, tmp_out);
    sprintf(tmp_out, "%.4f", global_time);
    strcat(csv_out, tmp_out);

    if (pid == 0) {
        fprintf(stdout, "Num_Files, Hash_size, Num_Processes, Num_Threads, Par_Read_Map, FS_Time, Read_Map, Local_Reduce, Final_Reduce, Write, Total\n%s\n\n", 
            csv_out);
    }

    MPI_Finalize();
    return 0;
}