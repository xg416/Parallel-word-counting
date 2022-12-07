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
    printf("process id: %d/%d\n", pid, size);
    int nthreads = 4;
    int nRM = nthreads/2;
    char files_dir[] = "../files";
    int repeat_files = 1;
    double global_time = -MPI_Wtime();
    double local_time = 0;
    char csv_out[400] = "";
    char tmp_out[200] = "";

    int recv_pid;
    int i, k;

    int count = 0;
    int done = 0;
    int file_count = 0;
    int done_sent_p_count = 0;

    struct Queue *file_name_queue;
    file_name_queue = createQueue();
    struct Queue *local_file_queue;
    MPI_Barrier(MPI_COMM_WORLD);
    

    if (pid==0){
        for (i = 0; i < repeat_files; i++)
        {
            int files = get_file_list(file_name_queue, files_dir);
            if (files == -1)
            {
                printf("Check input directory and rerun! Exiting!\n");
                return 1;
            }
            file_count += files;
        }
    }

    /*****************************************************************************************
     * Share files among the processes
     * master node send to other nodes on request
     *****************************************************************************************/
    // reader and mapper setting
    omp_lock_t requestlock;
    omp_init_lock(&requestlock);
    omp_lock_t queuelock[nRM];
    struct Queue **queues;
    struct ht **hash_tables;
    queues = (struct Queue **)malloc(sizeof(struct Queue *) * nRM);
    hash_tables = (struct ht **)malloc(sizeof(struct ht *) * nRM);
    
    local_time = -omp_get_wtime();
    #pragma omp parallel for 
    for (k=0; k<nRM; k++) {
        omp_init_lock(&queuelock[k]);
        queues[k] = createQueue();
        hash_tables[k] = ht_create(HASH_CAPACITY);
    } 
    local_time += omp_get_wtime();
    if (pid==0) printf("initialization takes time %f\n", local_time); 
    MPI_Barrier(MPI_COMM_WORLD);
    
    // cross node filename allocation setting
    MPI_Status status;
    MPI_Request request;
    int recv_len = 0;
    char empty_flag[] = "all done";
    double t1, t2; 
    t1 = MPI_Wtime();
    // start allocating filenames then read & map
    if (pid == 0){
        char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
        char *send_file = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
        int len;
        int recv_pid = 0;
        int nRM_tot = (size-1) * nRM;
        
        #pragma omp parallel shared(queues, hash_tables, file_name_queue, requestlock, queuelock) num_threads(nthreads+1)
        {
            char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
            int threadn = omp_get_thread_num();
            if (threadn == nthreads){
                while (file_name_queue->front != NULL){
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    strcpy(send_file, file_name_queue->front->line);
                    len = file_name_queue->front->len;
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    omp_set_lock(&requestlock);
                    deQueue(file_name_queue);
                    omp_unset_lock(&requestlock);
                    if (file_name_queue->front == NULL) break;
                }
                while(nRM_tot){
                    send_file = empty_flag;
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    nRM_tot--;
                }
            }
            else if(threadn < nRM){
                while (file_name_queue->front != NULL){
                    omp_set_lock(&requestlock);
                    strcpy(file_name, file_name_queue->front->line);
                    deQueue(file_name_queue);
                    omp_unset_lock(&requestlock);
                    printf("pid %d thread %d received file %s \n", pid, threadn, file_name);
                    populateQueueDynamic(queues[threadn], file_name, &queuelock[threadn], pid, threadn);    
                }
            }
            else{
                int thread = threadn - nRM;
                populateHashMapWL(queues[thread], hash_tables[thread], &queuelock[thread], pid, threadn);
            }
        }
    }
    else{   
        #pragma omp parallel shared(queues, hash_tables, requestlock, queuelock) num_threads(nthreads)
        {
            char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
            int threadn = omp_get_thread_num();
            if (threadn < nRM) {
                while (strcmp(file_name, empty_flag)!=0){
                    omp_set_lock(&requestlock);
                    MPI_Send(&pid, 1, MPI_INT, 0, TAG_COMM_REQ_DATA, MPI_COMM_WORLD);
                    MPI_Recv(file_name, FILE_NAME_BUF_SIZE, MPI_CHAR, 0, TAG_COMM_FILE_NAME, MPI_COMM_WORLD, &status);
                    // MPI_Wait(&request, &status);
                    MPI_Get_count(&status, MPI_CHAR, &recv_len);
                    omp_unset_lock(&requestlock);
                    // printf("pid %d thread %d received file %s, len %d \n", pid, threadn, file_name, recv_len);
                    if (strcmp(file_name, empty_flag)==0) break;
                    populateQueueDynamic(queues[threadn], file_name, &queuelock[threadn], pid, threadn);
                }
            }
            else{
                int thread = threadn - nRM;
                printf("pid %d, thread id: %d \n", pid, threadn);
                // printQueue(queues[thread]);
                populateHashMapWL(queues[thread], hash_tables[thread], &queuelock[thread], pid, threadn);
            }
        }
    }

    
    omp_destroy_lock(&requestlock);
    for (k=0; k<nRM; k++) {
        omp_destroy_lock(&queuelock[k]);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime(); 
    if (pid==0) printf("file processing and mapping time is %f\n", t2 - t1); 
    
    struct ht *sum_table = ht_create(HASH_CAPACITY);
    #pragma omp parallel shared(sum_table, hash_tables) num_threads(nthreads)
    {
        int threadn = omp_get_thread_num();
        int tot_threads = omp_get_num_threads();
        int interval = HASH_CAPACITY / tot_threads;
        int start = threadn * interval;
        int end = start + interval;
        
        if (end > HASH_CAPACITY) end = HASH_CAPACITY;

        int j;
        for (j = 0; j < nRM; j++)
        {
            ht_merge(sum_table, hash_tables[j], start, end);
        }
    }
    /*****************************************************************************************
     * Send/Receive [{word,count}] Array of Structs to/from other processes 
     *****************************************************************************************/
    if (pid==3) {
        printTable(sum_table);
    }
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

    for (k = 0; k < size; k++)
    {
        if (pid != k)
        {
            int j = 0;
            pair pairs[HASH_CAPACITY];
            struct item *current = NULL;
            for (i = h_space * k; i < h_space * (k + 1); i++)
            {
                current = sum_table->entries[i];
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
            int pr;
            for (pr = 0; pr < size - 1; pr++)
            {
                int recv_j = 0;
                pair recv_pairs[HASH_CAPACITY];
                MPI_Recv(recv_pairs, HASH_CAPACITY, istruct, MPI_ANY_SOURCE,
                         TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, istruct, &recv_j);
                // fprintf(outfile, "total words to received: %d from source: %d\n",recv_j, status.MPI_SOURCE);

                for (i = 0; i < recv_j; i++)
                {
                    pair recv_pair = recv_pairs[i];
                    int frequency = recv_pair.count;

                    struct item *node = ht_update(sum_table, recv_pair.word, recv_pair.count);
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

    local_time = -omp_get_wtime();
    char* filename = (char*)malloc(sizeof(char) * 32);
    sprintf(filename, "../output/mpi/%d.txt", pid);
    FILE* fp = fopen(filename, "w");
    item* current;
    // printTable(sum_table);
    for (i = h_start; i < h_end; i++)
    {
        current = sum_table->entries[i];
        if (current == NULL)
            continue;
        // printf("i: %d, key: %s, count: %d, start, end: %d %d\n", i, current->key, current->count, h_start, h_end); 
        fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
    }
    fclose(fp);
    local_time += omp_get_wtime();
    global_time += MPI_Wtime();
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