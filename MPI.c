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
#define WORD_MAX_LENGTH 32
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
    int nRM = nthreads/2;
    int nReduce = nthreads;
    char files_dir[] = "../files";
    int repeat_files = 2;
    double global_time = -MPI_Wtime();
    double local_time = 0;
    char csv_out[400] = "";
    char tmp_out[200] = "";

    int recv_pid;
    int i, j, k;

    int count = 0;
    int done = 0;
    int file_count = 0;
    int done_sent_p_count = 0;

    struct Queue *filesQueue;
    filesQueue = initQueue();
    struct Queue *local_file_queue;
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (pid==0){
        for (i = 0; i < repeat_files; i++){
            int files = createFileQ(filesQueue, files_dir);
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
        queues[k] = initQueue();
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
        
        #pragma omp parallel shared(queues, hash_tables, filesQueue, requestlock, queuelock) num_threads(nthreads+1)
        {
            char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE);
            int threadn = omp_get_thread_num();
            if (threadn == nthreads){
                while (filesQueue->front != NULL){
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    strcpy(send_file, filesQueue->front->line);
                    len = filesQueue->front->len;
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    omp_set_lock(&requestlock);
                    removeQ(filesQueue);
                    omp_unset_lock(&requestlock);
                    if (filesQueue->front == NULL) break;
                }
                while(nRM_tot){
                    send_file = empty_flag;
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    nRM_tot--;
                }
            }
            else if(threadn < nRM){
                while (filesQueue->front != NULL){
                    omp_set_lock(&requestlock);
                    strcpy(file_name, filesQueue->front->line);
                    removeQ(filesQueue);
                    omp_unset_lock(&requestlock);
                    // printf("pid %d thread %d received file %s \n", pid, threadn, file_name);
                    populateQueueDynamic(queues[threadn], file_name, &queuelock[threadn]);    
                }
            }
            else{
                int thread = threadn - nRM;
                populateHashMapWL(queues[thread], hash_tables[thread], &queuelock[thread]);
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
                    populateQueueDynamic(queues[threadn], file_name, &queuelock[threadn]);
                }
            }
            else{
                int thread = threadn - nRM;
                // printQueue(queues[thread]);
                populateHashMapWL(queues[thread], hash_tables[thread], &queuelock[thread]);
            }
        }
    }

    
    omp_destroy_lock(&requestlock);
    for (k=0; k<nRM; k++) {
        omp_destroy_lock(&queuelock[k]);
        freeQueue(queues[k]);
    }
    free(queues);
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime(); 
    if (pid==0) printf("file processing and mapping time is %f\n", t2 - t1); 
    
    // --------- DEFINE THE STRUCT DATA TYPE TO SEND
    local_time = -MPI_Wtime();
    MPI_Aint disps[2];
    int blocklens[] = {WORD_MAX_LENGTH, 1};
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};

    disps[0] = offsetof(pair, word);
    disps[1] = offsetof(pair, count);
    MPI_Datatype istruct;
    MPI_Type_create_struct(2, blocklens, disps, types, &istruct);
    MPI_Type_commit(&istruct);
    
    // assign to nodes
    struct Queue **reducerQueues = (struct Queue **)malloc(sizeof(struct Queue *) * size);
    #pragma omp parallel for
    for (k=0; k<size; k++) {
        reducerQueues[k] = initQueue();
    } 

    #pragma omp parallel num_threads(nthreads)
    {   
        int tid = omp_get_thread_num();
        int interval = HASH_CAPACITY / nthreads;
        int start = interval * tid;
        int end = interval * (tid+1);
        int i, j;
        int hscode, target_pid;
        int size_per_node = HASH_CAPACITY / size;
        size_t len;
        if (end>HASH_CAPACITY) end=HASH_CAPACITY;
        item* current;
        
        for (j = 0; j < nRM; j++){
            for (i = start; i < end; i++){
                current = hash_tables[j]->entries[i];
                if (current == NULL)
                    continue;
                else{
                    char *key = NULL;
                    key = strdup(current->key);
                    hscode = hashcode(key) % HASH_CAPACITY;
                    target_pid = hscode/size_per_node;
                    len = (size_t) current->count;
                    insertQHashKey(reducerQueues[target_pid], key, len); 
                    free(key);
                }
            }
        }
    }
    
    #pragma omp parallel for 
    for (k=0; k<nRM; k++) {
        freeHT(hash_tables[k]);
    } 
    free(hash_tables);
    
    // assign to nodes
    struct ht **reduceTables = (struct ht **)malloc(sizeof(struct ht *) * nReduce);
    struct Queue **localRQ = (struct Queue **)malloc(sizeof(struct Queue *) * nReduce);
    omp_lock_t myqueuelocks[nReduce];
    #pragma omp parallel for
    for (k=0; k<nReduce; k++) {
        reduceTables[k] = ht_create(HASH_CAPACITY/size);
        localRQ[k] = initQueue();
        omp_init_lock(&myqueuelocks[k]);
    }
    
    int src_p, tgt_p, nsend;  // send from k to next, recv from prev
    struct QNode* temp = NULL;
    struct Queue* q = NULL;
    size_t len;
    int recv_j = 0;
    int comm_size = 128;
    int interval = HASH_CAPACITY / size / nthreads;
    int remain = 2*(size-1);
    int hscode, tgt_qid;
    // int remain_recv = size-1;
    int doneRecv[size];
    int doneSend[size];
    int transCount[size];
    for (i=0; i<size; i++){
        doneRecv[i] = 0;
        doneSend[i] = 0;
        transCount[i] = 0;
    }
    // assign to thread
    q = reducerQueues[pid];
    while (q->front){
        temp = removeNode(q);
        // printf("pid tid: %d %d temp: %s \n", pid, tid, temp->line);
        // If front becomes NULL, then change rear also as NULL
        if (q->front == NULL) q->rear = NULL;
        char *key = NULL;
        key = strdup(temp->line);
        len = temp->len;
        hscode = hashcode(key) % (HASH_CAPACITY/size);
        tgt_qid = hscode / interval;
        insertQHashKey(localRQ[tgt_qid], key, len); 
        free(key);
        transCount[pid]++;
        if (temp != NULL) {
            free(temp->line);
            free(temp);
        }
    }
    // communication via pair format
    while(remain > 0){
        for (k = 1; k < size; k++) {
            // start send
            tgt_p = (pid + k) % size; // don't send to self
            src_p = (size + pid - k) % size;
            if (!doneSend[tgt_p]){
                nsend = 0;
                q = reducerQueues[tgt_p];
                // to alleviate pressure of communication, send and recv once with all other processes
                pair pairs[comm_size];
                while (q && q->front){
                    // can send
                    temp = q->front;
                    q->front = q->front->next;
                    pairs[nsend].count = (int) temp->len;
                    strcpy(pairs[nsend].word, temp->line);
                    if (q->front == NULL) q->rear = NULL;
                    nsend++;
                    if (temp != NULL) {
                        free(temp->line);
                        free(temp);
                    }
                    if (nsend == comm_size) break;
                }
                if (q->front==NULL && nsend<comm_size){
                    strcpy(pairs[nsend].word, empty_flag);
                    pairs[nsend].count = 1;
                    nsend++;
                    remain--;
                    doneSend[tgt_p] = 1;
                }
                MPI_Isend(pairs, nsend, istruct, tgt_p, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &request);
            }
            if (!doneRecv[src_p]){
                // start recv
                pair recv_pairs[comm_size];
                MPI_Recv(recv_pairs, comm_size, istruct, src_p, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, istruct, &recv_j); 
                for (i = 0; i < recv_j; i++) {
                    char *key = NULL;
                    pair recv_pair = recv_pairs[i];
                    len = (size_t) recv_pair.count;
                    key = strdup(recv_pair.word);
                    if (strcmp(key, empty_flag)==0){
                        doneRecv[src_p] = 1;
                        remain--;
                        free(key);
                        break;
                    }
                    //assign to thread
                    hscode = hashcode(key) % (HASH_CAPACITY/size);
                    tgt_qid = hscode / interval;
                    insertQHashKey(localRQ[tgt_qid], key, len); 
                    transCount[src_p]++;
                    free(key);
                }
            }
            if (!doneSend[tgt_p] && !doneRecv[src_p]){
                MPI_Wait(&request, &status);  
            }
        }
    }
    printf("pid = %d, recv count = %d, %d, %d, %d\n", pid, transCount[0], transCount[1], transCount[2], transCount[3]);
    MPI_Barrier(MPI_COMM_WORLD);
    local_time += MPI_Wtime();
    if (pid==0) printf("communication takes time %f \n", local_time);
    
    local_time -= MPI_Wtime();
    #pragma omp parallel shared(reducerQueues, pid) num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        int i;
        queueToHtWoL(localRQ[tid], reduceTables[tid]);
        freeQueue(localRQ[tid]);
        
        char* filename = (char*)malloc(sizeof(char) * 32);
        sprintf(filename, "../output/mpi/%d_%d.txt", pid, tid);
        FILE* fp = fopen(filename, "w");
        item* current;
        // printTable(sum_table);
        for (i = 0; i < reduceTables[tid]->capacity; i++){
            current = reduceTables[tid]->entries[i];
            if (current == NULL)
                continue;
            fprintf(fp, "key: %s, frequency: %d\n", current->key, current->count);
        }
        freeHT(reduceTables[tid]);
    }
    free(localRQ);
    free(reduceTables);
    free(reducerQueues);
    MPI_Barrier(MPI_COMM_WORLD);
    local_time += MPI_Wtime();
    if (pid==0) printf("reduction and writing takes time %f \n", local_time);
    global_time += MPI_Wtime();
    sprintf(tmp_out, "pid %d, %.8f \n", pid, global_time);
    strcat(csv_out, tmp_out);
    printf(csv_out);
    MPI_Finalize();
    return 0;
}