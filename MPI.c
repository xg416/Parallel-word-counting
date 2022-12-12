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
#define PAIR_CAPACITY 1024

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
    int nThreads, nReader, nMapper;
    int nf_repeat;
    char *files_dir;
    nThreads = atoi(argv[1]);      // first argument, # of thread
    nReader = atoi(argv[2]);       // second argument, # of reader thread  
    files_dir = argv[3];           // 3rd argument, the folder of all input files
    nf_repeat = atoi(argv[4]);     // 4th argument, repeat times of all files

    int size, pid, p_name_len;
    char p_name[MPI_MAX_PROCESSOR_NAME];
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Get_processor_name(p_name, &p_name_len);
    
    if (pid==0) printf("Number of thread: %d, reader%d, file from %s, repeat files %d \n", nThreads, nReader, files_dir, nf_repeat); 
    nMapper = nThreads - nReader;
    int nReduce = nThreads;
    double total_time = -MPI_Wtime();
    double temp_timer = 0;

    int recv_pid;
    int i, j, k;
    int file_count = 0;

    struct Queue *filesQueue;
    filesQueue = initQueue();
    struct Queue *local_file_queue;
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (pid==0){
        for (i = 0; i < nf_repeat; i++){
            file_count += createFileQ(filesQueue, files_dir);
        }
    }

    /*****************************************************************************************
     * Share files among the processes
     * master node send to other nodes on request
     *****************************************************************************************/
    // reader and mapper setting
    omp_lock_t requestlock;
    omp_init_lock(&requestlock);
    omp_lock_t queuelock[nMapper];
    struct Queue **queues;
    struct ht **hash_tables;
    queues = (struct Queue **)malloc(sizeof(struct Queue *) * nMapper);
    hash_tables = (ht **)malloc(sizeof(struct ht *) * nMapper);
    
    temp_timer = -omp_get_wtime();
    #pragma omp parallel for 
    for (k=0; k<nMapper; k++) {
        omp_init_lock(&queuelock[k]);
        queues[k] = initQueue();
        hash_tables[k] = ht_create(HASH_CAPACITY);
    } 
    temp_timer += omp_get_wtime();
    printf("pid %d initialization takes time %f\n", pid, temp_timer); 
    MPI_Barrier(MPI_COMM_WORLD);
    
    // cross node filename allocation setting
    MPI_Status status;
    MPI_Request request;
    int recv_len = 0;
    char empty_flag[] = "all done";
    double t1, t2; 
    t1 = MPI_Wtime();
    
    int queue_count = -1;
    // start allocating filenames then read & map
    if (pid == 0){
        char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_MAX_LENGTH);
        char *send_file = (char *)malloc(sizeof(char) * FILE_NAME_MAX_LENGTH);
        int len;
        int recv_pid = 0;
        int nReader_tot = (size-1) * nReader;
        #pragma omp parallel shared(queues, hash_tables, filesQueue, requestlock, queuelock) num_threads(nThreads+1)
        {
            char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_MAX_LENGTH);
            int tid = omp_get_thread_num();
            int queue_id;
            int counttt = 0;
            if (tid == nThreads){
                while (filesQueue->front != NULL){
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    omp_set_lock(&requestlock);
                    strcpy(send_file, filesQueue->front->line);
                    len = filesQueue->front->len;
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    // omp_set_lock(&requestlock);
                    removeQ(filesQueue);
                    omp_unset_lock(&requestlock);
                    counttt++;
                    if (filesQueue->front == NULL) break;
                }
                while(nReader_tot){
                    send_file = empty_flag;
                    MPI_Recv(&recv_pid, 1, MPI_INT, MPI_ANY_SOURCE, TAG_COMM_REQ_DATA, MPI_COMM_WORLD, &status);
                    MPI_Send(send_file, len + 1, MPI_CHAR, recv_pid, TAG_COMM_FILE_NAME, MPI_COMM_WORLD);
                    nReader_tot--;
                }
            }
            else if(tid < nReader){
                while (filesQueue->front != NULL){
                    omp_set_lock(&requestlock);
                    queue_count++;
                    strcpy(file_name, filesQueue->front->line);
                    removeQ(filesQueue);
                    omp_unset_lock(&requestlock);
                    queue_id = queue_count % nMapper;
                    // printf("pid %d tid %d get file %s \n", pid, tid, file_name);
                    populateQueueDynamic(queues[queue_id], file_name, &queuelock[queue_id]); 
                }
                for (i = 0; i< nMapper; i++){
                    queues[i]->NoMoreNode = 1;
                }
            }
            else{
                queue_id = tid - nReader;
                populateHashMapWL(queues[queue_id], hash_tables[queue_id], &queuelock[queue_id]);
                // printf("pid %d tid %d items in the table: %d \n", pid, tid, hash_tables[queue_id]->itemcount);
            }
        }
    }
    else{   
        #pragma omp parallel shared(queues, hash_tables, requestlock, queuelock) num_threads(nThreads)
        {
            char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_MAX_LENGTH);
            int tid = omp_get_thread_num();
            int queue_id;
            if (tid < nReader) {
                while (strcmp(file_name, empty_flag)!=0){
                    omp_set_lock(&requestlock);
                    queue_count++;
                    MPI_Send(&pid, 1, MPI_INT, 0, TAG_COMM_REQ_DATA, MPI_COMM_WORLD);
                    MPI_Recv(file_name, FILE_NAME_MAX_LENGTH, MPI_CHAR, 0, TAG_COMM_FILE_NAME, MPI_COMM_WORLD, &status);
                    MPI_Get_count(&status, MPI_CHAR, &recv_len);
                    omp_unset_lock(&requestlock);
                    if (strcmp(file_name, empty_flag)==0) break;
                    // printf("pid %d tid %d get file %s \n", pid, tid, file_name);
                    queue_id = queue_count % nMapper;
                    populateQueueDynamic(queues[queue_id], file_name, &queuelock[queue_id]);  
                }
                for (i = 0; i< nMapper; i++){
                    queues[i]->NoMoreNode = 1;
                }
            }
            else{
                queue_id = tid - nReader;
                populateHashMapWL(queues[queue_id], hash_tables[queue_id], &queuelock[queue_id]);
                // printf("pid %d tid %d items in the table %d\n", pid, tid, hash_tables[queue_id]->itemcount);
            }
            free(file_name);
        }
    }

    omp_destroy_lock(&requestlock);
    for (k=0; k<nMapper; k++) {
        omp_destroy_lock(&queuelock[k]);
        freeQueue(queues[k]);
    }
    free(queues);
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime(); 
    if (pid==0) printf("file processing and mapping time is %f\n", t2 - t1); 
    
    // --------- DEFINE THE STRUCT DATA TYPE TO SEND
    // assign to nodes
    struct Queue **reducerQueues = (struct Queue **)malloc(sizeof(struct Queue *) * size);
    #pragma omp parallel for
    for (k=0; k<size; k++) {
        reducerQueues[k] = initQueue();
    } 
    
    temp_timer = -MPI_Wtime();
    #pragma omp parallel num_threads(nThreads)
    {   
        int tid = omp_get_thread_num();
        int interval = HASH_CAPACITY / nThreads;
        int start = interval * tid;
        int end = interval * (tid+1);
        int i, j;
        int hscode, target_pid;
        int size_per_node = HASH_CAPACITY / size;
        size_t len;
        if (end>HASH_CAPACITY) end=HASH_CAPACITY;
        item* current;
        
        for (j = 0; j < nMapper; j++){
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
    t2 = MPI_Wtime(); 
    printf("PID %d takes time %f before sending data to reduction \n", pid, t2 - t1); 
    MPI_Barrier(MPI_COMM_WORLD);
    #pragma omp parallel for 
    for (k=0; k<nMapper; k++) {
        freeHT(hash_tables[k]);
    } 
    free(hash_tables);
    
    // assign to nodes
    MPI_Aint disps[2];
    int blocklens[] = {WORD_MAX_LENGTH, 1};
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};

    disps[0] = offsetof(pair, word);
    disps[1] = offsetof(pair, count);
    MPI_Datatype Tuple;
    MPI_Type_create_struct(2, blocklens, disps, types, &Tuple);
    MPI_Type_commit(&Tuple);
    
    struct ht **reduceTables = (struct ht **)malloc(sizeof(struct ht *) * nReduce);
    struct Queue **localRQ = (struct Queue **)malloc(sizeof(struct Queue *) * nReduce);
    omp_lock_t myqueuelocks[nReduce];
    #pragma omp parallel for
    for (k=0; k<nReduce; k++) {
        reduceTables[k] = ht_create(HASH_CAPACITY/size);
        localRQ[k] = initQueue();
        omp_init_lock(&myqueuelocks[k]);
    }
    
    int nsend, id_pair, n_recv_pair;  // send from k to next, recv from prev
    struct QNode* temp = NULL;
    struct Queue* q = NULL;
    size_t len;
    int recv_j = 0;
    int comm_size = PAIR_CAPACITY;
    int interval = HASH_CAPACITY / size / nThreads;
    int remain = size * 2;
    int hscode, tgt_tid;
    // int remain_recv = size-1;
    int recv_count[size];
    int send_count[size];
    int recv_disp[size];
    int send_disp[size];
    int doneRecv[size];
    int doneSend[size];
    for (i=0; i<size; i++){
        recv_count[i] = 0;
        send_count[i] = 0;
        recv_disp[i] = 0;
        send_disp[i] = 0;
        doneRecv[i] = 0;
        doneSend[i] = 0; 
    }
    
    // communication via pair format
    MPI_Barrier(MPI_COMM_WORLD);
    while(remain > 0){
        id_pair = 0;
        pair sendbuf[comm_size*size];
        pair recvbuf[comm_size*size];
        for (k = 0; k < size; k++) {
            q = reducerQueues[k];
            nsend = 0;
            while (q->front && !doneSend[k]){
                // can send
                temp = q->front;
                q->front = q->front->next;
                sendbuf[id_pair].count = (int) temp->len;
                strcpy(sendbuf[id_pair].word, temp->line);
                if (q->front == NULL) q->rear = NULL;
                id_pair++;
                nsend++;
                if (temp != NULL) {
                    free(temp->line);
                    free(temp);
                }
                if (nsend == comm_size) break;
            }
            if ((q==NULL || q->front==NULL) && nsend<comm_size){
                strcpy(sendbuf[id_pair].word, empty_flag);
                sendbuf[id_pair].count = 1;
                if (doneSend[k]==0){
                    remain--;
                    doneSend[k] = 1;
                }
                nsend++;
                id_pair++;
            }
            send_count[k] = nsend;
            if (k < size-1) {send_disp[k+1] = id_pair;}
        }
        // printf("pid %d, doneSend: %d %d %d  %d \n", pid, doneSend[0], doneSend[1], doneSend[2], doneSend[3]);
        
        MPI_Alltoall(send_count, 1, MPI_INT, recv_count, 1, MPI_INT, MPI_COMM_WORLD);
        for (k = 1; k < size; k++) {
            recv_disp[k] = recv_count[k-1] + recv_disp[k-1];
        }
        n_recv_pair = recv_disp[size-1] + recv_count[size-1];
        // printf("pid %d, send_count: %d %d %d  %d \n", pid, send_count[0], send_count[1], send_count[2], send_count[3]);
        // printf("pid %d, recv_disp: %d %d %d  %d \n", pid, recv_disp[0], recv_disp[1], recv_disp[2], recv_disp[3]);
        MPI_Alltoallv(sendbuf, send_count, send_disp, Tuple, recvbuf, recv_count, recv_disp, Tuple, MPI_COMM_WORLD);
        // printf("pid %d, recv_count: %d %d %d  %d \n", pid, recv_count[0], recv_count[1], recv_count[2], recv_count[3]);
        for (k = 0; k < size; k++) {
            int recv_id = recv_disp[k];
            for (i = 0; i < recv_count[k]; i++){
                char *key = NULL;
                pair recv_pair = recvbuf[recv_id+i];
                len = (size_t) recv_pair.count;
                key = strdup(recv_pair.word);
                if (strcmp(key, empty_flag)==0){
                    if (doneRecv[k]==0){
                        remain--;
                        doneRecv[k] = 1;
                    }
                    free(key);
                    break;
                }
                hscode = hashcode(key) % (HASH_CAPACITY/size);
                tgt_tid = hscode / interval;
                insertQHashKey(localRQ[tgt_tid], key, len); 
                free(key);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    temp_timer += MPI_Wtime();
    if (pid==0) printf("communication takes time %f \n", temp_timer);

    temp_timer -= MPI_Wtime();
    #pragma omp parallel shared(reducerQueues, localRQ, pid) num_threads(nThreads)
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
    temp_timer += MPI_Wtime();
    total_time += MPI_Wtime();
    if (pid==0) printf("reduction and writing takes time %f \n", temp_timer);
    if (pid==0) printf("pid %d, %.8f \n", pid, total_time);
    MPI_Finalize();
    return 0;
}