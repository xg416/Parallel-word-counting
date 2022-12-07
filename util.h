
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <time.h>

#include "queue.h"
#include "ht.h"

extern int errno;
extern int DEBUG_MODE;

#define FILE_NAME_BUF_SIZE 50

void delay(int milli_seconds)
{  
    // Storing start time
    clock_t start_time = clock();
    // looping till required time is not achieved
    while (clock() < start_time + milli_seconds);
}

int get_file_list(struct Queue *file_name_queue, char *dirpath)
{
    DIR *dir;
    struct dirent *in_file;

    char dirname[FILE_NAME_BUF_SIZE];
    // Assuming Linux only. Null character needs to be added to avoid garbage
    char directory_seperator[2] = "/\0";
    strcpy(dirname, dirpath);
    int file_count = 0;

    if ((dir = opendir(dirname)) == NULL)
    {
        fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
        return -1;
    }
    while ((in_file = readdir(dir)))
    {
        /* we don't want current and parent directories */
        if (!strcmp(in_file->d_name, ".") || !strcmp(in_file->d_name, "..") ||
            !strcmp(in_file->d_name, "./") || !strcmp(in_file->d_name, "../"))
            continue;

        /* Open directory entry file for common operation */
        // mallocing 3 times the directory buffer size for file_name
        char *file_name = (char *)malloc(sizeof(char) * FILE_NAME_BUF_SIZE * 3);
        strcpy(file_name, dirname);
        strcat(file_name, directory_seperator);
        strcat(file_name, in_file->d_name);
        printf("Queuing file: %s\n", file_name);
        #pragma omp critical
        {
            // To be executed only by one thread at a time as there is a single queue
            enQueue(file_name_queue, file_name, strlen(file_name)+1);
            file_count++;
        }
    }

        printf("Done Queuing all files\n\n");
    closedir(dir);
    return file_count;
}

void ht_mergeWL(ht* tgt_table, ht* src_table, int start, int end, omp_lock_t* reducelock)
{
    int i;
    int count;
    int hscode;
    item *current, *tgt_get;
    for (i = 0; i < src_table->capacity; i++)
    {
        current = src_table->entries[i];
        if (current == NULL)
            continue;
        else{
            hscode = hashcode(current->key) % src_table->capacity;
            if (hscode >= start && hscode < end){
                omp_set_lock(reducelock);
                tgt_get = ht_update(tgt_table, current->key, current->count);
                omp_unset_lock(reducelock);
            } 
        }
    }
}


void ht_merge(ht* tgt_table, ht* src_table, int start, int end)
{
    int i;
    item *current, *tgt_get;

    for (i = start; i < end; i++)
    {
        current = src_table->entries[i];
        if (current == NULL)
            continue;
        else{
            tgt_get = ht_update(tgt_table, current->key, current->count);
        }
    }
}

void ht_merge_remap(ht* tgt_table, ht* src_table, int start, int end)
{
    int i, hscode;
    item *current, *tgt_get;
    int table_size = src_table->capacity;
    for (i = 0; i < table_size; i++)
    {
        current = src_table->entries[i];
        if (current == NULL)
            continue;
        else{
            hscode = hashcode(current->key) % table_size;
            if (hscode >= start && hscode < end){
                tgt_get = ht_update(tgt_table, current->key, current->count);
            }
            else{
                continue;
            }
            
        }
    }
}
/**
 * Format string with only lower case alphabetic letters
 */
char *format_string(char *original)
{
    int len = strlen(original)+1;
    char *word = (char *)malloc(len * sizeof(char));
    int c = 0;
    for (int i = 0; i < len; i++)
    {
        if (isalnum(original[i]) || original[i] == '\'')
        {
            word[c] = tolower(original[i]);
            c++;
        }
    }
    word[c] = '\0';
    return word;
}

void populateQueue(struct Queue *q, char *file_name)
{
    // file open operation
    FILE *filePtr;
    if ((filePtr = fopen(file_name, "r")) == NULL)
    {
        fprintf(stderr, "could not open file: [%p], err: %d, %s\n", filePtr, errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    // read line by line from the file and add to the queue
    size_t len;
    char *line = NULL;
    int line_count = 0;
    ssize_t n;
    while ((n = getline(&line, &len, filePtr)) != -1)
    {
        enQueue(q, line, n+1);
        line_count++;
    }
    // printf("line count %d, %s\n", line_count, file_name);
    fclose(filePtr);
    free(line);
}


void populateQueueDynamic(struct Queue *q, char *file_name, omp_lock_t *queuelock)
{
    // file open operation
    FILE *filePtr;
    if ((filePtr = fopen(file_name, "r")) == NULL)
    {
        fprintf(stderr, "could not open file: [%p], err: %d, %s\n", filePtr, errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    // read line by line from the file and add to the queue
    size_t len = 0;
    ssize_t n;
    char *line = NULL;
    int line_count = 0;
    struct QNode *temp_node;
    while ((n = getline(&line, &len, filePtr)) != -1)
    {
        omp_set_lock(queuelock);
        temp_node = newNode(line, n+1);
        enQueueData(q, temp_node);
        line_count++;
        omp_unset_lock(queuelock);
    }
    // printf("pid, tid: %d %d, line count %d, %s\n", pid, tid, line_count, file_name);
    fclose(filePtr);
    q->NoMoreNode = 1;
    free(line);
}

void populateHashMap(struct Queue *q, ht *hashMap)
{
    struct item* node = NULL;
    // wait until queue is good to start. Useful for parallel accesses.
    while (q == NULL)
        continue;
    while (q->front)
    {
        if (q->front == NULL) {
            continue;
        }
        char str[q->front->len];
        strcpy(str, q->front->line);
        char *token;
        char *rest = str;
        // https://www.geeksforgeeks.org/strtok-strtok_r-functions-c-examples/
        while ((token = strtok_r(rest, " ", &rest)))
        {
            char *word = format_string(token);
            if (strlen(word) > 0)
            {
                node = ht_update(hashMap, word, 1);
            }
            free(word);
        }
        deQueue(q);
    }
}

void populateHashMapWL(struct Queue* q, struct ht* hashMap, omp_lock_t* queuelock)
{
    struct item* node = NULL;
    struct QNode* temp = NULL;
    // wait until queue is good to start. Useful for parallel accesses.
    // printf("pid tid: %d %d waiting queue \n", pid, tid);
    while (q == NULL)
        continue;
    // if (q->front == NULL && !q->NoMoreNode) {printf("pid tid: %d %d: q->front is NULL \n", pid, tid);}
    
    while (q->front || !q->NoMoreNode)
    {
        // this block should be locked ------------------------------------------------------------------------------//
        omp_set_lock(queuelock);
        if (q->front == NULL) {
            omp_unset_lock(queuelock);
            continue;
        }

        temp = q->front;
        q->front = q->front->next;
        // If front becomes NULL, then change rear also as NULL
        if (q->front == NULL) q->rear = NULL;

        omp_unset_lock(queuelock);
        char str[temp->len];
        strcpy(str, temp->line);

        // separated out freeing part to save some time lost due to locking
        if (temp != NULL) {
            free(temp->line);
            free(temp);
        }

        char* token;
        char* rest = str;
        // https://www.geeksforgeeks.org/strtok-strtok_r-functions-c-examples/
        while ((token = strtok_r(rest, " ", &rest)))
        {
            char* word = format_string(token);
            if (strlen(word) > 0)
            {
                // printf("word %s \n", word);
                node = ht_update(hashMap, word, 1);
            }
            free(word);
        }
    }
}


void populateRQ(struct Queue *q, ht* src_table, int start, int end, int tid) //, omp_lock_t* htlock)
{
    size_t len;
    int i;
    int hscode;
    struct item *current;
    int table_size = src_table->capacity;
    for (i = 0; i < table_size; i++)
    {
        current = src_table->entries[i];
        if (current == NULL)
            continue;
        else{
            char *key = NULL;
            key = strdup(current->key);
            hscode = hashcode(key) % table_size;
            if (hscode >= start && hscode < end){
                len = (size_t) current->count;
                enQueueHashKey(q, key, len); 
                free(key);
            }
            else{
                continue;
            }
        }
    }
    q->NoMoreNode = 1;
}


void queueToHtWoL(struct Queue* q, ht* hashMap)
{
    struct item* node = NULL;
    struct QNode* temp = NULL;
    int count;
    // wait until queue is good to start. Useful for parallel accesses.
    // printf("pid tid: %d %d waiting queue \n", pid, tid);
    while (q->front){
        temp = deQueueData(q);
        // printf("pid tid: %d %d temp: %s \n", pid, tid, temp->line);
        // If front becomes NULL, then change rear also as NULL
        if (q->front == NULL) q->rear = NULL;
        char *word = NULL;
        word = strdup(temp->line);
        count = (int) temp->len;
        if (temp != NULL) {
            node = ht_update(hashMap, word, count);
            free(temp->line);
            free(temp);
            free(word);
        }
    }
}

