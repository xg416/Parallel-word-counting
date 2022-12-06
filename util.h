
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
            enQueue(file_name_queue, file_name, strlen(file_name));
            file_count++;
        }
    }

        printf("Done Queuing all files\n\n");
    closedir(dir);
    return file_count;
}

/**
 * Format string with only lower case alphabetic letters
 */
char* format_string(char* original)
{
    int len = strlen(original);
    char* word = (char*)malloc(len * sizeof(char));
    int c = 0;
    int i;
    for (i = 0; i < len; i++)
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
    size_t len = 0;
    char *line = NULL;
    int line_count = 0;
    while (getline(&line, &len, filePtr) != -1)
    {
        enQueue(q, line, len);
        line_count++;
    }
    // printf("line count %d, %s\n", line_count, file_name);
    fclose(filePtr);
    free(line);
}


void populateQueueWL_ML(struct Queue *q, char *file_name, omp_lock_t *queuelock)
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
    char *line = NULL;
    int line_count = 0;
    int file_done = 0;
    int i;
    int lines_per_iter = 30;
    int actual_lines;
    char *word = NULL;
    struct QNode **temp_nodes;
    temp_nodes = (struct QNode **) malloc(sizeof(struct QNode *) * lines_per_iter);
    while (file_done != 1)
    {
        actual_lines = 0;
        for (i=0; i<lines_per_iter; i++){
            if (getline(&line, &len, filePtr) == -1) {
                file_done = 1;
                break;
            } else {
                // separated out the node creation to save some time lost due to locking
                temp_nodes[i] = newNode(line, len);
                actual_lines++;
                line_count++;
            }
        }
        omp_set_lock(queuelock);
        for (i=0; i<actual_lines; i++){
            if (temp_nodes[i] != NULL)
                enQueueData(q, temp_nodes[i]);
        }
        omp_unset_lock(queuelock);

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
    char *line = NULL;
    int line_count = 0;
    struct QNode *temp_node;
    while (getline(&line, &len, filePtr) != -1)
    {
        omp_set_lock(queuelock);
        temp_node = newNode(line, len);
        enQueueData(q, temp_node);
        line_count++;
        omp_unset_lock(queuelock);
    }
    // printf("line count %d, %s\n", line_count, file_name);
    fclose(filePtr);
    free(line);
}


void populateHashMapWL(struct Queue* q, struct ht* hashMap, omp_lock_t* queuelock)
{
    struct item* node = NULL;
    struct QNode* temp = NULL;
    // wait until queue is good to start. Useful for parallel accesses.
    while (q == NULL)
        continue;
    while (q->front)
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
        if (q->front == NULL)
            q->rear = NULL;

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
                node = ht_update(hashMap, word, 1);
            }
            free(word);
        }
    }
}