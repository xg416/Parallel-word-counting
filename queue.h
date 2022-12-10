// reference: https://www.digitalocean.com/community/tutorials/queue-in-c
// reference: https://www.tutorialspoint.com/data_structures_algorithms/queue_program_in_c.htm

#ifndef QUEUE_H_INCLUDED
#define QUEUE_H_INCLUDED

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct QNode
{
	char *line;
	size_t len;
	struct QNode *next;
} node;

struct Queue
{
	int NoMoreNode;
	struct QNode *front, *rear;
} queue;

struct QNode *newNode(char *line, size_t len)
{
	struct QNode *current = (struct QNode *)malloc(sizeof(struct QNode));
	current->line = (char *)malloc(len * sizeof(char));
	strcpy(current->line, line);
	current->len = len;
	current->next = NULL;
	return current;
}

struct QNode *newNodeHashKey(char *line, size_t count)
{
	struct QNode *current = (struct QNode *)malloc(sizeof(struct QNode));
	size_t len = strlen(line) + 1;
	current->line = (char *)malloc(len * sizeof(char));
	strcpy(current->line, line);
	current->len = count;
	current->next = NULL;
	return current;
}

struct Queue *initQueue()
{
	struct Queue *q = (struct Queue *)malloc(sizeof(struct Queue));
	q->front = q->rear = NULL;
	q->NoMoreNode = 0;
	return q;
}

void insertNode(struct Queue *q, struct QNode *temp)
{
	// If queue is empty
	if (q->rear == NULL){
		q->front = q->rear = temp;
		return;
	}

	// Add the new node at the end of queue and change rear
	q->rear->next = temp;
	q->rear = q->rear->next;
}

struct QNode *removeNode(struct Queue *q)
{
	// If queue is empty, return NULL.
	if (q->front == NULL)
		return NULL;

	// Store previous front and move front one node ahead
	struct QNode *current = q->front;

	q->front = q->front->next;

	// If front becomes NULL, then change rear also as NULL
	if (q->front == NULL)
		q->rear = NULL;

	return current;
}

void insertQ(struct Queue *q, char *line, size_t len)
{
	// Create a new LL node
	struct QNode *current = newNode(line, len);
	insertNode(q, current);
}

void insertQHashKey(struct Queue *q, char *line, size_t len)
{
	// Create a new LL node
	struct QNode *current = newNodeHashKey(line, len);
	insertNode(q, current);
}

void removeQ(struct Queue *q)
{
	struct QNode *current = removeNode(q);
	if (current == NULL) {
		return;
	}
	free(current->line);
	free(current);
}

void freeQueue(struct Queue *q)
{
	struct QNode *current = removeNode(q);
	while (current != NULL){
		free(current->line);
		free(current);
		current = removeNode(q);
	}
	return;
}

void printQueue(struct Queue *q)
{
	struct QNode *current;
	int count=0;
	while(q->front){
		current = q->front;
		char str[current->len];
		strcpy(str, current->line);
		count++;
		q->front = q->front->next;
		printf("id %d, line: %s \n", count, str);
	}
}

#endif
