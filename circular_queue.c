#include "circular_queue.h"
#include <stdlib.h>

CircularQueue* createQueue(int capacity) {
    CircularQueue *queue = malloc(sizeof(CircularQueue));
    if (!queue) return NULL;

    queue->capacity = capacity;
    queue->array = malloc(sizeof(int) * capacity);
    if (!queue->array) {
        free(queue);
        return NULL;
    }
    queue->size = 0;
    queue->front = 0;
    queue->rear = capacity - 1;

    return queue;
}

void destroyQueue(CircularQueue *queue) {
    if (queue) {
        free(queue->array);
        free(queue);
    }
}

int enqueue(CircularQueue *queue, int item) {
    if (queue->size == queue->capacity) return -1;  
    queue->rear = (queue->rear + 1) % queue->capacity;
    queue->array[queue->rear] = item;
    queue->size++;
    return 0;
}

int dequeue(CircularQueue *queue) {
    if (queue->size == 0) return -1;  

    int item = queue->array[queue->front];
    queue->front = (queue->front + 1) % queue->capacity;
    queue->size--;
    return item;
}


int peek(CircularQueue *queue) {
    if (queue->size == 0) return -1;  
    return queue->array[queue->front];
}

void rotateConsumer(CircularQueue *queue) {
    if (queue->size > 0) {
        queue->rear = (queue->rear + 1) % queue->capacity;  
        queue->array[queue->rear] = queue->array[queue->front];  
        queue->front = (queue->front + 1) % queue->capacity;  
    }
}



















