

typedef struct {
    int *array;    // Vetor de armazenamento
    int capacity;  // Capacidade máxima da fila
    int size;      // Quantidade atual de elementos
    int front;     // Índice do primeiro elemento
    int rear;      // Índice do último elemento
} CircularQueue;





CircularQueue* createQueue(int capacity);
void destroyQueue(CircularQueue *queue);
int enqueue(CircularQueue *queue, int item);
int dequeue(CircularQueue *queue);
int peek(CircularQueue *queue);
void rotateConsumer(CircularQueue *queue);

