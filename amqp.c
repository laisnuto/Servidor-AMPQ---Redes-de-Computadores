#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "circular_queue.h"


#define LISTENQ 1
#define MAXLINE 4096
#define METHOD 1
#define HEADER 2
#define BODY 3
#define HEARTBEAT 8
#define HEADER_SIZE 8
#define METHOD_PAYLOAD_HEADER_SIZE 4
#define MAX_CONSUMERS 200
#define MAX_QUEUES 200  

#define CONNECTION_START_OK_METHOD_ID   11
#define CONNECTION_TUNE_OK_METHOD_ID    31
#define CONNECTION_OPEN_METHOD_ID       40    
#define CHANNEL_OPEN_METHOD_ID          10
#define CHANNEL_CLOSE_METHOD_ID         40
#define CONNECTION_CLOSE_METHOD_ID      50


#define CHANNEL_CLOSE_CLASS_ID          20
#define DECLARE_QUEUE_METHOD_ID         10
#define QUEUE_CLASS                     50
#define PUBLISH_QUEUE_METHOD_ID         40
#define PUBLISH_CLASS                   60
#define CONSUME_QUEUE_METHOD_ID         20
#define CONSUME_CLASS                   60
#define DELIVER_METHOD                  60
#define CONSUME_METHOD_OK               21
#define ACK_METHOD_ID                   80


/*struct que armazena as principais informações de um pacote lido*/
typedef struct Packet {
    uint8_t type;
    uint16_t channel;
    uint32_t length;
    char* payload;
    uint16_t method;
    uint16_t class;
    char* arguments;
    
} Packet;

/*lista ligada para guardar as mensagens*/
typedef struct MessageNode {
    char *message;                     
    struct MessageNode *next;          
} MessageNode;


/*struct que armazena da fila de mensagens (lista ligada) e a fila de consumidores (fila ciircular)*/
typedef struct Queue {
    char* queueName;                         // Nome da fila
    CircularQueue* consumerQueue;            // Fila de consumidores
    MessageNode* messageFront, *messageRear; // Fila de mensagens
    pthread_mutex_t queueMutex;              // Mutex para a fila
} Queue;





int client_closed_connection = 0;

Queue* queues[MAX_QUEUES];
int numQueues = 0;
pthread_mutex_t queuesMutex = PTHREAD_MUTEX_INITIALIZER;


void printQueue(Queue* q) {
    if (!q) {
        printf("Queue is NULL.\n");
        return;
    }
    printf("Queue Name: %s \n", q->queueName);
    printf("Consumers in the Queue:\n");
    for (int i = 0; i < q->consumerQueue->size; i++) {
        printf("%d, ", q->consumerQueue->array[(q->consumerQueue->front + i) % MAX_CONSUMERS]);
    }
    printf("\n");
    printf("\n");

    printf("Messages in the Queue:\n");
    MessageNode* currentMessage = q->messageFront;
    while (currentMessage) {
        printf("%s\n", currentMessage->message);
        currentMessage = currentMessage->next;
    }
    printf("------- End of Queue %s -------\n\n", q->queueName);
}

void printAllQueues() {
    pthread_mutex_lock(&queuesMutex);  
    for (int i = 0; i < numQueues; i++) {
        printQueue(queues[i]);
    }
    pthread_mutex_unlock(&queuesMutex);  
}



/*função que lê os pacotes e identifica os compos da struct Packet*/
Packet* read_packet(int connfd) {
    uint8_t lastByte;
    Packet* newMessage = malloc(sizeof(Packet));
    if (!newMessage) {
        return NULL;  
    }

    int n = read(connfd, &(newMessage->type), 1);
    if (n <= 0) {
        if (n == 0) client_closed_connection = 1;  
        free(newMessage);
        return NULL;
    }

    n = read(connfd, &(newMessage->channel), 2);
    if (n <= 0) {
        free(newMessage);
        return NULL;
    }
    newMessage->channel = ntohs(sizeof(char*)*newMessage->channel);

    n = read(connfd, &(newMessage->length), 4);
    if (n <= 0) {
        free(newMessage);
        return NULL;
    }
    newMessage->length = ntohl(newMessage->length);

    char* payload =  malloc(sizeof(char*)*newMessage->length);
    if (!payload) {
        free(newMessage);
        return NULL; 
    }

    n = read(connfd, payload, newMessage->length);
    if (n <= 0) {
        free(payload);
        free(newMessage);
        return NULL;
    }

    newMessage->payload = payload;

    if (newMessage->type == METHOD) {
        newMessage->class = ntohs(*((uint16_t*) &payload[0]));
        newMessage->method = ntohs(*((uint16_t*) &payload[2]));
        newMessage->arguments = payload + METHOD_PAYLOAD_HEADER_SIZE;
    }  
    else if(newMessage->type == HEADER){
        newMessage->class = ntohs(*((uint16_t*) &payload[0]));
        newMessage->method = 0;
        newMessage->arguments = payload + 2 ;
    }
    else if(newMessage->type == BODY){
        newMessage->class = ntohs(*((uint16_t*) &payload[0]));
        newMessage->method =  ntohs(*((uint16_t*) &payload[2]));
        newMessage->arguments = payload + METHOD_PAYLOAD_HEADER_SIZE;
    }
    else if(newMessage->type == HEARTBEAT){
        newMessage->class = ntohs(*((uint16_t*) &payload[0]));
        newMessage->method = ntohs(*((uint16_t*) &payload[2]));
        newMessage->arguments = payload + METHOD_PAYLOAD_HEADER_SIZE;
    }
    else {
        newMessage->class = -1;
        newMessage->method = -1;
        newMessage->arguments = payload;
    }
    
    n = read(connfd, &lastByte, 1);

    if(lastByte != 0xCE){
        return NULL;        
    }

    return newMessage;    

}




int start_connection(int connfd){
    char packet[] = "\x01\x00\x00\x00\x00\x01\xf3\x00\x0a\x00\x0a\x00\x09\x00\x00\x01" \
    "\xce\x0c\x63\x61\x70\x61\x62\x69\x6c\x69\x74\x69\x65\x73\x46\x00" \
    "\x00\x00\xc7\x12\x70\x75\x62\x6c\x69\x73\x68\x65\x72\x5f\x63\x6f" \
    "\x6e\x66\x69\x72\x6d\x73\x74\x01\x1a\x65\x78\x63\x68\x61\x6e\x67" \
    "\x65\x5f\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x62\x69\x6e\x64\x69" \
    "\x6e\x67\x73\x74\x01\x0a\x62\x61\x73\x69\x63\x2e\x6e\x61\x63\x6b" \
    "\x74\x01\x16\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x63\x61\x6e\x63" \
    "\x65\x6c\x5f\x6e\x6f\x74\x69\x66\x79\x74\x01\x12\x63\x6f\x6e\x6e" \
    "\x65\x63\x74\x69\x6f\x6e\x2e\x62\x6c\x6f\x63\x6b\x65\x64\x74\x01" \
    "\x13\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x70\x72\x69\x6f\x72\x69" \
    "\x74\x69\x65\x73\x74\x01\x1c\x61\x75\x74\x68\x65\x6e\x74\x69\x63" \
    "\x61\x74\x69\x6f\x6e\x5f\x66\x61\x69\x6c\x75\x72\x65\x5f\x63\x6c" \
    "\x6f\x73\x65\x74\x01\x10\x70\x65\x72\x5f\x63\x6f\x6e\x73\x75\x6d" \
    "\x65\x72\x5f\x71\x6f\x73\x74\x01\x0f\x64\x69\x72\x65\x63\x74\x5f" \
    "\x72\x65\x70\x6c\x79\x5f\x74\x6f\x74\x01\x0c\x63\x6c\x75\x73\x74" \
    "\x65\x72\x5f\x6e\x61\x6d\x65\x53\x00\x00\x00\x16\x72\x61\x62\x62" \
    "\x69\x74\x40\x6c\x61\x69\x73\x6e\x75\x74\x6f\x2d\x35\x35\x30\x58" \
    "\x44\x41\x09\x63\x6f\x70\x79\x72\x69\x67\x68\x74\x53\x00\x00\x00" \
    "\x2e\x43\x6f\x70\x79\x72\x69\x67\x68\x74\x20\x28\x63\x29\x20\x32" \
    "\x30\x30\x37\x2d\x32\x30\x31\x39\x20\x50\x69\x76\x6f\x74\x61\x6c" \
    "\x20\x53\x6f\x66\x74\x77\x61\x72\x65\x2c\x20\x49\x6e\x63\x2e\x0b" \
    "\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69\x6f\x6e\x53\x00\x00\x00\x39" \
    "\x4c\x69\x63\x65\x6e\x73\x65\x64\x20\x75\x6e\x64\x65\x72\x20\x74" \
    "\x68\x65\x20\x4d\x50\x4c\x20\x31\x2e\x31\x2e\x20\x57\x65\x62\x73" \
    "\x69\x74\x65\x3a\x20\x68\x74\x74\x70\x73\x3a\x2f\x2f\x72\x61\x62" \
    "\x62\x69\x74\x6d\x71\x2e\x63\x6f\x6d\x08\x70\x6c\x61\x74\x66\x6f" \
    "\x72\x6d\x53\x00\x00\x00\x11\x45\x72\x6c\x61\x6e\x67\x2f\x4f\x54" \
    "\x50\x20\x32\x32\x2e\x32\x2e\x37\x07\x70\x72\x6f\x64\x75\x63\x74" \
    "\x53\x00\x00\x00\x08\x52\x61\x62\x62\x69\x74\x4d\x51\x07\x76\x65" \
    "\x72\x73\x69\x6f\x6e\x53\x00\x00\x00\x05\x33\x2e\x38\x2e\x32\x00" \
    "\x00\x00\x0e\x50\x4c\x41\x49\x4e\x20\x41\x4d\x51\x50\x4c\x41\x49" \
    "\x4e\x00\x00\x00\x05\x65\x6e\x5f\x55\x53\xce";

    ssize_t bytes_written = write(connfd, packet, 507);
    return bytes_written;
}

int connection_tune(int connfd){
    char packet[] = "\x01\x00\x00\x00\x00\x00\x0c\x00\x0a\x00\x1e\x07\xff\x00\x02\x00" \
    "\x00\x00\x3c\xce";
    ssize_t bytes_written = write(connfd, packet, 20);
    return bytes_written;
}

int connection_open(int connfd){
    char packet[] = "\x01\x00\x00\x00\x00\x00\x05\x00\x0a\x00\x29\x00\xce";
    ssize_t bytes_written = write(connfd, packet, 13);
    return bytes_written;
}


int channel_open(int connfd){
    char packet[] = "\x01\x00\x01\x00\x00\x00\x08\x00\x14\x00\x0b\x00\x00\x00\x00\xce";
    ssize_t bytes_written = write(connfd, packet, 16);
    return bytes_written;
}


int channel_close(int connfd){
    char packet[] = "\x01\x00\x01\x00\x00\x00\x04\x00\x14\x00\x29\xce";
    ssize_t bytes_written = write(connfd, packet, 12);
    return bytes_written;
}

int connection_close(int connfd){
    char packet[] = "\x01\x00\x00\x00\x00\x00\x04\x00\x0a\x00\x33\xce";
    ssize_t bytes_written = write(connfd, packet, 12);
    return bytes_written;
}



/*função de escrever o pacote de declare queue ok concatenando byte por byte*/
int declare_queue_ok(int connfd, Packet* msg) {
    
    uint8_t queue_length = (uint8_t) msg->arguments[2];   
    char* queue_name =  malloc(sizeof(char)*queue_length);
    memcpy(queue_name, msg->arguments + 3, queue_length);

    uint16_t channel_n = htons(1);
    uint16_t class_id_n = htons(QUEUE_CLASS);
    uint16_t method_id_n = htons(11);

    uint32_t message_count = htonl(0);
    uint32_t consumer_count = htonl(0);

    uint32_t length = 2 + 2 + 1 + queue_length + 4 + 4;
    uint32_t length_n = htonl(length);

    char buffer[500];
    int offset = 0;

    buffer[offset++] = METHOD;

    memcpy(buffer + offset, &channel_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &length_n, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, &class_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &method_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    buffer[offset++] = queue_length;

    memcpy(buffer + offset, queue_name, queue_length);
    offset += queue_length;

    memcpy(buffer + offset, &message_count, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, &consumer_count, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    buffer[offset++] = 0xCE;

    free(queue_name);

    ssize_t bytes_written = write(connfd, buffer, length + 8);
    return bytes_written;
}

/*função de escrever o pacote de consume ok concatenando byte por byte*/
int consume_ok(int connfd) {

    uint16_t channel_n = htons(1);
    uint16_t class_id_n = htons(CONSUME_CLASS); 
    uint16_t method_id_n = htons(CONSUME_METHOD_OK); 

    
    char consumer_tag[] = "consumer-tag";
    uint8_t tag_length = 12;

    uint32_t length = 2 + 2 + 1 + tag_length; 
    uint32_t length_n = htonl(length);

    char buffer[500];
    int offset = 0;

    buffer[offset++] = METHOD;

    memcpy(buffer + offset, &channel_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &length_n, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, &class_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &method_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    buffer[offset++] = tag_length;

    memcpy(buffer + offset, consumer_tag, tag_length);
    offset += tag_length;

    buffer[offset++] = 0xCE;

    ssize_t bytes_written = write(connfd, buffer, length + 8);
    return bytes_written;

}


/*função de escrever na ordem correta 64 bits*/
uint64_t htoll(uint64_t hostlonglong) {
    uint32_t high = (hostlonglong >> 32) & 0xFFFFFFFF;
    uint32_t low = hostlonglong & 0xFFFFFFFF;
    high = htonl(high);
    low = htonl(low);
    return ((uint64_t)low << 32) | high;
}


/*função de escrever o pacote de basic deliver method concatenando byte por byte*/
int basic_deliver_method(int connfd,  char* queueName) {

    uint16_t channel_n = htons(1);
    uint16_t class_id_n = htons(CONSUME_CLASS);
    uint16_t method_id_n = htons(DELIVER_METHOD);
    uint64_t delivery_tag = htoll(1);

    char consumer_tag[] = "consumer-tag";
    uint8_t tag_length = 12;

    uint8_t routing_key_length = (strlen(queueName));
   
    uint8_t exchange = 0; 
    uint8_t redelivery_false = 0;

    uint32_t length = 2 + 2 + 1 + tag_length + 8 + 1 + 1  + 1 + routing_key_length;
    uint32_t length_n = htonl(length);

    char buffer[500];
    int offset = 0;

    buffer[offset++] = METHOD;

    memcpy(buffer + offset, &channel_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &length_n, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, &class_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &method_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    buffer[offset++] = tag_length;
    memcpy(buffer + offset, consumer_tag, tag_length);
    offset += tag_length;

    memcpy(buffer + offset, &delivery_tag, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    buffer[offset++] = exchange; 

    buffer[offset++] = redelivery_false; 

    buffer[offset++] = routing_key_length;
    memcpy(buffer + offset, queueName, strlen(queueName));
    offset += strlen(queueName);

    buffer[offset++] = 0xCE;

    return write(connfd, buffer, offset);

}


/*função de escrever o pacote de basic deliver header concatenando byte por byte*/
int basic_deliver_header(int connfd, char* messageContent) {
    
    uint16_t channel_n = htons(1);
    uint16_t class_id_n = htons(CONSUME_CLASS); 
    uint16_t weight = htons(0);
    uint64_t body_size = htoll(strlen(messageContent));


    uint32_t length = 2 + 2 + 8 + 2 + 1;
    uint32_t length_n = htonl(length);

    uint8_t delivery_mode = 1; 

    char buffer[500];
    int offset = 0;

    buffer[offset++] = HEADER;

    memcpy(buffer + offset, &channel_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &length_n, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, &class_id_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &weight, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &body_size, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    uint16_t property_flags = htons(0x1000); 
    memcpy(buffer + offset, &property_flags, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    buffer[offset++] = delivery_mode; 

    buffer[offset++] = 0xCE;

    return write(connfd, buffer, offset);

}


/*função de escrever o pacote de basic deliver body concatenando byte por byte*/
int basic_delivery_body(int connfd, char* messageContent) {
    uint16_t channel_n = htons(1);

    char payload[] = "payload";

    uint32_t payload_length = htonl(strlen(messageContent));

    char buffer[500];
    int offset = 0;

    buffer[offset++] = BODY;

    memcpy(buffer + offset, &channel_n, sizeof(uint16_t));
    offset += sizeof(uint16_t);

    memcpy(buffer + offset, &payload_length, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(buffer + offset, messageContent, strlen(messageContent));
    offset += strlen(messageContent);

    buffer[offset++] = 0xCE;

    return write(connfd, buffer, offset);

}


/*função que cria a struct Queue para receber os publishs e consumes com o mesmo nome da fila declarrada*/
void declareQueue(char* queueName) {
    pthread_mutex_lock(&queuesMutex);
    
    Queue* newQueue = malloc(sizeof(Queue));
    if (!newQueue) {
        pthread_mutex_unlock(&queuesMutex);
        return; // Falha na alocação de memória
    }

    newQueue->queueName = strdup(queueName);
    if (!newQueue->queueName) {
        free(newQueue);
        pthread_mutex_unlock(&queuesMutex);
        return; // Falha na alocação de memória
    }

    newQueue->consumerQueue = createQueue(MAX_CONSUMERS);
    newQueue->messageFront = newQueue->messageRear = NULL; // Inicializando a fila de mensagens
    pthread_mutex_init(&(newQueue->queueMutex), NULL);
    
    queues[numQueues++] = newQueue;
    
    pthread_mutex_unlock(&queuesMutex);
}


/*função que adiciona o consumidor na fila circular do nome correspondente*/
int addConsumerToQueue(char* queueName, int connfd) {
    pthread_mutex_lock(&queuesMutex);
    
    int isNext = 0;

    for(int i = 0; i < numQueues; i++) {
        if(strcmp(queues[i]->queueName, queueName) == 0) {
            pthread_mutex_lock(&(queues[i]->queueMutex));
            enqueue(queues[i]->consumerQueue, connfd);

            // Verifique se o consumidor é o próximo da fila
            if (connfd == peek(queues[i]->consumerQueue)) {
                isNext = 1;
            }

            pthread_mutex_unlock(&(queues[i]->queueMutex));
            break;
        }
    }
    
    pthread_mutex_unlock(&queuesMutex);
    return isNext;
}


/*função que remove o consumidor que foi desconectador da fila que ele estava*/
void removeConsumerFromQueue(char* queueName, int connfd) {
    pthread_mutex_lock(&queuesMutex);
    
    for(int i = 0; i < numQueues; i++) {
        if(strcmp(queues[i]->queueName, queueName) == 0) {
            pthread_mutex_lock(&(queues[i]->queueMutex));
            
            int size = queues[i]->consumerQueue->size;
            for (int j = 0; j < size; j++) {
                int consumer = dequeue(queues[i]->consumerQueue);
                if (consumer != connfd) {
                    enqueue(queues[i]->consumerQueue, consumer); 
                }
            }
            
            pthread_mutex_unlock(&(queues[i]->queueMutex));
            break;
        }
    }
    
    pthread_mutex_unlock(&queuesMutex);
}


/*função que coloca as mensagens publicadas na lista ligada de mensagens com onome correspondente*/
void publishMessageToQueue(char* queueName, const char *message) {
    pthread_mutex_lock(&queuesMutex);
    
    Queue* q = NULL;
    for (int i = 0; i < numQueues; i++) {
        if (strcmp(queues[i]->queueName, queueName) == 0) {
            q = queues[i];
            break;
        }
    }
    
    if (!q) {
        pthread_mutex_unlock(&queuesMutex);
        return; 
    }

    
    MessageNode *temp = (MessageNode *)malloc(sizeof(MessageNode));
    if (!temp) {
        pthread_mutex_unlock(&queuesMutex);
        return; 
    }

    temp->message = strdup(message);
    if (!temp->message) {
        free(temp);
        pthread_mutex_unlock(&queuesMutex);
        return; 
    }
    temp->next = NULL;

    pthread_mutex_lock(&(q->queueMutex));
    pthread_mutex_unlock(&queuesMutex); 

    if (q->messageRear == NULL) {
        q->messageFront = q->messageRear = temp;
    } else {
        q->messageRear->next = temp;
        q->messageRear = temp;
    }

    pthread_mutex_unlock(&(q->queueMutex));
}


/*função que consome a mensagem*/
/*pega o consumidor do topo da fila, envia mensagem para ele e o coloca no final da fila dos consumidore*/
/*remove a mensagem da lista ligada de mensagens*/
char *consumeMessageFromQueue(char* queueName) {
    pthread_mutex_lock(&queuesMutex);

    
    Queue* q = NULL;
    for (int i = 0; i < numQueues; i++) {
        if (strcmp(queues[i]->queueName, queueName) == 0) {
            q = queues[i];
            break;
        }
    }

    if (!q) {
        pthread_mutex_unlock(&queuesMutex);
        return NULL; 
    }

    pthread_mutex_lock(&(q->queueMutex));
    pthread_mutex_unlock(&queuesMutex); 
    
    if (q->messageFront == NULL) {
        pthread_mutex_unlock(&(q->queueMutex));
        return NULL; 
    }

    MessageNode* temp = q->messageFront;
    char *message = strdup(temp->message);
    q->messageFront = q->messageFront->next;

    if (q->messageFront == NULL) q->messageRear = NULL;

    free(temp->message);
    free(temp);

    
    rotateConsumer(q->consumerQueue);
    pthread_mutex_unlock(&(q->queueMutex));

    return message;
}


/*função que lida com cada cliente conectado*/
void *handle_client(void *arg) {


    int connfd = *(int *)arg;
    free(arg); 
    ssize_t n; 
    printf("[Uma conexão aberta]\n");
    client_closed_connection = 0;
            


    /* read o header do protocolo */
    char protocolHeaderPacket[8];
    n = read(connfd, protocolHeaderPacket, 8);

    if (n < 0 || memcmp(protocolHeaderPacket, "AMQP\x00\x00\x09\x01", 8) != 0) {
        write(connfd, "AMQP\x00\x00\x09\x01", 8);
        fprintf(stderr, "Erro ao ler o protocol header\n");
        return NULL;
    }

    /* write connection start */
    if (start_connection(connfd) < 0) {
        fprintf(stderr, "Erro ao escrever o pacote connection start \n");
        return NULL;
    }
    

    /* read Connection Start-ok */
    Packet* startOkPacket = read_packet(connfd);
    if (!startOkPacket || startOkPacket->method != CONNECTION_START_OK_METHOD_ID) {
        fprintf(stderr, "Erro ao ler o pacote Connection Start-ok \n");
        return NULL;
    }


    /* Write Connection Tune */
    if (connection_tune(connfd) < 0) {
        fprintf(stderr, "Erro ao escrever o pacote connection tune \n");
        return NULL;
    }



    /* read connection Tune Ok  */
    Packet* tuneOkPacket = read_packet(connfd);
    if (!tuneOkPacket || tuneOkPacket->method != CONNECTION_TUNE_OK_METHOD_ID) {
        fprintf(stderr, "Erro ao ler o pacote Connection Tune Ok \n");
        return NULL;
    }




        /* read connection open */
    Packet* connectionOpenPacket = read_packet(connfd);
    if (!connectionOpenPacket || connectionOpenPacket->method != CONNECTION_OPEN_METHOD_ID) {
        fprintf(stderr, "Erro ao ler o pacote Connection Open\n");
        return NULL;
    }

    /* write connection open Ok */
    if (connection_open(connfd) < 0) {
        fprintf(stderr, "Erro ao escrever o pacote connection open OK packet\n");
        return NULL;
    }


    /* read channel open */
    Packet* channelOpenPacket = read_packet(connfd);
    if (!channelOpenPacket || channelOpenPacket->method != CHANNEL_OPEN_METHOD_ID) {
        fprintf(stderr, "Erro ao ler o pacote channel open\n");
        
        return NULL;
    }

    /* write channel open OK */
    if (channel_open(connfd) < 0) {
        fprintf(stderr, "Erro ao escrever o pacote channel open OK \n");
        
        return NULL;
    }



        
    Packet* packet = read_packet(connfd);

    if (packet){


        
        /*verifica se o pacote lido é um declare queue*/
        if(packet->method == DECLARE_QUEUE_METHOD_ID && packet->class == QUEUE_CLASS){
            
            
            uint8_t queue_length = (uint8_t) packet->arguments[2];
            char* queue_name = malloc(sizeof(char) * (queue_length + 1));
            memcpy(queue_name, packet->arguments + 3, queue_length);
            queue_name[queue_length] = '\0'; 

            /*cria a scruct com nome da fila*/
            declareQueue(queue_name);

        
            

            /* write declare queue_ok */
            if (declare_queue_ok(connfd, packet) < 0) {
                fprintf(stderr, "Erro ao escrever o pacote declare queue OK \n");
                free(queue_name);
                return NULL;
            }

    

            free(queue_name);



            /* read channel close */
            Packet* channelClose = read_packet(connfd);
            if (!channelClose || channelClose->method != CHANNEL_CLOSE_METHOD_ID || channelClose->class != CHANNEL_CLOSE_CLASS_ID) {
                fprintf(stderr, "Erro ao ler o pacote channel close\n");
                close(connfd);
                return NULL;
            }



            /* write channel close OK */
            if (channel_close(connfd) < 0) {
                fprintf(stderr, "Erro ao escrever o pacote channel close OK\n");
            
                return NULL;
            }


            /* read connection close */
            Packet* connectionClose = read_packet(connfd);
            if (! connectionClose || connectionClose->method != CONNECTION_CLOSE_METHOD_ID) {
                fprintf(stderr, "Erro ao ler o pacote connection close\n");
                return NULL;
            }
            /* write connection close */
            if (connection_close(connfd) < 0) {
            fprintf(stderr, "Erro ao escrever o pacote connection close\n");
            return NULL;
            }

            printf("[Uma conexão fechada]\n");
            close(connfd);

        }

        /*verifica se o pacote lido é um publish*/
        else  if(packet->method == PUBLISH_QUEUE_METHOD_ID && packet->class == PUBLISH_CLASS){
           


            uint8_t queue_length = (uint8_t) packet->arguments[3];   
            char* queue_name =  malloc(sizeof(char)*queue_length);
            memcpy(queue_name, packet->arguments + 4, queue_length);
            queue_name[queue_length] = '\0';  


            Packet* header = read_packet(connfd);



            if (!header || header->class != PUBLISH_CLASS) {
                fprintf(stderr, "Erro ao ler o header do pusblish\n");
                free(queue_name);
            
                return NULL;
            }


            Packet* body = read_packet(connfd);

            if (!body) {
                fprintf(stderr, "Erro ao ler o body do pusblish\n");
                free(queue_name);
                
                return NULL;
            }

            

            /* read channel close */
            Packet* channelClose = read_packet(connfd);
            if (!channelClose || channelClose->method != CHANNEL_CLOSE_METHOD_ID || channelClose->class != CHANNEL_CLOSE_CLASS_ID) {
                fprintf(stderr, "Erro ao ler o pacote channel close\n");
                close(connfd);
                return NULL;
            }

            /* write channel close OK */
            if (channel_close(connfd) < 0) {
                fprintf(stderr, "Erro ao escrever o pacote channel close OK\n");
            
                return NULL;
            }


            /* read connection close */
            Packet* connectionClose = read_packet(connfd);
            if (! connectionClose || connectionClose->method != CONNECTION_CLOSE_METHOD_ID) {
                fprintf(stderr, "Erro ao ler o pacote connection close\n");
                return NULL;
            }
            /* write connection close */
            if (connection_close(connfd) < 0) {
            fprintf(stderr, "Erro ao escrever o pacote connection close\n");
            return NULL;
            }

            
            printf("[Uma conexão fechada]\n");
            close(connfd);


 

            for (int i = 0; i < numQueues; i++) {

                /*achou a scruct com o mesmo nome da fila em que a mensagem vai ser publicada*/
                if (strcmp(queues[i]->queueName, queue_name) == 0) {

                    /*publica as mensagens na lista ligada de mensagens*/
                    publishMessageToQueue(queue_name, body->payload);

                   

                    /* Se houver consumidores esperando, envia a mensagem imediatamente.*/
                    if (queues[i]->consumerQueue->size > 0) {

                        
                        int consumer = peek(queues[i]->consumerQueue); 
                        char* consumedMessage = consumeMessageFromQueue(queue_name);
                       

                       
                        
                        if (consume_ok(consumer) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote consume ok \n");
                            free(queue_name);
                            return NULL;
                        }

                        
                        if (basic_deliver_method(consumer, queue_name) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote do method do basic deliver \n");
                            free(queue_name);
                        
                            return NULL;
                        }

                        if (basic_deliver_header(consumer, consumedMessage) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote do header do basic deliver \n");
                            free(queue_name);
                            return NULL;
                        }

                        if (basic_delivery_body(consumer, consumedMessage) < 0 ) {
                            fprintf(stderr, "Erro ao escrever o pacote do body do basic deliver \n");
                            free(queue_name);
                            return NULL;
                        }


                        /* read Basic Ack  */
                        Packet* basicAck = read_packet(consumer);
                        if ( client_closed_connection || basicAck == 0 || basicAck == NULL){
                            /*se não ler nada, é porque esse cliente já fechou a conexão*/
                            printf("[Uma conexão fechada]\n");
                            return NULL;
                        }
                        else if ( basicAck < 0 || basicAck->method != ACK_METHOD_ID || basicAck->class != CONSUME_CLASS) {
                            fprintf(stderr, "Erro ao ler o pacote Basik Ack\n");
                            return NULL;
                        }
                        
                        
                        
                    }
                    
                    
                    break;
                }
            }

            free(queue_name);

    
        
        }

        /*verifica se o pacote lido é um consume*/
        else if (packet->method == CONSUME_QUEUE_METHOD_ID && packet->class == CONSUME_CLASS) {
           



            uint8_t queue_length = (uint8_t) packet->arguments[2];   
            char* queue_name =  malloc(sizeof(char)*queue_length);
            memcpy(queue_name, packet->arguments + 3, queue_length);
            queue_name[queue_length] = '\0'; 
            
            /*adiciona o consumidor na fila circular*/
            int isConsumerNext = addConsumerToQueue(queue_name, connfd);

            
            char buffer[256];
                  
            /*verifica se o consumidor é o primeiro da fila (ou seja, ele é o único)*/
            if (isConsumerNext) {

                   
                /*looping infinito para consumir as mensagens que estão na fila já que ele é o único consumidor */
                while(1){
                
                    char* consumedMessage = NULL;
                    for (int i = 0; i < numQueues; i++) {
                        if (strcmp(queues[i]->queueName, queue_name) == 0) {
                            consumedMessage = consumeMessageFromQueue(queue_name);
                            break;
                        }
                    }

                    if(consumedMessage){
                        if (consume_ok(connfd) < 0) {
                        fprintf(stderr, "Erro ao escrever o pacote consume ok \n");
                        free(queue_name);
                        return NULL;
                        }

                        if (basic_deliver_method(connfd, queue_name) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote do method do basic deliver \n");
                            free(queue_name);
                        
                            return NULL;
                        }

                        if (basic_deliver_header(connfd, consumedMessage) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote do header do basic deliver \n");
                            free(queue_name);
                            return NULL;
                        }

                        if (basic_delivery_body(connfd, consumedMessage) < 0) {
                            fprintf(stderr, "Erro ao escrever o pacote do body do basic deliver \n");
                            free(queue_name);
                            return NULL;
                        }

                        /* read Basic Ack  */
                        Packet* basicAck = read_packet(connfd);
                        if (client_closed_connection  || basicAck == 0 || basicAck == NULL){
                            return NULL;

                        }
                        else if (basicAck < 0 || basicAck->method != ACK_METHOD_ID || basicAck->class != CONSUME_CLASS ) {
                            fprintf(stderr, "Erro ao ler o pacote Basic ack aqui Ok \n");
                            return NULL;
                        }
                   
                    }
                    /*se o cliente desconectou*/
                    else{
                        
                        break;
                    
                    }

                }

            }

            /*verifica se o servidor ainda está conectado*/
            while(read(connfd, buffer, sizeof(buffer)) != 0 );

            free(queue_name); 

            printf("[Uma conexão fechada]\n");
            close(connfd);

            for (int i = 0; i < numQueues; i++) {
                /*remove os consumidores desconectados da fila*/
                removeConsumerFromQueue(queues[i]->queueName, connfd);
            }

            return NULL;
        }

    }


    return NULL;
}





int main (int argc, char **argv) {


    /* Os sockets. Um que será o socket que vai escutar pelas conexões e o outro que vai ser o socket específico de cada conexão */
    int listenfd, connfd;

    /* Informações sobre o socket (endereço e porta) ficam nesta struct*/
    struct sockaddr_in servaddr;

    /* Retorno da função fork para saber quem é o processo filho e quem é o processo pai*/
    pid_t childpid;

    /* Armazena linhas recebidas do cliente*/
    char recvline[MAXLINE + 1];

    /* Armazena o tamanho da string lida do cliente*/
    ssize_t n;


   
    if (argc != 2) {
        fprintf(stderr,"Uso: %s <Porta>\n",argv[0]);
        fprintf(stderr,"Vai rodar um servidor de echo na porta <Porta> TCP\n");
        exit(1);
    }

    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket :(\n");
        exit(2);
    }


    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port        = htons(atoi(argv[1]));
    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
        perror("bind :(\n");
        exit(3);
    }

    if (listen(listenfd, LISTENQ) == -1) {
        perror("listen :(\n");
        exit(4);
    }

    printf("[Servidor no ar. Aguardando conexões na porta %s]\n",argv[1]);
    printf("[Para finalizar, pressione CTRL+c ou rode um kill ou killall]\n");
   


	for (;;) {

        if ((connfd = accept(listenfd, (struct sockaddr *) NULL, NULL)) == -1) {
                perror("accept :(\n");
                exit(5);
            }

            pthread_t tid; 
            int *fd_ptr = malloc(sizeof(int));
            *fd_ptr = connfd; 
            
            if (pthread_create(&tid, NULL, &handle_client, fd_ptr) != 0) {
                perror("Falha ao criar thread");
                close(connfd);
                free(fd_ptr);
                continue;  
            }

            pthread_detach(tid); 
        }



    exit(0);
}
