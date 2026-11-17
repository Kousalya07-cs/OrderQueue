/*
 * Online Order Processing Queue - Producer-Consumer Problem
 *
 * Name            : KOUSALYA S
 * Register Number : 814725104079
 * Department      : Computer Science and Engineering
 *
 * Producers  = customers placing orders on the website
 * Consumers  = workers who process the orders
 * Buffer     = bounded order queue (circular queue)
 *
 * Synchronization:
 *   empty - counting semaphore: free slots in the queue
 *   full  - counting semaphore: orders waiting in the queue
 *   mutex - protects the queue (critical section)
 *
 * Compile : gcc order_queue.c -o order_queue -pthread
 * Run     : ./order_queue
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define QUEUE_SIZE          5   /* maximum orders waiting in the queue */
#define NUM_PRODUCERS       2   /* customers placing orders            */
#define NUM_CONSUMERS       2   /* workers processing orders           */
#define ORDERS_PER_PRODUCER 5   /* orders placed by each customer      */

#define TOTAL_ORDERS        (NUM_PRODUCERS * ORDERS_PER_PRODUCER)
#define ORDERS_PER_CONSUMER (TOTAL_ORDERS / NUM_CONSUMERS)

typedef struct {
    int    order_id;
    char   item[30];
    double amount;
} Order;

/* Shared circular queue */
Order queue[QUEUE_SIZE];
int   in = 0, out = 0, count = 0;

sem_t empty_slots;      /* counts free slots      */
sem_t full_slots;       /* counts filled slots    */
pthread_mutex_t mutex;  /* mutual exclusion       */

int next_order_id = 1;  /* protected by id_mutex  */
pthread_mutex_t id_mutex;

const char *items[] = {"Laptop", "Mobile", "Headphones", "Keyboard",
                       "Mouse", "Monitor", "Smart Watch", "Tablet"};
const double prices[] = {55000.0, 18000.0, 2500.0, 1200.0,
                         600.0, 9500.0, 4500.0, 22000.0};

void *producer(void *arg)
{
    int id = *(int *)arg;

    for (int i = 0; i < ORDERS_PER_PRODUCER; i++) {
        Order o;
        int k = rand() % 8;

        pthread_mutex_lock(&id_mutex);
        o.order_id = next_order_id++;
        pthread_mutex_unlock(&id_mutex);

        strcpy(o.item, items[k]);
        o.amount = prices[k];

        sem_wait(&empty_slots);          /* wait for a free slot   */
        pthread_mutex_lock(&mutex);      /* enter critical section */

        queue[in] = o;
        in = (in + 1) % QUEUE_SIZE;
        count++;
        printf("[Customer %d] Placed Order #%d (%s, Rs.%.2f) | Queue size: %d\n",
               id, o.order_id, o.item, o.amount, count);

        pthread_mutex_unlock(&mutex);    /* leave critical section */
        sem_post(&full_slots);           /* one more order waiting */

        usleep((rand() % 5 + 1) * 100000);
    }
    return NULL;
}

void *consumer(void *arg)
{
    int id = *(int *)arg;

    for (int i = 0; i < ORDERS_PER_CONSUMER; i++) {
        sem_wait(&full_slots);           /* wait for an order      */
        pthread_mutex_lock(&mutex);      /* enter critical section */

        Order o = queue[out];
        out = (out + 1) % QUEUE_SIZE;
        count--;
        printf("    [Worker %d] Picked Order #%d (%s) | Queue size: %d\n",
               id, o.order_id, o.item, count);

        pthread_mutex_unlock(&mutex);    /* leave critical section */
        sem_post(&empty_slots);          /* one more free slot     */

        usleep((rand() % 8 + 2) * 100000);   /* processing time */
        printf("    [Worker %d] Completed Order #%d\n", id, o.order_id);
    }
    return NULL;
}

int main(void)
{
    pthread_t prod[NUM_PRODUCERS], cons[NUM_CONSUMERS];
    int pid[NUM_PRODUCERS], cid[NUM_CONSUMERS];

    printf("==================================================\n");
    printf(" Online Order Processing Queue\n");
    printf(" Producer-Consumer Problem\n");
    printf(" Name            : KOUSALYA S\n");
    printf(" Register Number : 814725104079\n");
    printf("==================================================\n\n");

    sem_init(&empty_slots, 0, QUEUE_SIZE);
    sem_init(&full_slots, 0, 0);
    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&id_mutex, NULL);
    srand(42);

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pid[i] = i + 1;
        pthread_create(&prod[i], NULL, producer, &pid[i]);
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        cid[i] = i + 1;
        pthread_create(&cons[i], NULL, consumer, &cid[i]);
    }

    for (int i = 0; i < NUM_PRODUCERS; i++) pthread_join(prod[i], NULL);
    for (int i = 0; i < NUM_CONSUMERS; i++) pthread_join(cons[i], NULL);

    printf("\nAll %d orders were processed successfully.\n", TOTAL_ORDERS);

    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&id_mutex);
    return 0;
}
