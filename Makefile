all: amqp

amqp: amqp.c circular_queue.c
	gcc -o amqp amqp.c circular_queue.c -lpthread

clean:
	rm -rf amqp
