gcc -O2 -Wall -std=c11 cm_atomic_counter_server.c -o cm_atomic_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_atomic_counter_client.c -o cm_atomic_client -lrdmacm -libverbs
