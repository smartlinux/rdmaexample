gcc -O2 -Wall -D_GNU_SOURCE -std=c11 rdma_kv_server_ev.c -o rdma_kv_server_ev -lrdmacm -libverbs
gcc -O2 -Wall -D_GNU_SOURCE -std=c11 rdma_kv_client.c -o rdma_kv_client -lrdmacm -libverbs

