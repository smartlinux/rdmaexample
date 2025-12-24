gcc -O2 -Wall -D_GNU_SOURCE -std=c11 rdma_pool_server.c -o rdma_pool_server -lrdmacm -libverbs
gcc -O2 -Wall -D_GNU_SOURCE -std=c11 rdma_pool_client.c -o rdma_pool_client -lrdmacm -libverbs
gcc -O2 -Wall -D_GNU_SOURCE -std=c11  rdma_srq_pool_server_ev.c -o rdma_srq_pool_server_ev  -lrdmacm -libverbs
