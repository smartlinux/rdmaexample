gcc -O2 -Wall rdma_write_client.c -o rdma_write_client -lrdmacm -libverbs
gcc -O2 -Wall rdma_write_server.c -o rdma_write_server -lrdmacm -libverbs
