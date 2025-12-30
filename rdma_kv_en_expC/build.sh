gcc -O2 -Wall rdma_kv_server_ev_expC.c -o kv_server_expC -lrdmacm -libverbs
gcc -O2 -Wall rdma_kv_client_expC.c    -o kv_client_expC -lrdmacm -libverbs