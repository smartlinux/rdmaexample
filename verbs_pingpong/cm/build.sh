gcc -O2 -Wall -std=c11 cm_pingpong_server_inline.c -o cm_pp_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_pingpong_client_inline.c -o cm_pp_client -lrdmacm -libverbs
