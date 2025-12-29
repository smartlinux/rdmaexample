gcc -O2 -Wall -std=c11 verbs_pingpong_server_inline.c -o pp_server -libverbs
gcc -O2 -Wall -std=c11 verbs_pingpong_client_inline.c -o pp_client -libverbs

