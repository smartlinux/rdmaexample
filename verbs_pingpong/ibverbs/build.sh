gcc -O2 -Wall -std=c11 verbs_pingpong_server.c -o pp_server -libverbs
gcc -O2 -Wall -std=c11 verbs_pingpong_client.c -o pp_client -libverbs

