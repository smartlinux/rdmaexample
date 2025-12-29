#!/usr/bin/env bash
set -euo pipefail

gcc -O2 -Wall -std=c11 cm_ticket_lock_server.c -o cm_ticket_lock_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_ticket_lock_client.c -o cm_ticket_lock_client -lrdmacm -libverbs
