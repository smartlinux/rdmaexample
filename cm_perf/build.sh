#!/usr/bin/env bash
set -euo pipefail

CC=${CC:-gcc}
CFLAGS="-O2 -Wall -Wextra -pedantic"
LIBS="-lrdmacm -libverbs"

$CC $CFLAGS cm_pp_server_perf.c -o cm_pp_server_perf $LIBS
$CC $CFLAGS cm_pp_client_perf.c -o cm_pp_client_perf $LIBS

echo "Built: ./cm_pp_server_perf ./cm_pp_client_perf"
