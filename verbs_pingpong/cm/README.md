# RDMA CM inline ping-pong (v2)

This v2 adds two practical improvements over v1:

1) Better diagnostics:
   - On unexpected CM events, prints `ev->status` (negative errno) and strerror.
   - This helps when you see `RDMA_CM_EVENT_ADDR_ERROR`.

2) More robust `rdma_resolve_addr` usage:
   - Calls `rdma_resolve_addr(id, res->ai_src_addr, res->ai_dst_addr, ...)`
     which matches common rdma-core examples.
   - Optional `local_ip` lets you force which NIC/address the cm_id binds to.

## Build

```bash
gcc -O2 -Wall -std=c11 cm_pingpong_server_inline_v2.c -o cm_pp_server_v2 -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_pingpong_client_inline_v2.c -o cm_pp_client_v2 -lrdmacm -libverbs
```

## Run

Server:

```bash
./cm_pp_server_v2 10000 1000
```

Client (simple):

```bash
./cm_pp_client 192.168.157.133 10000 1000 1
```

Client (force local IP / NIC):

```bash
./cm_pp_client 192.168.157.133 10000 1000 1 192.168.157.132
```

If you get `RDMA_CM_EVENT_ADDR_ERROR`, the printed `errno` is usually one of:
- `ENOENT` / `EADDRNOTAVAIL`: the IP route maps to a netdev that has no RDMA (RXE) device.
- `ENETUNREACH`: IP route is broken.
