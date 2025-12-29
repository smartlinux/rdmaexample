# RDMA Atomic demo: global counter allocator (Fetch-and-Add)

This demo uses RDMA CM + RC QP to expose a single 64-bit counter on the server.
Clients allocate unique IDs by issuing **RDMA Atomic Fetch-and-Add**.

## What it demonstrates

- RDMA CM connection setup (ADDR_RESOLVED, ROUTE_RESOLVED, CONNECT_REQUEST, ESTABLISHED)
- RC QP creation via `rdma_create_qp()`
- Control-plane (two-sided): SEND/RECV to exchange `(remote_addr, rkey)`
- Data-plane (one-sided): `IBV_WR_ATOMIC_FETCH_AND_ADD`
- Required MR permission for atomics: `IBV_ACCESS_REMOTE_ATOMIC`

## Build

```bash
gcc -O2 -Wall -std=c11 cm_atomic_counter_server.c -o cm_atomic_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_atomic_counter_client.c -o cm_atomic_client -lrdmacm -libverbs
```

## Run

Server (recommended to bind explicitly):

```bash
./cm_atomic_server 10000 192.168.157.133
```

Client:

```bash
./cm_atomic_client 192.168.157.133 10000 16 1 192.168.157.132
```

Arguments:
- iters: number of atomic ops (default 16)
- add: increment value (default 1)
- local_ip: optional, binds the source IP for RDMA CM route selection

## Notes

- Atomics require RC QP and provider support.
- The remote address must be 8-byte aligned.
- For atomic ops, the "return value" is written into the local SGE buffer.
- In RDMA CM, `initiator_depth` / `responder_resources` should be non-zero to allow READ/ATOMIC.
