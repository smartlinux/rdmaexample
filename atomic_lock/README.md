# RDMA Atomic lock demo (CAS) + one-sided WRITE

This demo shows a common pattern:

1) A shared lock word on the server (remote memory)
2) Clients use RDMA Atomic Compare-and-Swap (CAS) to acquire the lock
3) Under the lock, clients update shared data via RDMA WRITE
4) Clients release the lock

The server does not participate in the data path after sending
`(base_addr, rkey, data_offset)`.

## Files

- `cm_atomic_lock_server.c`: RDMA CM server, accepts multiple clients concurrently
- `cm_atomic_lock_client.c`: client that spins on remote CAS, writes a message, releases

## Build

```bash
gcc -O2 -Wall -std=c11 cm_atomic_lock_server.c -o cm_atomic_lock_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 cm_atomic_lock_client.c -o cm_atomic_lock_client -lrdmacm -libverbs
```

## Run

Server:

```bash
./cm_atomic_lock_server 10002 192.168.157.133
```

Run two clients concurrently (in two terminals):

```bash
./cm_atomic_lock_client 192.168.157.133 10002 "clientA" 200 192.168.157.132
./cm_atomic_lock_client 192.168.157.133 10002 "clientB" 200 192.168.157.132
```

- `message`: string that will be written into shared memory
- `hold_ms`: optional delay while holding the lock (increase contention)
- `local_ip`: optional source IP for RDMA CM route selection

## What to observe

- Only one client acquires the lock at a time.
- The other client spins and then succeeds after the first releases the lock.
- The server prints the shared data snapshot on each disconnect.

## Notes

- Atomics require RC QP and provider support.
- The lock word must be 8-byte aligned.
- For CAS, the old value is returned into the local SGE buffer.
- You can replace the "release via RDMA WRITE" with a CAS(1->0) if you want strict atomic unlock semantics.
