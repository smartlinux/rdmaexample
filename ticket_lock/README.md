# RDMA Ticket Lock (Fetch-and-Add)

This demo implements a fair ticket lock using RDMA atomics.

## Key idea

Remote memory contains two 64-bit counters:

- `next_ticket`: allocated by clients via Atomic Fetch-and-Add (FAA)
- `now_serving`: incremented by the lock holder to release

Acquire:
1. `my = FAA(next_ticket, 1)` (returns old value)
2. Spin until `READ(now_serving) == my`

Release:
- `FAA(now_serving, 1)`

## Files

- `cm_ticket_lock_server.c`: RDMA CM server; exposes shared MR; supports multiple clients
- `cm_ticket_lock_client.c`: client; acquires lock, writes message, releases
- `build.sh`: build script

## Build

```bash
./build.sh
```

## Run

Server:

```bash
./cm_ticket_lock_server 10003 192.168.157.133
```

Clients (run two terminals to see contention/fairness):

```bash
./cm_ticket_lock_client 192.168.157.133 10003 clientA 200 192.168.157.132
./cm_ticket_lock_client 192.168.157.133 10003 clientB 200 192.168.157.132
```

`hold_ms` (200) is optional; it holds the lock longer so the other client must wait.
