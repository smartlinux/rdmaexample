# Pure ibverbs RC Send/Recv Ping-Pong (No rdma_cm)

This mini-lab implements the classic RC (Reliable Connected) two-sided ping-pong using **libibverbs only**.
There is **no rdma_cm**. QP connection parameters are exchanged out-of-band over a TCP socket.

## What you learn

- Manual QP state machine: RESET -> INIT -> RTR -> RTS
- RoCE/RXE addressing via GID (GRH) vs InfiniBand LID routing
- Two-sided Send/Recv and CQ polling

## Build

```bash
gcc -O2 -Wall -std=c11 verbs_pingpong_server.c -o pp_server -libverbs
gcc -O2 -Wall -std=c11 verbs_pingpong_client.c -o pp_client -libverbs
```

## Run (RoCE / RXE)

For RXE devices like `rxe_eth0`, use `gid_index=0` (most systems) and `ib_port=1`.

Server:
```bash
ulimit -l unlimited
./pp_server 18515 rxe_eth0 1 0 10000
```

Client:
```bash
ulimit -l unlimited
./pp_client <server_ip> 18515 rxe_eth0 1 0 10000
```

## Run (InfiniBand LID-based)

Pass `gid_index=-1` to use LID-based addressing (no GRH):

Server:
```bash
./pp_server 18515 mlx5_0 1 -1 10000
```

Client:
```bash
./pp_client <server_ip> 18515 mlx5_0 1 -1 10000
```
