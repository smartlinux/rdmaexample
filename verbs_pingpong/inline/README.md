# Inline + Wait-RECV-Only Ping-Pong (Pure ibverbs, RC)

This variant is intentionally closer to the "RPC header" style:

- Small control messages (PING/PONG) are sent via **two-sided SEND/RECV**.
- The SEND is posted with **IBV_SEND_INLINE**:
  - The sender does **not** need to register the SEND buffer.
  - The sender can safely reuse/overwrite the SEND buffer immediately after `ibv_post_send()` returns.
- The program does **not** request SEND completions (no `IBV_SEND_SIGNALED`).
  - The CQ contains only **RECV** completions, so the wait loop is simpler and avoids "CQE order" pitfalls.

## Files
- `verbs_pingpong_server_inline.c`
- `verbs_pingpong_client_inline.c`

## Build
```bash
gcc -O2 -Wall -std=c11 verbs_pingpong_server_inline.c -o pp_server_inline -libverbs
gcc -O2 -Wall -std=c11 verbs_pingpong_client_inline.c -o pp_client_inline -libverbs
```

## Run (RXE/RoCE)
Pick a `gid_index` that matches your IPv4-mapped GID, e.g. `::ffff:192.168.x.y`.

Server:
```bash
./pp_server 10000 rxe_eth0 1 2 1000
```

Client:
```bash
./pp_client <server_ip> 10000 rxe_eth0 1 2 1000 1
```

## Notes
- We request `max_inline_data = MSG_SIZE` at QP creation and validate it via `ibv_query_qp`.
- In real systems, headers are often smaller (e.g. 32~128 bytes), and large payloads move via RDMA READ/WRITE.
