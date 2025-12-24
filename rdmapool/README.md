# RDMA Pool + Doorbell Demo (RDMA CM + ibverbs)

This mini-demo implements a **pool-based remote memory grant** plus a **doorbell** mechanism using
**RDMA_WRITE_WITH_IMM**.

It is intentionally small and educational, focusing on the most common RDMA concepts:
- RDMA CM connection management
- RC QP creation
- Memory registration (MR), rkey, remote_addr
- Control-plane messaging via SEND/RECV
- Data-plane one-sided writes via RDMA WRITE
- Write-with-immediate as a doorbell / notification
- CQ polling vs event-driven CQ handling
- Shared Receive Queue (SRQ) and reposting receives to avoid RNR

---

## Files

### 1) `rdma_pool_client.c`
**Client**:

1. Connects to the server (RDMA CM).
2. Sends `REQ_ALLOC` (SEND) to request a remote memory slice of `alloc_size_bytes`.
3. Receives `RESP_ALLOC` (RECV), which contains:
   - `addr` (remote virtual address inside the server’s pre-registered MR)
   - `rkey` (remote key for access)
   - `size` (granted bytes)
4. Issues a stream of one-sided writes:
   - Most writes are **unsignaled RDMA WRITE** (no CQ entry on the sender).
   - Every `DOORBELL_EVERY` writes, it posts a **signaled RDMA_WRITE_WITH_IMM**
     and waits for the completion. The immediate value is used as a doorbell sequence number.

What you learn from this file:
- `rdma_resolve_addr`, `rdma_resolve_route`, `rdma_connect`
- `rdma_create_qp` (RC QP)
- `ibv_reg_mr` (register buffers)
- `IBV_WR_SEND` control message
- `IBV_WR_RDMA_WRITE`, `IBV_WR_RDMA_WRITE_WITH_IMM`
- `IBV_SEND_SIGNALED` vs unsignaled
- `ibv_poll_cq` to wait for completions

Usage:
```bash
./rdma_pool_client <server_ip> <alloc_size_bytes>
```

---

### 2) `rdma_pool_server.c`
**Server (polling / tick loop)**:

- Listens on `0.0.0.0:7471` via RDMA CM.
- On the first connection, initializes global resources:
  - PD, CQ
  - SRQ + pre-posted receive buffers
  - A large pre-registered pool MR (4MiB by default)
- For each connection:
  - Creates a QP attached to the shared SRQ and CQ
  - Accepts the connection
- When a control-plane request arrives (`REQ_ALLOC`):
  - Allocates a contiguous chunk range from the pool bitmap
  - Replies with `RESP_ALLOC { addr, rkey, size }`
- When a doorbell arrives (`RECV_RDMA_WITH_IMM`):
  - Reads the corresponding 32-bit value from the remote slice memory
  - Prints it

This variant drains CQ by calling `ibv_poll_cq()` periodically (simple, but not fully event-driven).

What you learn from this file:
- `rdma_bind_addr`, `rdma_listen`, `rdma_accept`
- SRQ creation: `ibv_create_srq` + `ibv_post_srq_recv`
- Pool MR registration with remote-access flags:
  `IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ`
- CQ polling loop and reposting SRQ receives
- Handling `IBV_WC_RECV` vs `IBV_WC_RECV_RDMA_WITH_IMM`

---

### 3) `rdma_srq_pool_server_ev.c`
**Server (event-driven CQ)**:

Same core functionality as `rdma_pool_server.c`, but uses:
- `ibv_create_comp_channel` to get a **completion channel**
- `ibv_req_notify_cq` to **arm** CQ notifications
- `ibv_get_cq_event` to dequeue CQ events from the channel
- `ibv_ack_cq_events` to acknowledge (avoid CQ event queue overflow)

It blocks in `poll(-1)` on:
- RDMA CM event fd (connect/disconnect)
- CQ channel fd (completions)

It also includes a **send buffer pool** for control-plane responses, and frees buffers
on `IBV_WC_SEND` completions using `wc.wr_id`.

What you learn from this file:
- The standard CQ event-driven pattern:
  1) `ibv_get_cq_event()`
  2) `ibv_ack_cq_events()`
  3) `ibv_req_notify_cq()` (rearm)
  4) `ibv_poll_cq()` drain until empty
- Why you must fully drain CQ (and repost SRQ receives) to keep progress
- How to tag WRs via `wr_id` and free resources on completion

---

## Control-plane message format

The control message is `struct ctrl_msg` (application-defined). This is sent over the
**out-of-band control path** created by RDMA CM (RC QP SEND/RECV):

- `REQ_ALLOC`: client -> server (type=1, size=request bytes)
- `RESP_ALLOC`: server -> client (type=2, status + {addr, rkey, size})

The *format is entirely decided by the application*. You must handle endianness
(network byte order) because you are defining your own protocol.

---

## Build

Install dependencies (Ubuntu example):
```bash
sudo apt-get update
sudo apt-get install -y rdma-core librdmacm-dev libibverbs-dev build-essential
```

Build:
```bash
gcc -O2 -Wall -std=c11 rdma_pool_server.c -o rdma_pool_server -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 rdma_srq_pool_server_ev.c -o rdma_srq_pool_server_ev -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 rdma_pool_client.c -o rdma_pool_client -lrdmacm -libverbs
```

---

## Run

Terminal 1 (server):
```bash
./rdma_srq_pool_server_ev
# or:
./rdma_pool_server
```

Terminal 2 (client):
```bash
./rdma_pool_client <server_ip> 10000
```

Expected behavior:
- Server prints that it granted a slice (`addr`, `rkey`, `size`).
- Client prints doorbell completions.
- Server prints doorbell notifications and reads values from the remote slice.

---

## Notes / Troubleshooting

1) **You must have an RDMA device** (hardware RNIC or Soft-RoCE).
   - If using Soft-RoCE, you need to configure it on both sides.

2) **memlock / pinned memory**
   - `ibv_reg_mr()` pins pages. If registration fails, check:
     - `ulimit -l` (memlock)
     - `/etc/security/limits.conf` or systemd limits

3) **RNR (Receiver Not Ready)**
   - This demo uses SRQ + pre-posted receives to avoid RNR for SEND/RECV.
   - If you stop draining CQ / stop reposting SRQ receives, progress will stall.

4) **Why align to 4KiB?**
   - Registration pins memory in pages. Page alignment is a convenient default.
   - Other alignments can work; 4KiB just makes reasoning easier and often reduces surprises.

---

## RDMA APIs highlighted

RDMA CM:
- `rdma_create_event_channel`, `rdma_create_id`
- `rdma_bind_addr`, `rdma_listen`
- `rdma_get_cm_event`, `rdma_ack_cm_event`
- `rdma_accept`, `rdma_connect`, `rdma_disconnect`
- `rdma_create_qp`, `rdma_destroy_qp`

ibverbs:
- `ibv_alloc_pd`, `ibv_dealloc_pd`
- `ibv_create_cq`, `ibv_destroy_cq`
- `ibv_create_srq`, `ibv_post_srq_recv`
- `ibv_reg_mr`, `ibv_dereg_mr`
- `ibv_post_send`, `ibv_post_recv`
- `ibv_poll_cq`
- (event-driven server) `ibv_create_comp_channel`, `ibv_req_notify_cq`,
  `ibv_get_cq_event`, `ibv_ack_cq_events`

