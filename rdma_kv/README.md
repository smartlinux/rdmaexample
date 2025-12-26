# RDMA KV Demo (Control-plane SEND/RECV + Data-plane One-sided)

This folder contains a small learning-oriented RDMA key-value store:

- `rdma_kv_server_ev.c`: event-driven server using **rdma_cm**, **SRQ**, and **CQ completion channel**
- `rdma_kv_client.c`: client using **rdma_cm** and one-sided **RDMA_WRITE / RDMA_READ**

The code is intentionally compact, but it covers a *very typical RDMA application pattern*:

- Use **two-sided SEND/RECV** to exchange metadata and commands (the *control plane*).
- Use **one-sided RDMA** (READ/WRITE) for the large payload (the *data plane*).

---

## What you learn from this demo

### RDMA concepts & features exercised

- **rdma_cm connection management**
  - `rdma_create_event_channel`, `rdma_create_id`
  - `rdma_resolve_addr`, `rdma_resolve_route`, `rdma_connect`
  - `rdma_listen`, `rdma_accept`
  - `rdma_get_cm_event`, `rdma_ack_cm_event`

- **QP/CQ/SRQ objects**
  - Create a **RC QP** (`IBV_QPT_RC`) per connection
  - Use a **shared receive queue (SRQ)** so all connections share the same receive buffers
  - Use a **completion channel** (`ibv_create_comp_channel`) and CQ events:
    - `ibv_req_notify_cq`, `ibv_get_cq_event`, `ibv_ack_cq_events`
    - `ibv_poll_cq` to drain completions after an event

- **Memory registration**
  - Server pre-registers a big MR (the **value pool**) with:
    - `IBV_ACCESS_REMOTE_WRITE` and `IBV_ACCESS_REMOTE_READ`
  - Client registers local buffers for RDMA READ/WRITE:
    - `ibv_reg_mr`, `ibv_dereg_mr`

- **Two-sided operations**
  - `rdma_post_recv`
  - `rdma_post_send`

- **One-sided operations**
  - Client issues:
    - `IBV_WR_RDMA_WRITE` for PUT payload
    - `IBV_WR_RDMA_READ` for GET payload
    - `IBV_WR_RDMA_WRITE_WITH_IMM` as a **doorbell commit**

- **Immediate data (WRITE_WITH_IMM)**
  - Server receives `IBV_WC_RECV_RDMA_WITH_IMM` and reads `wc.imm_data`
  - This demo uses imm_data as a 32-bit **token** to match the commit to the pending PUT

---

## Protocol overview

All control messages are fixed-size `struct kv_msg` (256 bytes) and use **network byte order**.

### PUT

1) Client sends `KV_PUT_REQ(key, value_len)` via SEND/RECV  
2) Server allocates a slice from its pre-registered pool and replies `KV_PUT_RESP` with:
   - `addr` (remote address)
   - `rkey`
   - `granted_len` (rounded up to CHUNK_SIZE)
   - `token` (32-bit)
   - `version`
3) Client does `RDMA_WRITE` to fill payload into `(addr, rkey)`  
4) Client does `RDMA_WRITE_WITH_IMM` with `imm_data = token` as a **commit doorbell**  
5) Server receives `RECV_RDMA_WITH_IMM` and commits the KV entry (key -> slice metadata)

**Why WRITE_WITH_IMM is useful here**

On an **RC QP**, operations are ordered. If the server observes the WRITE_WITH_IMM completion,
it implies the preceding RDMA_WRITE has already been applied to memory. This gives you a simple
“payload ready” signal without extra messages.

### GET

1) Client sends `KV_GET_REQ(key)`  
2) Server replies `KV_GET_RESP` with `(addr, rkey, value_len, version)`  
3) Client performs `RDMA_READ` into a local buffer and prints the value

---

## Build

```bash
cd rdma_kv

gcc -O2 -Wall -std=c11 rdma_kv_server_ev.c -o rdma_kv_server_ev -lrdmacm -libverbs
gcc -O2 -Wall -std=c11 rdma_kv_client.c     -o rdma_kv_client     -lrdmacm -libverbs
```

---

## Run

### Server

```bash
./rdma_kv_server_ev 7471
```

### Client

PUT:

```bash
./rdma_kv_client <server_ip> 7471 put mykey "hello rdma"
```

GET:

```bash
./rdma_kv_client <server_ip> 7471 get mykey
```

---

## Notes / limitations

- Key length is limited to 32 bytes.
- This demo allows only **one outstanding PUT per connection** (keeps pending state simple).
- The KV table is a simple open-addressing hash table (no resizing).
- Values are stored in a fixed-size pre-registered pool (chunk allocator).

If you want the next step after this:
- Support multiple outstanding PUTs (token -> pending map)
- Add DEL operation + garbage collection
- Add batching and unsignaled writes (reduce CQ load)
- Add multi-client concurrency stress test
