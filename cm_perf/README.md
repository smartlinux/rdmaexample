# cm_pingpong performance demo (v4)
A small RDMA CM (librdmacm) “ping-pong” microbenchmark for **RC** QPs.

It is designed for learning and for quickly validating a RoCE v2 / RXE setup.
The client sends a small “request” message; the server echoes it back. The client
measures throughput (ops/s) and optionally RTT percentiles.

This version supports:
- **RDMA CM** address/route resolution, connect/accept, disconnect.
- **CQ polling** (default) or **CQ events** (`--event`).
- **Inline SEND** when the device supports it, otherwise a **registered send ring**
  to safely support message sizes **larger than max_inline_data**.
- A pipelined “window” of in-flight requests.
- **Periodic signaled SENDs** (`--signal-every`) to avoid SQ bookkeeping issues
  with 100% unsignaled workloads on some providers.

## Build
```bash
./build.sh
```

## Run
Server:
```bash
./cm_pp_server_perf <port> [--msg N] [--recv-depth N] [--event] [--signal-every N]
```

Client:
```bash
./cm_pp_client_perf <server_ip> <port> \
  [--msg N] [--iters N] [--window N] [--recv-depth N] [--batch N] [--lat] [--event] \
  [--retry N] [--rnr-retry N] [--signal-every N] [local_ipv4]
```

### Important knobs
- `--msg`: message size in bytes for the ping/pong payload.
- `--iters`: number of round-trips.
- `--window`: maximum in-flight requests (pipeline depth).
- `--recv-depth`: how many RECV WQEs are kept posted on the receive queue.
- `--batch`: how many SENDs to post at once when filling the pipeline.
- `--lat`: collect RTT samples and print p50/p99/p99.9 (slower).
- `--event`: use CQ events (`ibv_get_cq_event` + `ibv_req_notify_cq`) instead of polling.

### Periodic signaled SENDs: `--signal-every`
Many providers do **not** generate a CQE for unsignaled SENDs. The user-space driver
may only reclaim SQ WQEs after it observes a signaled completion, so a workload with
**100% unsignaled SENDs** can eventually hit “send queue full” (or stall), especially
when posting at high rate.

To avoid that, the benchmark supports:
- `--signal-every 64` (default): every 64th SEND has `IBV_SEND_SIGNALED`.
- `--signal-every 1`: every SEND is signaled (slow, but simplest).
- `--signal-every 0`: disable signaling (useful to reproduce SQ starvation on some systems).

The CQ is shared and the main loop already polls/drains it, so `IBV_WC_SEND` CQEs will
be reaped even if the benchmark focuses on RECV completions for correctness/latency.

## Notes
- If you see timeouts / `RDMA_CM_EVENT_UNREACHABLE`, verify the **GID index** and that
  UDP/4791 (RoCE v2) is not blocked.
- For RXE, using the IPv4-mapped GID (often something like `::ffff:192.168.x.y`) is
  typically the easiest.

