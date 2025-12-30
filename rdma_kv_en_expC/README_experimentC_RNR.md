# Experiment C: Receiver resources (SRQ RECV WQEs) and RNR behavior

This variant adds **two small knobs** to the RDMA KV (event-driven) program so you can *intentionally* reproduce
and then fix the classic "hang due to no posted RECV" problem.

## What this experiment demonstrates

On RC QPs, **SEND / SEND_WITH_IMM / WRITE_WITH_IMM** require the receiver to have a posted **RECV WQE**
(Receive Work Request). If the receiver has no RECV WQE available, it returns an RNR NAK (**Receiver Not Ready**).
The sender will retry (based on `rnr_retry_count`). With a large/infinite retry setting, your program can look
"stuck" even though no user-space bug exists — the NIC is retrying.

In this server we use an **SRQ** (Shared Receive Queue) for control-plane messages. Each incoming SEND consumes one
SRQ RECV WQE. **If you do not replenish RECVs**, the SRQ eventually becomes empty and senders will hit RNR.

---

## Files

- `rdma_kv_server_ev_expC.c`
  - Adds:
    - `--recv-depth N` : number of SRQ RECV WQEs to pre-post
    - `--no-repost`    : disable reposting on each RECV completion (**to reproduce RNR**)

- `rdma_kv_client_expC.c`
  - Adds:
    - `bench_get <key> <iters>` : repeatedly issues GET requests (control-plane stress)
    - `--rnr-retry N`           : set sender-side RNR retry count (0..7)
    - `--retry N`               : set sender-side retry count (0..7)
    - `--progress M`            : progress print frequency for `bench_get`

---

## Build

```bash
gcc -O2 -Wall rdma_kv_server_ev_expC.c -o kv_server_expC -lrdmacm -libverbs
gcc -O2 -Wall rdma_kv_client_expC.c    -o kv_client_expC -lrdmacm -libverbs
```

---

## Run: normal (everything is healthy)

Terminal A (server):

```bash
./kv_server_expC 7471
# default: recv_depth=2048, repost enabled
```

Terminal B (client):

```bash
./kv_client_expC <server_ip> 7471 bench_get aa 1000
```

You should see progress prints and completion.

---

## Reproduce RNR / hang (SRQ runs out of RECV WQEs)

Terminal A (server) — *only 1 RECV posted and NO reposting*:

```bash
./kv_server_expC 7471 --recv-depth 1 --no-repost
```

Terminal B (client):

### Case 1: "hang" style (large retry counts)
```bash
./kv_client_expC <server_ip> 7471 bench_get aa 1000
```
Often you will see the first iteration succeed, then the next request will appear to hang
(because the sender keeps retrying due to RNR).

### Case 2: "fail fast" style (make the RNR visible in user space)
Set `--rnr-retry 0` so the sender gives up and you get a WC error:

```bash
./kv_client_expC <server_ip> 7471 bench_get aa 1000 --rnr-retry 0
```

You should see something like:
- `WC error: RNR retry exceeded ...`
- plus the hint printed by the client.

---

## Fix it

Re-enable reposting and/or increase recv depth:

```bash
./kv_server_expC 7471 --recv-depth 64
# repost enabled by default
```

Now `bench_get` should complete even with large iteration counts.

---

## What to observe

1. **Why draining CQ matters**
   - If you stop polling the CQ (or don't drain enough), you stop reposting RECVs and SRQ empties.

2. **RNR is a sender-side symptom**
   - The receiver just "didn't have a RECV"; the *sender* sees RNR retries and possibly a WC error.

3. **Receive WQE == resource**
   - Think of SRQ depth as your "control-plane credit".
   - Production code typically uses a watermark strategy (replenish in batches) rather than reposting 1-by-1.
