#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
 * cm_pp_client_perf.c
 *
 * RDMA CM (RC) echo client (performance-friendly, correctness-first):
 *
 *   - Uses RDMA CM for address/route resolution + connection setup.
 *   - Uses an RC QP and two-sided SEND/RECV for request/echo.
 *   - Windowed pipeline: up to --window outstanding requests.
 *   - Pre-post RECVs and repost on every RECV completion (receiver credits).
 *   - Optional doorbell batching: chain multiple SEND WRs in one ibv_post_send().
 *   - Works with both:
 *       * INLINE send (fast path) when msg_size <= max_inline_data.
 *       * Non-inline send (fallback) using a per-window send buffer ring.
 *   - Optional CQ completion channel: --event
 *   - Optional latency stats (RTT): --lat
 *
 * NOTE on non-inline:
 *   If SEND is not inline, the NIC DMA-reads the send buffer asynchronously.
 *   Therefore we must not overwrite/reuse the same send buffer while the send is in flight.
 *   This demo uses a ring of send buffers sized = window, and only reuses a slot after the
 *   corresponding echo is received (outstanding <= window guarantees safety).
 */

#define DEFAULT_MSG_SIZE        32
#define DEFAULT_ITERS           200000
#define DEFAULT_WINDOW          256
#define DEFAULT_RECV_DEPTH      512
#define DEFAULT_BATCH           16
#define DEFAULT_CQ_DEPTH        4096
#define POLL_BATCH              32

static void die(const char *what) {
    perror(what);
    exit(1);
}

static void die_gai(const char *what, int rc) {
    fprintf(stderr, "%s: %s\n", what, gai_strerror(rc));
    exit(1);
}

static int parse_int(const char *s, const char *what) {
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (!s[0] || (end && *end) || v < 0 || v > 1000000000L) {
        fprintf(stderr, "Bad %s: '%s'\n", what, s);
        exit(2);
    }
    return (int)v;
}

static uint64_t nsec_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void cq_arm(struct ibv_cq *cq) {
    if (ibv_req_notify_cq(cq, 0)) die("ibv_req_notify_cq");
}

static int cq_poll(struct ibv_cq *cq, struct ibv_wc *wcs, int max_wc) {
    int n = ibv_poll_cq(cq, max_wc, wcs);
    if (n < 0) die("ibv_poll_cq");
    return n;
}

static int cq_wait_and_drain(struct ibv_comp_channel *ch, struct ibv_wc *wcs, int max_wc) {
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;

    if (ibv_get_cq_event(ch, &ev_cq, &ev_ctx)) die("ibv_get_cq_event");
    ibv_ack_cq_events(ev_cq, 1);

    cq_arm(ev_cq);
    return cq_poll(ev_cq, wcs, max_wc);
}

static uint32_t query_max_inline(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init;

    memset(&attr, 0, sizeof(attr));
    memset(&init, 0, sizeof(init));

    if (ibv_query_qp(qp, &attr, IBV_QP_CAP, &init)) die("ibv_query_qp");
    return init.cap.max_inline_data;
}

static void expect_cm_event(struct rdma_event_channel *ec, enum rdma_cm_event_type want) {
    struct rdma_cm_event *ev = NULL;

    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    enum rdma_cm_event_type got = ev->event;

    if (got != want) {
        fprintf(stderr, "Expected cm event %s, got %s (status=%d errno=%d '%s')\n",
                rdma_event_str(want), rdma_event_str(got),
                ev->status, ev->status, strerror(ev->status));
        rdma_ack_cm_event(ev);
        exit(2);
    }

    rdma_ack_cm_event(ev);
}

/*
 * Try to create the QP with a given max_inline_data. Many providers return EINVAL
 * when max_inline_data is too large. We probe to find a supported inline value.
 */
static int try_create_qp(struct rdma_cm_id *id,
                         struct ibv_pd *pd,
                         struct ibv_cq *cq,
                         uint32_t max_send_wr,
                         uint32_t max_recv_wr,
                         uint32_t max_inline_data) {
    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));

    qia.qp_type = IBV_QPT_RC;
    qia.send_cq = cq;
    qia.recv_cq = cq;

    qia.cap.max_send_wr = max_send_wr;
    qia.cap.max_recv_wr = max_recv_wr;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;
    qia.cap.max_inline_data = max_inline_data;

    if (rdma_create_qp(id, pd, &qia) == 0) return 0;
    return -1;
}

/*
 * Create QP with a probed max_inline_data.
 * On success, id->qp is created and *out_max_inline is set.
 */
static void create_qp_with_inline_probe(struct rdma_cm_id *id,
                                        struct ibv_pd *pd,
                                        struct ibv_cq *cq,
                                        uint32_t max_send_wr,
                                        uint32_t max_recv_wr,
                                        size_t msg_size,
                                        uint32_t *out_max_inline) {
    uint32_t hi = (uint32_t)msg_size;
    if (hi > 4096) hi = 4096;

    uint32_t ok = 0;
    uint32_t first_fail = 0;

    uint32_t probe = hi;
    for (;;) {
        if (try_create_qp(id, pd, cq, max_send_wr, max_recv_wr, probe) == 0) {
            ok = probe;
            break;
        }
        if (errno != EINVAL) die("rdma_create_qp");
        first_fail = probe;
        if (probe == 0) die("rdma_create_qp");
        probe /= 2;
    }

    /*
     * If we saw an EINVAL at a higher inline value, binary-search the max supported
     * in (ok, first_fail). We destroy/recreate the QP a few times at startup only.
     */
    if (first_fail > 0 && ok < first_fail - 1) {
        uint32_t lo = ok;
        uint32_t hi2 = first_fail - 1;

        /* Start fresh for probing. */
        rdma_destroy_qp(id);

        while (lo < hi2) {
            uint32_t mid = lo + (hi2 - lo + 1) / 2;

            if (try_create_qp(id, pd, cq, max_send_wr, max_recv_wr, mid) == 0) {
                lo = mid;
                rdma_destroy_qp(id);
            } else {
                if (errno != EINVAL) die("rdma_create_qp");
                hi2 = mid - 1;
            }
        }

        if (try_create_qp(id, pd, cq, max_send_wr, max_recv_wr, lo) != 0) die("rdma_create_qp");
        ok = lo;
    }

    *out_max_inline = query_max_inline(id->qp);
}

/* Post one RECV (wr_id = slot index). */
static void post_one_recv(struct ibv_qp *qp, void *buf, size_t len, struct ibv_mr *mr, uint64_t wr_id) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(qp, &wr, &bad)) die("ibv_post_recv");
}

/*
 * Batch-post cnt SENDs in one ibv_post_send().
 * Each SEND uses a different buffer slot in send_ring (size ring_slots).
 *
 * For correctness in non-inline mode, each outstanding request must use a distinct buffer
 * until its echo is received. The (seq % ring_slots) scheme is safe if outstanding <= ring_slots.
 */
static void post_send_batch(struct ibv_qp *qp,
                            void *send_ring,
                            int ring_slots,
                            size_t msg_size,
                            struct ibv_mr *send_mr,
                            uint32_t max_inline,
                            uint64_t first_seq,
                            int cnt,
                            int signal_every,
                            uint64_t *send_posted,
                            uint64_t *send_ts_or_null) {
    if (cnt <= 0) return;
    if (cnt > 64) cnt = 64;

    struct ibv_send_wr wrs[64];
    struct ibv_sge sges[64];

    const int use_inline = (msg_size <= (size_t)max_inline && max_inline > 0);

    for (int i = 0; i < cnt; i++) {
        uint64_t seq = first_seq + (uint64_t)i;
        int slot = (int)(seq % (uint64_t)ring_slots);

        void *buf = (char *)send_ring + (size_t)slot * msg_size;
        /* Put sequence number in the first 8 bytes; the rest can be anything. */
        memcpy(buf, &seq, sizeof(seq));

        if (send_ts_or_null) send_ts_or_null[seq] = nsec_now();

        memset(&sges[i], 0, sizeof(sges[i]));
        sges[i].addr = (uintptr_t)buf;
        sges[i].length = (uint32_t)msg_size;
        sges[i].lkey = send_mr->lkey;

        memset(&wrs[i], 0, sizeof(wrs[i]));
        wrs[i].wr_id = seq;
        wrs[i].sg_list = &sges[i];
        wrs[i].num_sge = 1;
        wrs[i].opcode = IBV_WR_SEND;
        wrs[i].send_flags = 0;

        if (use_inline) {
            /* Data is copied into the WQE at post time. */
            wrs[i].send_flags |= IBV_SEND_INLINE;
        } else {
            /* Non-inline: NIC will DMA-read the buffer later (must not be overwritten). */
        }

        /*
         * Periodic signaled SENDs:
         *   - Many providers do not generate a CQE for unsignaled SENDs.
         *   - The driver may only reclaim SQ WQEs when it observes a signaled completion,
         *     so a workload of 100% unsignaled SENDs can eventually hit "send queue full".
         *
         * We therefore request a CQE every Nth SEND (N=signal_every). The CQ is shared,
         * and the main loop already polls/drains it, so SEND CQEs will be reaped even if we
         * otherwise ignore IBV_WC_SEND.
         */
        if (send_posted) {
            (*send_posted)++;
            if (signal_every > 0 && ((*send_posted) % (uint64_t)signal_every) == 0) {
                wrs[i].send_flags |= IBV_SEND_SIGNALED;
            }
        }

        wrs[i].next = (i + 1 < cnt) ? &wrs[i + 1] : NULL;
    }

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wrs[0], &bad)) die("ibv_post_send(batch)");
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <server_ip> <port> [--msg N] [--iters N] [--window N]"
                "           [--recv-depth N] [--batch N] [--lat] [--event]"
                "           [--retry N] [--rnr-retry N] [--signal-every N]"
                "           [local_ipv4]",
                argv[0]);
        return 2;
    }

    const char *server_ip = argv[1];
    const char *port = argv[2];

    size_t msg_size = DEFAULT_MSG_SIZE;
    int iters = DEFAULT_ITERS;
    int window = DEFAULT_WINDOW;
    int recv_depth = DEFAULT_RECV_DEPTH;
    int batch = DEFAULT_BATCH;
    int want_lat = 0;
    int use_event = 0;
    int retry = 7;
    int rnr_retry = 7;
    int signal_every = 64; /* every Nth SEND is signaled; 0 disables */
    const char *local_ip = NULL;

    for (int i = 3; i < argc; i++) {
        if (!strcmp(argv[i], "--msg") && i + 1 < argc) {
            msg_size = (size_t)parse_int(argv[++i], "msg_size");
        } else if (!strcmp(argv[i], "--iters") && i + 1 < argc) {
            iters = parse_int(argv[++i], "iters");
        } else if (!strcmp(argv[i], "--window") && i + 1 < argc) {
            window = parse_int(argv[++i], "window");
        } else if (!strcmp(argv[i], "--recv-depth") && i + 1 < argc) {
            recv_depth = parse_int(argv[++i], "recv_depth");
        } else if (!strcmp(argv[i], "--batch") && i + 1 < argc) {
            batch = parse_int(argv[++i], "batch");
        } else if (!strcmp(argv[i], "--lat")) {
            want_lat = 1;
        } else if (!strcmp(argv[i], "--event")) {
            use_event = 1;
        } else if (!strcmp(argv[i], "--retry") && i + 1 < argc) {
            retry = parse_int(argv[++i], "retry");
        } else if (!strcmp(argv[i], "--rnr-retry") && i + 1 < argc) {
            rnr_retry = parse_int(argv[++i], "rnr_retry");
        } else if (!strcmp(argv[i], "--signal-every") && i + 1 < argc) {
            signal_every = parse_int(argv[++i], "signal_every");
        } else {
            /* If it looks like an option but is unknown, fail fast (catches typos). */
            if (argv[i][0] == '-') {
                fprintf(stderr, "Unknown option: %s\n", argv[i]);
                return 2;
            }
            local_ip = argv[i];
        }
    }

    if (msg_size == 0 || msg_size > 1u * 1024u * 1024u) {
        fprintf(stderr, "msg_size must be 1..1048576\n");
        return 2;
    }
    if (iters <= 0 || window <= 0 || recv_depth <= 0) {
        fprintf(stderr, "iters/window/recv_depth must be > 0\n");
        return 2;
    }
    if (batch <= 0) batch = 1;
    if (batch > window) batch = window;

    if (signal_every < 0) {
        fprintf(stderr, "signal_every must be >= 0");
        return 2;
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct rdma_addrinfo hints, *dst = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_family = AF_INET;

    int rc = rdma_getaddrinfo(server_ip, port, &hints, &dst);
    if (rc) die_gai("rdma_getaddrinfo(dst)", rc);

    struct rdma_addrinfo *src_ai = NULL;
    if (local_ip) {
        struct rdma_addrinfo shints, *sres = NULL;
        memset(&shints, 0, sizeof(shints));
        shints.ai_port_space = RDMA_PS_TCP;
        shints.ai_family = AF_INET;

        rc = rdma_getaddrinfo(local_ip, NULL, &shints, &sres);
        if (rc) die_gai("rdma_getaddrinfo(src)", rc);
        src_ai = sres;
    }

    if (rdma_resolve_addr(id, src_ai ? src_ai->ai_src_addr : NULL, dst->ai_dst_addr, 2000))
        die("rdma_resolve_addr");
    expect_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(id, 2000)) die("rdma_resolve_route");
    expect_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    struct ibv_pd *pd = ibv_alloc_pd(id->verbs);
    if (!pd) die("ibv_alloc_pd");

    struct ibv_comp_channel *ch = NULL;
    if (use_event) {
        ch = ibv_create_comp_channel(id->verbs);
        if (!ch) die("ibv_create_comp_channel");
    }

    struct ibv_cq *cq = ibv_create_cq(id->verbs, DEFAULT_CQ_DEPTH, NULL, ch, 0);
    if (!cq) die("ibv_create_cq");

    /* Clamp QP depths to device limits (best-effort). */
    struct ibv_device_attr da;
    memset(&da, 0, sizeof(da));
    if (ibv_query_device(id->verbs, &da)) die("ibv_query_device");

    uint32_t max_wr = (uint32_t)da.max_qp_wr ? (uint32_t)da.max_qp_wr : 16384u;

    uint32_t qp_send_wr = 8192;
    if (qp_send_wr > max_wr) qp_send_wr = max_wr;

    uint32_t qp_recv_wr = (uint32_t)recv_depth;
    if (qp_recv_wr > max_wr) {
        fprintf(stderr, "recv_depth=%d exceeds device max_qp_wr=%u\n", recv_depth, max_wr);
        return 2;
    }

    if ((uint32_t)window > qp_send_wr) {
        fprintf(stderr, "window=%d exceeds max_send_wr=%u; clamping window to %u\n",
                window, qp_send_wr, qp_send_wr);
        window = (int)qp_send_wr;
        if (batch > window) batch = window;
    }

    uint32_t max_inline = 0;
    create_qp_with_inline_probe(id, pd, cq, qp_send_wr, qp_recv_wr, msg_size, &max_inline);

    /* Post initial RECVs (receiver credits). */
    void **rx_bufs = calloc((size_t)recv_depth, sizeof(void *));
    struct ibv_mr **rx_mrs = calloc((size_t)recv_depth, sizeof(struct ibv_mr *));
    if (!rx_bufs || !rx_mrs) die("calloc(rx)");

    for (int i = 0; i < recv_depth; i++) {
        if (posix_memalign(&rx_bufs[i], 64, msg_size)) die("posix_memalign(rx)");
        memset(rx_bufs[i], 0, msg_size);

        rx_mrs[i] = ibv_reg_mr(pd, rx_bufs[i], msg_size, IBV_ACCESS_LOCAL_WRITE);
        if (!rx_mrs[i]) die("ibv_reg_mr(rx)");
        post_one_recv(id->qp, rx_bufs[i], msg_size, rx_mrs[i], (uint64_t)i);
    }

    /* Send buffer ring (size = window slots). */
    void *send_ring = NULL;
    size_t send_ring_bytes = (size_t)window * msg_size;
    if (posix_memalign(&send_ring, 64, send_ring_bytes)) die("posix_memalign(send_ring)");
    memset(send_ring, 0, send_ring_bytes);

    struct ibv_mr *send_mr = ibv_reg_mr(pd, send_ring, send_ring_bytes, 0);
    if (!send_mr) die("ibv_reg_mr(send_ring)");

    if (use_event) cq_arm(cq);

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.initiator_depth = 4;
    cp.responder_resources = 4;
    cp.retry_count = (uint8_t)retry;
    cp.rnr_retry_count = (uint8_t)rnr_retry;

    if (rdma_connect(id, &cp)) die("rdma_connect");
    expect_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED);

    printf("[client] connected QP=%u msg=%zu iters=%d window=%d recv_depth=%d batch=%d "
           "max_inline=%u cq_mode=%s retry=%d rnr_retry=%d signal_every=%d\n",
           id->qp->qp_num, msg_size, iters, window, recv_depth, batch,
           max_inline, use_event ? "event" : "poll", retry, rnr_retry, signal_every);

    uint64_t t0 = nsec_now();

    uint64_t *send_ts = NULL;
    uint64_t *rtt = NULL;
    if (want_lat) {
        send_ts = calloc((size_t)iters + 1, sizeof(uint64_t));
        rtt = calloc((size_t)iters + 1, sizeof(uint64_t));
        if (!send_ts || !rtt) die("calloc(lat)");
    }

    uint64_t sent = 0;
    uint64_t recvd = 0;
    uint64_t next_seq = 0;
    uint64_t send_posted = 0; /* counts posted SEND WQEs (for periodic signaling) */

    /* Prime the pipeline. */
    while ((int)(sent - recvd) < window && sent < (uint64_t)iters) {
        int left = (int)((uint64_t)iters - sent);
        int can = window - (int)(sent - recvd);
        int todo = left < can ? left : can;
        int b = todo < batch ? todo : batch;

        post_send_batch(id->qp, send_ring, window, msg_size, send_mr, max_inline,
                        next_seq, b, signal_every, &send_posted,
                        want_lat ? send_ts : NULL);

        sent += (uint64_t)b;
        next_seq += (uint64_t)b;
    }

    struct ibv_wc wcs[POLL_BATCH];

    while (recvd < (uint64_t)iters) {
        int n = 0;
        if (use_event) {
            n = cq_wait_and_drain(ch, wcs, POLL_BATCH);
        } else {
            n = cq_poll(cq, wcs, POLL_BATCH);
            if (n == 0) continue;
        }

        for (int i = 0; i < n; i++) {
            struct ibv_wc *wc = &wcs[i];
            if (wc->status != IBV_WC_SUCCESS) {
                fprintf(stderr, "WC error: %s (status=%d) opcode=%d wr_id=%" PRIu64 "\n",
                        ibv_wc_status_str(wc->status), wc->status, wc->opcode, wc->wr_id);
                return 1;
            }

            if (wc->opcode == IBV_WC_RECV) {
                /* Echo received -> this seq is complete. */
                uint64_t seq = 0;
                memcpy(&seq, rx_bufs[wc->wr_id], sizeof(seq));

                if (want_lat && seq < (uint64_t)iters) {
                    rtt[seq] = nsec_now() - send_ts[seq];
                }
                recvd++;

                /* Repost this RECV slot immediately (client doesn't SEND from rx buffer). */
                post_one_recv(id->qp, rx_bufs[wc->wr_id], msg_size, rx_mrs[wc->wr_id], wc->wr_id);

                /* Maintain pipeline: keep (sent - recvd) <= window. */
                while ((int)(sent - recvd) < window && sent < (uint64_t)iters) {
                    int left = (int)((uint64_t)iters - sent);
                    int can = window - (int)(sent - recvd);
                    int todo = left < can ? left : can;
                    int b = todo < batch ? todo : batch;

                    post_send_batch(id->qp, send_ring, window, msg_size, send_mr, max_inline,
                                    next_seq, b, signal_every, &send_posted,
                                    want_lat ? send_ts : NULL);

                    sent += (uint64_t)b;
                    next_seq += (uint64_t)b;
                }
            }
        }
    }

    uint64_t t1 = nsec_now();
    double sec = (double)(t1 - t0) / 1e9;
    double ops = (double)iters / sec;

    printf("[client] done: iters=%d time=%.3fs ops=%.2f ops/s\n", iters, sec, ops);

    if (want_lat) {
        /* Compute percentiles over RTT samples. */
        qsort(rtt, (size_t)iters, sizeof(rtt[0]), cmp_u64);

        uint64_t p50 = rtt[(size_t)(iters * 0.50)];
        uint64_t p99 = rtt[(size_t)(iters * 0.99)];
        uint64_t p999 = rtt[(size_t)(iters * 0.999)];

        printf("[client] rtt: samples=%d p50=%.1fus p99=%.1fus p99.9=%.1fus\n",
               iters,
               (double)p50 / 1e3, (double)p99 / 1e3, (double)p999 / 1e3);
    }

    /* Best-effort graceful shutdown. */
    rdma_disconnect(id);
    struct rdma_cm_event *dev = NULL;
    if (rdma_get_cm_event(ec, &dev) == 0) rdma_ack_cm_event(dev);

    if (send_mr) ibv_dereg_mr(send_mr);
    free(send_ring);

    for (int i = 0; i < recv_depth; i++) {
        if (rx_mrs[i]) ibv_dereg_mr(rx_mrs[i]);
        free(rx_bufs[i]);
    }
    free(rx_mrs);
    free(rx_bufs);

    if (id && id->qp) rdma_destroy_qp(id);
    if (cq) ibv_destroy_cq(cq);
    if (ch) ibv_destroy_comp_channel(ch);
    if (pd) ibv_dealloc_pd(pd);

    if (dst) rdma_freeaddrinfo(dst);
    if (src_ai) rdma_freeaddrinfo(src_ai);

    if (id) rdma_destroy_id(id);
    if (ec) rdma_destroy_event_channel(ec);

    free(send_ts);
    free(rtt);

    return 0;
}
