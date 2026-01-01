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
#include <unistd.h>
#include <poll.h>

/*
 * cm_pp_server_perf.c
 *
 * RDMA CM (RC) echo server:
 *
 *   - rdma_listen() on <port>
 *   - On CONNECT_REQUEST:
 *       * allocate PD, CQ (+ optional completion channel)
 *       * create RC QP with probed max_inline_data
 *       * allocate and pre-post RECV buffers (receiver credits)
 *       * rdma_accept()
 *   - After ESTABLISHED:
 *       * CQ loop:
 *           - On RECV completion: immediately SEND the same payload back (echo)
 *           - Repost the RECV buffer to keep receiver credits
 *
 * IMPORTANT correctness detail:
 *   If SEND is not inline, the NIC DMA-reads the send buffer asynchronously.
 *   This demo uses the RECV buffer as the SEND payload (zero-copy echo).
 *   Therefore, in non-inline mode we MUST NOT repost the same RECV buffer
 *   until the SEND is completed; otherwise the NIC might overwrite that buffer
 *   with a new incoming message before the outgoing SEND has consumed it.
 *
 *   Implementation:
 *     - Inline path: SEND is inline, so it is safe to repost RECV immediately.
 *     - Non-inline path: Each echo SEND is signaled; on SEND completion we repost
 *       the corresponding RECV buffer.
 */

#define DEFAULT_PORT            "10000"
#define DEFAULT_MSG_SIZE        32
#define DEFAULT_RECV_DEPTH      512
#define DEFAULT_CQ_DEPTH        4096
#define POLL_BATCH              32

static void die(const char *what) {
    perror(what);
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

static void expect_cm_event(struct rdma_event_channel *ec, enum rdma_cm_event_type want,
                            struct rdma_cm_event **out_ev) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");

    if (ev->event != want) {
        fprintf(stderr, "Expected cm event %s, got %s (status=%d)\n",
                rdma_event_str(want), rdma_event_str(ev->event), ev->status);
        rdma_ack_cm_event(ev);
        exit(2);
    }
    *out_ev = ev; /* caller must ack */
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

    if (first_fail > 0 && ok < first_fail - 1) {
        uint32_t lo = ok;
        uint32_t hi2 = first_fail - 1;

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

struct rx_slot {
    void *buf;
    struct ibv_mr *mr;
};

struct conn_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_comp_channel *ch;
    struct ibv_cq *cq;

    size_t msg_size;
    int recv_depth;

    uint32_t max_inline;
    int use_event;

    int signal_every;
    uint64_t tx_posted;

    struct rx_slot *rx;
    uint8_t *rx_wait_send_cq; /* 1 if a non-inline SEND is in-flight using this rx slot */
};

static void post_one_recv(struct conn_ctx *c, int idx) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)c->rx[idx].buf;
    sge.length = (uint32_t)c->msg_size;
    sge.lkey = c->rx[idx].mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(c->id->qp, &wr, &bad)) die("ibv_post_recv");
}

/*
 * Post an echo SEND using rx[idx] buffer as payload.
 * - Inline path: SEND_INLINE, unsignaled; repost RECV immediately is safe.
 * - Non-inline: signaled; repost RECV on SEND completion.
 *
 * wr_id encoding:
 *   - RECV wr_id: idx (low bits)
 *   - SEND wr_id: (1ULL<<63) | idx
 */
static void post_echo_send(struct conn_ctx *c, int idx) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)c->rx[idx].buf;
    sge.length = (uint32_t)c->msg_size;
    sge.lkey = c->rx[idx].mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (1ULL << 63) | (uint64_t)(uint32_t)idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;

    if (c->msg_size <= (size_t)c->max_inline && c->max_inline > 0) {
        /* Inline path: safe to repost RECV immediately. */
        wr.send_flags = IBV_SEND_INLINE;
        c->rx_wait_send_cq[idx] = 0;

        /* Periodic signaled inline SENDs to let the driver reclaim SQ WQEs. */
        c->tx_posted++;
        if (c->signal_every > 0 && (c->tx_posted % (uint64_t)c->signal_every) == 0) {
            wr.send_flags |= IBV_SEND_SIGNALED;
        }
    } else {
        /*
         * Non-inline: must wait for SEND completion before reposting this buffer.
         * We request a completion for each echo in this mode.
         */
        /* Non-inline: must wait for SEND completion before reposting this buffer. */
        wr.send_flags = IBV_SEND_SIGNALED;
        c->rx_wait_send_cq[idx] = 1;
        c->tx_posted++;
    }

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->id->qp, &wr, &bad)) die("ibv_post_send(echo)");
}

static void conn_cleanup(struct conn_ctx *c) {
    if (!c) return;

    if (c->id) {
        if (c->id->qp) rdma_destroy_qp(c->id);
        rdma_destroy_id(c->id);
    }

    if (c->rx) {
        for (int i = 0; i < c->recv_depth; i++) {
            if (c->rx[i].mr) ibv_dereg_mr(c->rx[i].mr);
            free(c->rx[i].buf);
        }
        free(c->rx);
    }
    free(c->rx_wait_send_cq);

    if (c->cq) ibv_destroy_cq(c->cq);
    if (c->ch) ibv_destroy_comp_channel(c->ch);
    if (c->pd) ibv_dealloc_pd(c->pd);
}

static void serve_one_connection(struct rdma_event_channel *ec,
                                 struct rdma_cm_id *child,
                                 size_t msg_size,
                                 int recv_depth,
                                 int use_event,
                                 int signal_every) {
    struct conn_ctx c;
    memset(&c, 0, sizeof(c));

    c.id = child;
    c.msg_size = msg_size;
    c.recv_depth = recv_depth;
    c.use_event = use_event;
    c.signal_every = signal_every;
    c.tx_posted = 0;

    c.pd = ibv_alloc_pd(child->verbs);
    if (!c.pd) die("ibv_alloc_pd");

    if (use_event) {
        c.ch = ibv_create_comp_channel(child->verbs);
        if (!c.ch) die("ibv_create_comp_channel");
    }

    c.cq = ibv_create_cq(child->verbs, DEFAULT_CQ_DEPTH, NULL, c.ch, 0);
    if (!c.cq) die("ibv_create_cq");

    /* Clamp QP depths to device limits. */
    struct ibv_device_attr da;
    memset(&da, 0, sizeof(da));
    if (ibv_query_device(child->verbs, &da)) die("ibv_query_device");

    uint32_t max_wr = (uint32_t)da.max_qp_wr ? (uint32_t)da.max_qp_wr : 16384u;
    uint32_t qp_recv_wr = (uint32_t)recv_depth;
    if (qp_recv_wr > max_wr) {
        fprintf(stderr, "recv_depth=%d exceeds device max_qp_wr=%u\n", recv_depth, max_wr);
        exit(2);
    }

    uint32_t qp_send_wr = 4096;
    if (qp_send_wr > max_wr) qp_send_wr = max_wr;
    if (qp_send_wr < 2) qp_send_wr = 2;

    create_qp_with_inline_probe(child, c.pd, c.cq, qp_send_wr, qp_recv_wr, msg_size, &c.max_inline);

    c.rx = calloc((size_t)recv_depth, sizeof(*c.rx));
    c.rx_wait_send_cq = calloc((size_t)recv_depth, sizeof(uint8_t));
    if (!c.rx || !c.rx_wait_send_cq) die("calloc(rx)");

    for (int i = 0; i < recv_depth; i++) {
        if (posix_memalign(&c.rx[i].buf, 64, msg_size)) die("posix_memalign(rx)");
        memset(c.rx[i].buf, 0, msg_size);

        c.rx[i].mr = ibv_reg_mr(c.pd, c.rx[i].buf, msg_size, IBV_ACCESS_LOCAL_WRITE);
        if (!c.rx[i].mr) die("ibv_reg_mr(rx)");

        post_one_recv(&c, i);
    }

    if (use_event) cq_arm(c.cq);

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.initiator_depth = 4;
    cp.responder_resources = 4;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;

    if (rdma_accept(child, &cp)) die("rdma_accept");

    struct rdma_cm_event *ev = NULL;
    expect_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &ev);
    rdma_ack_cm_event(ev);

    printf("[server] connected QP=%u msg=%zu recv_depth=%d max_inline=%u cq_mode=%s signal_every=%d\n",
           child->qp->qp_num, msg_size, recv_depth, c.max_inline,
           use_event ? "event" : "poll", signal_every);

    /* CQ loop: echo back every message until disconnect. */
    struct ibv_wc wcs[POLL_BATCH];

    for (;;) {
        int n = 0;

        if (use_event) {
            /* Wait for either a CQ completion event or a CM event. */
            struct pollfd fds[2];
            memset(fds, 0, sizeof(fds));

            fds[0].fd = c.ch->fd;
            fds[0].events = POLLIN;

            fds[1].fd = ec->fd;
            fds[1].events = POLLIN;

            int prc = poll(fds, 2, -1);
            if (prc < 0) die("poll");

            if (fds[1].revents & POLLIN) {
                struct rdma_cm_event *cev = NULL;
                if (rdma_get_cm_event(ec, &cev)) die("rdma_get_cm_event");
                if (cev->event == RDMA_CM_EVENT_DISCONNECTED) {
                    rdma_ack_cm_event(cev);
                    break;
                }
                rdma_ack_cm_event(cev);
            }

            if (fds[0].revents & POLLIN) {
                n = cq_wait_and_drain(c.ch, wcs, POLL_BATCH);
            } else {
                continue;
            }
        } else {
            n = cq_poll(c.cq, wcs, POLL_BATCH);
            if (n == 0) {
                /* Non-blocking check for CM events (disconnect). */
                struct pollfd fd;
                memset(&fd, 0, sizeof(fd));
                fd.fd = ec->fd;
                fd.events = POLLIN;

                int prc = poll(&fd, 1, 0);
                if (prc < 0) die("poll");
                if (prc > 0 && (fd.revents & POLLIN)) {
                    struct rdma_cm_event *cev = NULL;
                    if (rdma_get_cm_event(ec, &cev)) die("rdma_get_cm_event");
                    if (cev->event == RDMA_CM_EVENT_DISCONNECTED) {
                        rdma_ack_cm_event(cev);
                        goto out;
                    }
                    rdma_ack_cm_event(cev);
                }
                continue;
            }
        }

        for (int i = 0; i < n; i++) {
            struct ibv_wc *wc = &wcs[i];
            if (wc->status != IBV_WC_SUCCESS) {
                fprintf(stderr, "WC error: %s (status=%d) opcode=%d wr_id=%" PRIu64 "\n",
                        ibv_wc_status_str(wc->status), wc->status, wc->opcode, wc->wr_id);
                goto out;
            }

            if (wc->opcode == IBV_WC_RECV) {
                int idx = (int)wc->wr_id;

                post_echo_send(&c, idx);

                /* Repost policy: inline => repost now; non-inline => repost on SEND completion. */
                if (c.msg_size <= (size_t)c.max_inline && c.max_inline > 0) {
                    post_one_recv(&c, idx);
                }
            } else if (wc->opcode == IBV_WC_SEND) {
                /* Non-inline echo completion -> safe to repost rx buffer. */
                if (wc->wr_id & (1ULL << 63)) {
                    int idx = (int)(wc->wr_id & 0xffffffffu);
                    if (idx >= 0 && idx < c.recv_depth && c.rx_wait_send_cq[idx]) {
                        c.rx_wait_send_cq[idx] = 0;
                        post_one_recv(&c, idx);
                    }
                }
            }
        }
    }

out:
    printf("[server] disconnecting...\n");
    rdma_disconnect(child);
    /* Wait for DISCONNECTED event (best-effort). */
    struct rdma_cm_event *dev = NULL;
    if (rdma_get_cm_event(ec, &dev) == 0) rdma_ack_cm_event(dev);

    conn_cleanup(&c);
}

int main(int argc, char **argv) {
    const char *port = DEFAULT_PORT;

    size_t msg_size = DEFAULT_MSG_SIZE;
    int recv_depth = DEFAULT_RECV_DEPTH;
    int use_event = 0;
    int signal_every = 64; /* every Nth inline echo SEND is signaled; 0 disables */

    /*
     * Usage:
     *   ./cm_pp_server_perf <port> [--msg N] [--recv-depth N] [--event] [--signal-every N]
     *
     * Keep it minimal: server expects client to use the same msg_size.
     */
    if (argc >= 2 && argv[1][0] != '-') {
        port = argv[1];
    }

    for (int i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--msg") && i + 1 < argc) {
            msg_size = (size_t)parse_int(argv[++i], "msg_size");
        } else if (!strcmp(argv[i], "--recv-depth") && i + 1 < argc) {
            recv_depth = parse_int(argv[++i], "recv_depth");
        } else if (!strcmp(argv[i], "--event")) {
            use_event = 1;
        } else if (!strcmp(argv[i], "--signal-every") && i + 1 < argc) {
            signal_every = atoi(argv[++i]);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 2;
        }
    }

    if (msg_size == 0 || msg_size > 1u * 1024u * 1024u) {
        fprintf(stderr, "msg_size must be 1..1048576\n");
        return 2;
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_family = AF_INET;
    hints.ai_flags = RAI_PASSIVE;

    int rc = rdma_getaddrinfo(NULL, port, &hints, &res);
    if (rc) die("rdma_getaddrinfo");

    if (rdma_bind_addr(listen_id, res->ai_src_addr)) die("rdma_bind_addr");
    rdma_freeaddrinfo(res);

    if (rdma_listen(listen_id, 8)) die("rdma_listen");

    printf("[server] rdma_cm listening on 0.0.0.0:%s msg=%zu recv_depth=%d mode=%s\n",
           port, msg_size, recv_depth, use_event ? "event" : "poll");

    for (;;) {
        struct rdma_cm_event *ev = NULL;
        if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");

        if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            struct rdma_cm_id *child = ev->id;
            rdma_ack_cm_event(ev);

            printf("[server] CONNECT_REQUEST (id=%p)\n", (void *)child);
            serve_one_connection(ec, child, msg_size, recv_depth, use_event, signal_every);
            printf("[server] one connection served.\n");
            continue;
        }

        if (ev->event == RDMA_CM_EVENT_DISCONNECTED) {
            rdma_ack_cm_event(ev);
            continue;
        }

        printf("[server] cm event: %s (status=%d)\n", rdma_event_str(ev->event), ev->status);
        rdma_ack_cm_event(ev);
    }

    /* not reached */
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);
    return 0;
}
