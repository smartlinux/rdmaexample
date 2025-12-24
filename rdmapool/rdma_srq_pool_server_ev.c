// rdma_srq_pool_server_ev.c
// An event-driven RDMA CM server with SRQ and completion channel.
//
// This server is functionally similar to rdma_pool_server.c, but it demonstrates
// a "true" event-driven CQ handling model:
//
// - Uses ibv_create_comp_channel() + ibv_req_notify_cq() to receive CQ events.
// - Uses poll() blocking on both:
//     (1) the RDMA CM event channel fd (connection management events)
//     (2) the CQ completion channel fd (data/completion events)
// - On CQ event, calls ibv_get_cq_event() to dequeue the notification,
//   acknowledges it via ibv_ack_cq_events(), rearms notifications via ibv_req_notify_cq(),
//   and then drains the CQ via ibv_poll_cq() until empty.
//
// It also uses a shared receive queue (SRQ) and a small send-buffer pool to support
// multiple concurrent outstanding SEND responses without per-connection buffer reuse issues.
//
// Build:
//   gcc -O2 -Wall -std=c11 rdma_srq_pool_server_ev.c -o rdma_srq_pool_server_ev -lrdmacm -libverbs
//
// Run:
//   ./rdma_srq_pool_server_ev

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h> 
#include <infiniband/verbs.h>

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define LISTEN_PORT 7471
#define BACKLOG 64

// Control-plane protocol.
#define MAGIC   0x52444d41u  // 'RDMA'
#define VERSION 1

// Remote memory pool.
#define CHUNK_SIZE 4096
#define NUM_CHUNKS 1024
#define POOL_SIZE ((size_t)CHUNK_SIZE * (size_t)NUM_CHUNKS)

// SRQ parameters.
#define SRQ_MAX_WR   4096
#define SRQ_MAX_SGE  1
#define RECV_DEPTH   2048
#define RECV_BUF_SZ  64

// CQ parameters.
#define CQ_CAP 4096

// Send buffer pool for control-plane responses.
// In an event-driven server, multiple connections may request allocations concurrently,
// so it is convenient to have a pool of pre-registered send buffers.
#define SEND_POOL_SZ 4096  // Must be >= maximum concurrent outstanding control-plane SENDs.

static inline uint64_t htonll_u64(uint64_t x) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(x & 0xffffffffULL)) << 32) | htonl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static inline uint64_t ntohll_u64(uint64_t x) {
    return htonll_u64(x);
}

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

enum msg_type {
    REQ_ALLOC  = 1,
    RESP_ALLOC = 2,
};

struct ctrl_msg {
    // All fields are in network byte order on the wire.
    uint32_t magic;
    uint16_t ver;
    uint16_t type;
    uint32_t size;
    uint64_t addr;
    uint32_t rkey;
    uint32_t status;
};

static void fill_hdr(struct ctrl_msg *m, uint16_t type) {
    m->magic = htonl(MAGIC);
    m->ver = htons(VERSION);
    m->type = htons(type);
}

static int check_hdr(const struct ctrl_msg *m, uint16_t expect_type) {
    if (ntohl(m->magic) != MAGIC) return -1;
    if (ntohs(m->ver) != VERSION) return -2;
    if (ntohs(m->type) != expect_type) return -3;
    return 0;
}

static void *xmalloc_aligned(size_t alignment, size_t size) {
    void *p = NULL;
    int rc = posix_memalign(&p, alignment, size);
    if (rc != 0) {
        errno = rc;
        return NULL;
    }
    memset(p, 0, size);
    return p;
}

// Per-connection state.
struct conn_ctx {
    struct rdma_cm_id *id;
    uint32_t qp_num;

    int alloc_start;
    int alloc_nchunks;
};

// QP number -> conn_ctx map (open-addressing hash table).
// We need this because CQ completions only tell us qp_num, not the rdma_cm_id pointer.
#define QPMAP_CAP 8192  // Must be power-of-two.

struct qpmap_entry {
    uint32_t key;
    struct conn_ctx *val;
    uint8_t used;  // 0 empty, 1 used, 2 tombstone
};

struct qpmap {
    struct qpmap_entry *tab;
    size_t cap;
};

static uint32_t hash_u32(uint32_t x) {
    x ^= x >> 16;
    x *= 0x7feb352dU;
    x ^= x >> 15;
    x *= 0x846ca68bU;
    x ^= x >> 16;
    return x;
}

static void qpmap_init(struct qpmap *m, size_t cap_pow2) {
    m->cap = cap_pow2;
    m->tab = calloc(m->cap, sizeof(*m->tab));
    if (!m->tab) die("calloc qpmap");
}

static void qpmap_put(struct qpmap *m, uint32_t key, struct conn_ctx *val) {
    size_t mask = m->cap - 1;
    size_t i = (size_t)hash_u32(key) & mask;
    size_t first_tomb = (size_t)-1;

    for (size_t step = 0; step < m->cap; step++) {
        struct qpmap_entry *e = &m->tab[i];
        if (e->used == 0) {
            if (first_tomb != (size_t)-1) e = &m->tab[first_tomb];
            e->key = key;
            e->val = val;
            e->used = 1;
            return;
        }
        if (e->used == 2) {
            if (first_tomb == (size_t)-1) first_tomb = i;
        } else if (e->key == key) {
            e->val = val;
            return;
        }
        i = (i + 1) & mask;
    }

    fprintf(stderr, "qpmap full\n");
    exit(1);
}

static struct conn_ctx *qpmap_get(struct qpmap *m, uint32_t key) {
    size_t mask = m->cap - 1;
    size_t i = (size_t)hash_u32(key) & mask;

    for (size_t step = 0; step < m->cap; step++) {
        struct qpmap_entry *e = &m->tab[i];
        if (e->used == 0) return NULL;
        if (e->used == 1 && e->key == key) return e->val;
        i = (i + 1) & mask;
    }
    return NULL;
}

static void qpmap_del(struct qpmap *m, uint32_t key) {
    size_t mask = m->cap - 1;
    size_t i = (size_t)hash_u32(key) & mask;

    for (size_t step = 0; step < m->cap; step++) {
        struct qpmap_entry *e = &m->tab[i];
        if (e->used == 0) return;
        if (e->used == 1 && e->key == key) {
            e->used = 2;
            e->val = NULL;
            return;
        }
        i = (i + 1) & mask;
    }
}

// Global server resources.
struct global_ctx {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *listen_id;

    int inited;
    struct ibv_context *verbs;
    struct ibv_pd *pd;

    struct ibv_comp_channel *comp_ch;
    struct ibv_cq *cq;
    struct ibv_srq *srq;

    // Pool MR.
    char *pool_base;
    struct ibv_mr *pool_mr;
    uint8_t used[NUM_CHUNKS];

    // SRQ receive buffers.
    uint8_t *recv_region;
    size_t recv_region_bytes;
    struct ibv_mr *recv_mr;

    // Connection map.
    struct qpmap qpm;

    // Send buffer pool.
    uint8_t *send_region;
    size_t send_region_bytes;
    struct ibv_mr *send_mr;

    int send_free[SEND_POOL_SZ];
    int send_top;
};

static int pool_alloc(struct global_ctx *g, size_t want_bytes, int *out_start, int *out_nchunks) {
    int need = (int)((want_bytes + CHUNK_SIZE - 1) / CHUNK_SIZE);
    if (need <= 0) need = 1;
    if (need > NUM_CHUNKS) return -1;

    for (int i = 0; i + need <= NUM_CHUNKS; i++) {
        int ok = 1;
        for (int j = 0; j < need; j++) {
            if (g->used[i + j]) { ok = 0; break; }
        }
        if (ok) {
            for (int j = 0; j < need; j++) g->used[i + j] = 1;
            *out_start = i;
            *out_nchunks = need;
            return 0;
        }
    }
    return -2;
}

static void pool_free(struct global_ctx *g, int start, int nchunks) {
    if (start < 0 || nchunks <= 0) return;
    for (int i = 0; i < nchunks; i++) {
        int idx = start + i;
        if (0 <= idx && idx < NUM_CHUNKS) g->used[idx] = 0;
    }
}

static void post_one_srq_recv(struct global_ctx *g, uint32_t idx) {
    uint8_t *buf = g->recv_region + (size_t)idx * (size_t)RECV_BUF_SZ;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = RECV_BUF_SZ;
    sge.lkey = g->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)idx;  // recv slot index
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_srq_recv(g->srq, &wr, &bad)) die("ibv_post_srq_recv");
}

static int sendbuf_alloc(struct global_ctx *g, struct ctrl_msg **out_msg) {
    if (g->send_top == 0) return -1;
    int idx = g->send_free[--g->send_top];
    *out_msg = (struct ctrl_msg *)(g->send_region + (size_t)idx * sizeof(struct ctrl_msg));
    return idx;
}

static void sendbuf_free(struct global_ctx *g, int idx) {
    if (idx < 0 || idx >= SEND_POOL_SZ) return;
    g->send_free[g->send_top++] = idx;
}

// We encode the send-buffer index in wc.wr_id for SEND completions.
// Note: wc.wr_id is 64-bit; we use low bits for our index.
static inline uint64_t make_send_wr_id(int send_idx) {
    return (uint64_t)(uint32_t)send_idx | (1ULL << 63);
}

static inline int is_send_wr_id(uint64_t wr_id) {
    return (wr_id >> 63) != 0;
}

static inline int send_idx_from_wr_id(uint64_t wr_id) {
    return (int)(uint32_t)(wr_id & 0x7fffffffULL);
}

static void init_global_resources(struct global_ctx *g, struct ibv_context *verbs) {
    g->verbs = verbs;

    g->pd = ibv_alloc_pd(g->verbs);
    if (!g->pd) die("ibv_alloc_pd");

    // Send buffer pool (control plane).
    g->send_region_bytes = (size_t)SEND_POOL_SZ * sizeof(struct ctrl_msg);
    size_t bytes = (g->send_region_bytes + 4095) & ~((size_t)4095);

    g->send_region = (uint8_t *)xmalloc_aligned(4096, bytes);
    if (!g->send_region) die("posix_memalign send_region");

    g->send_mr = ibv_reg_mr(g->pd, g->send_region, bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!g->send_mr) die("ibv_reg_mr send_mr");

    g->send_top = 0;
    for (int i = 0; i < SEND_POOL_SZ; i++) g->send_free[g->send_top++] = i;

    // CQ completion channel + CQ.
    g->comp_ch = ibv_create_comp_channel(g->verbs);
    if (!g->comp_ch) die("ibv_create_comp_channel");

    g->cq = ibv_create_cq(g->verbs, CQ_CAP, NULL, g->comp_ch, 0);
    if (!g->cq) die("ibv_create_cq");

    // "Arm" CQ notifications. This requests an event on the completion channel when new CQEs arrive.
    // Notifications are edge-triggered, so we must rearm after each event.
    if (ibv_req_notify_cq(g->cq, 0)) die("ibv_req_notify_cq");

    // SRQ.
    struct ibv_srq_init_attr sattr;
    memset(&sattr, 0, sizeof(sattr));
    sattr.attr.max_wr = SRQ_MAX_WR;
    sattr.attr.max_sge = SRQ_MAX_SGE;

    g->srq = ibv_create_srq(g->pd, &sattr);
    if (!g->srq) die("ibv_create_srq");

    // Pool MR.
    g->pool_base = (char *)xmalloc_aligned(4096, POOL_SIZE);
    if (!g->pool_base) die("posix_memalign pool");

    int pool_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    g->pool_mr = ibv_reg_mr(g->pd, g->pool_base, POOL_SIZE, pool_flags);
    if (!g->pool_mr) die("ibv_reg_mr pool");

    memset(g->used, 0, sizeof(g->used));

    // SRQ receive buffers.
    g->recv_region_bytes = (size_t)RECV_DEPTH * (size_t)RECV_BUF_SZ;
    bytes = (g->recv_region_bytes + 4095) & ~((size_t)4095);

    g->recv_region = (uint8_t *)xmalloc_aligned(4096, bytes);
    if (!g->recv_region) die("posix_memalign recv_region");

    g->recv_mr = ibv_reg_mr(g->pd, g->recv_region, bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!g->recv_mr) die("ibv_reg_mr recv_mr");

    for (uint32_t i = 0; i < RECV_DEPTH; i++) post_one_srq_recv(g, i);

    qpmap_init(&g->qpm, QPMAP_CAP);

    g->inited = 1;

    printf("[server] pool ready: base=0x%llx size=%zu rkey=0x%x (chunk=%d, n=%d)\n",
           (unsigned long long)(uintptr_t)g->pool_base,
           (size_t)POOL_SIZE,
           g->pool_mr->rkey,
           CHUNK_SIZE,
           NUM_CHUNKS);
    printf("[server] SRQ depth=%d, CQ channel fd=%d\n", RECV_DEPTH, g->comp_ch->fd);
}

static struct conn_ctx *create_conn_ctx(struct rdma_cm_id *id) {
    struct conn_ctx *c = calloc(1, sizeof(*c));
    if (!c) die("calloc conn_ctx");

    c->id = id;
    c->qp_num = id->qp ? id->qp->qp_num : 0;
    c->alloc_start = -1;
    c->alloc_nchunks = 0;

    return c;
}

static void destroy_conn_ctx(struct global_ctx *g, struct conn_ctx *c) {
    if (!c) return;
    pool_free(g, c->alloc_start, c->alloc_nchunks);

    if (c->id) {
        if (c->id->qp) rdma_destroy_qp(c->id);
        rdma_destroy_id(c->id);
    }
    free(c);
}

static void on_connect_request(struct global_ctx *g, struct rdma_cm_id *id) {
    if (!g->inited) init_global_resources(g, id->verbs);

    // Create a QP attached to the shared SRQ and the shared CQ.
    struct ibv_qp_init_attr qattr;
    memset(&qattr, 0, sizeof(qattr));
    qattr.send_cq = g->cq;
    qattr.recv_cq = g->cq;
    qattr.srq = g->srq;
    qattr.qp_type = IBV_QPT_RC;
    qattr.cap.max_send_wr = 256;
    qattr.cap.max_send_sge = 1;

    if (rdma_create_qp(id, g->pd, &qattr)) die("rdma_create_qp");

    struct conn_ctx *c = create_conn_ctx(id);
    qpmap_put(&g->qpm, c->qp_num, c);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.rnr_retry_count = 7;

    if (rdma_accept(id, &param)) die("rdma_accept");

    printf("[server] CONNECT_REQUEST qp=%u id=%p\n", c->qp_num, (void *)id);
}

static void handle_req_alloc(struct global_ctx *g, struct conn_ctx *c, const struct ctrl_msg *m) {
    size_t want = (size_t)ntohl(m->size);

    int start = -1;
    int nchunks = 0;
    int st = pool_alloc(g, want, &start, &nchunks);

    struct ctrl_msg *resp = NULL;
    int send_idx = sendbuf_alloc(g, &resp);
    if (send_idx < 0) {
        fprintf(stderr, "[server] send buffer pool exhausted (increase SEND_POOL_SZ)\n");
        return;
    }

    memset(resp, 0, sizeof(*resp));
    fill_hdr(resp, RESP_ALLOC);
    resp->status = htonl(st == 0 ? 0u : (uint32_t)(-st));

    if (st == 0) {
        c->alloc_start = start;
        c->alloc_nchunks = nchunks;

        size_t alloc_sz = (size_t)nchunks * (size_t)CHUNK_SIZE;
        uint64_t remote_addr = (uint64_t)(uintptr_t)(g->pool_base + (size_t)start * (size_t)CHUNK_SIZE);

        resp->size = htonl((uint32_t)alloc_sz);
        resp->addr = htonll_u64(remote_addr);
        resp->rkey = htonl(g->pool_mr->rkey);

        printf("[server][qp %u] grant slice: start=%d nchunks=%d addr=0x%llx size=%zu rkey=0x%x\n",
               c->qp_num,
               start,
               nchunks,
               (unsigned long long)remote_addr,
               alloc_sz,
               g->pool_mr->rkey);
    } else {
        printf("[server][qp %u] alloc failed want=%zu st=%d\n", c->qp_num, want, st);
    }

    // Post the response SEND. We use wr_id to remember which send buffer to free on completion.
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)resp;
    sge.length = sizeof(*resp);
    sge.lkey = g->send_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = make_send_wr_id(send_idx);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->id->qp, &wr, &bad)) die("ibv_post_send RESP_ALLOC");
}

static void handle_doorbell(struct global_ctx *g, struct conn_ctx *c, uint32_t seq) {
    if (c->alloc_start < 0) return;

    size_t alloc_sz = (size_t)c->alloc_nchunks * (size_t)CHUNK_SIZE;
    char *slice = g->pool_base + (size_t)c->alloc_start * (size_t)CHUNK_SIZE;

    size_t off = (size_t)(seq - 1) * sizeof(uint32_t);
    if (off + sizeof(uint32_t) > alloc_sz) {
        printf("[server][qp %u] doorbell seq=%u out of range\n", c->qp_num, seq);
        return;
    }

    uint32_t v = 0;
    memcpy(&v, slice + off, sizeof(v));
    printf("[server][qp %u] doorbell seq=%u mem=%u\n", c->qp_num, seq, v);
}

static void drain_cq(struct global_ctx *g) {
    // Drain CQ completely. This is essential:
    // - It reposts SRQ receives so the server does not run out of recv credits.
    // - It frees send buffers when SEND completions arrive.
    struct ibv_wc wcs[64];

    while (1) {
        int n = ibv_poll_cq(g->cq, (int)(sizeof(wcs) / sizeof(wcs[0])), wcs);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) break;

        for (int i = 0; i < n; i++) {
            struct ibv_wc *wc = &wcs[i];
            if (wc->status != IBV_WC_SUCCESS) {
                fprintf(stderr, "[server] WC error status=%d opcode=%d qp=%u vendor_err=%u\n",
                        wc->status, wc->opcode, wc->qp_num, wc->vendor_err);
                continue;
            }

            // Free send buffers on SEND completions.
            if (wc->opcode == IBV_WC_SEND && is_send_wr_id(wc->wr_id)) {
                int send_idx = send_idx_from_wr_id(wc->wr_id);
                sendbuf_free(g, send_idx);
                continue;
            }

            // SRQ receive completions.
            if (wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                uint32_t recv_idx = (uint32_t)wc->wr_id;
                if (recv_idx >= RECV_DEPTH) {
                    fprintf(stderr, "[server] bad recv wr_id=%u\n", recv_idx);
                    continue;
                }

                struct conn_ctx *c = qpmap_get(&g->qpm, wc->qp_num);
                uint8_t *buf = g->recv_region + (size_t)recv_idx * (size_t)RECV_BUF_SZ;

                if (wc->opcode == IBV_WC_RECV) {
                    if (c) {
                        struct ctrl_msg *m = (struct ctrl_msg *)buf;
                        if (check_hdr(m, REQ_ALLOC) == 0) handle_req_alloc(g, c, m);
                    }
                } else {
                    if (c && (wc->wc_flags & IBV_WC_WITH_IMM)) {
                        uint32_t seq = ntohl(wc->imm_data);
                        handle_doorbell(g, c, seq);
                    }
                }

                // Repost SRQ recv buffer to maintain receive credits.
                post_one_srq_recv(g, recv_idx);
                continue;
            }

            // Other completions are ignored in this demo.
        }
    }
}

static void on_disconnected(struct global_ctx *g, struct rdma_cm_id *id) {
    uint32_t qp = id->qp ? id->qp->qp_num : 0;
    struct conn_ctx *c = qp ? qpmap_get(&g->qpm, qp) : NULL;

    printf("[server] DISCONNECTED qp=%u id=%p\n", qp, (void *)id);

    if (c) {
        qpmap_del(&g->qpm, qp);
        destroy_conn_ctx(g, c);
    } else {
        if (id->qp) rdma_destroy_qp(id);
        rdma_destroy_id(id);
    }
}

static void handle_cq_event(struct global_ctx *g) {
    // Dequeue exactly one CQ event from the completion channel.
    // You must call ibv_get_cq_event() to remove the event from the channel,
    // and then ibv_ack_cq_events() to acknowledge it (to avoid event-queue overflow).
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;

    if (ibv_get_cq_event(g->comp_ch, &ev_cq, &ev_ctx)) die("ibv_get_cq_event");

    // Acknowledge one event.
    ibv_ack_cq_events(ev_cq, 1);

    // Rearm notifications. Typically you rearm before draining to avoid a race where
    // completions arrive between draining and rearming (edge-triggered notifications).
    if (ibv_req_notify_cq(ev_cq, 0)) die("ibv_req_notify_cq");

    // Now drain all CQEs.
    drain_cq(g);
}

int main(void) {
    struct global_ctx g;
    memset(&g, 0, sizeof(g));

    g.ec = rdma_create_event_channel();
    if (!g.ec) die("rdma_create_event_channel");

    if (rdma_create_id(g.ec, &g.listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id listen");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LISTEN_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (rdma_bind_addr(g.listen_id, (struct sockaddr *)&addr)) die("rdma_bind_addr");
    if (rdma_listen(g.listen_id, BACKLOG)) die("rdma_listen");

    printf("[server] listening on 0.0.0.0:%d\n", LISTEN_PORT);

    while (1) {
        struct pollfd fds[2];
        nfds_t nfds = 0;

        // RDMA CM event channel.
        fds[nfds].fd = g.ec->fd;
        fds[nfds].events = POLLIN;
        nfds++;

        // CQ completion channel (available only after global init).
        if (g.inited) {
            fds[nfds].fd = g.comp_ch->fd;
            fds[nfds].events = POLLIN;
            nfds++;
        }

        int pr = poll(fds, nfds, -1);
        if (pr < 0) die("poll");

        // If CQ events are enabled, handle them first so we do not starve recv reposting.
        if (g.inited && nfds == 2 && (fds[1].revents & POLLIN)) {
            handle_cq_event(&g);
        }

        if (fds[0].revents & POLLIN) {
            struct rdma_cm_event *ev = NULL;
            if (rdma_get_cm_event(g.ec, &ev)) die("rdma_get_cm_event");

            enum rdma_cm_event_type e = ev->event;
            struct rdma_cm_id *id = ev->id;

            // ACK the event after reading the relevant fields.
            rdma_ack_cm_event(ev);

            if (e == RDMA_CM_EVENT_CONNECT_REQUEST) {
                on_connect_request(&g, id);
            } else if (e == RDMA_CM_EVENT_ESTABLISHED) {
                uint32_t qp = id->qp ? id->qp->qp_num : 0;
                printf("[server] ESTABLISHED qp=%u id=%p\n", qp, (void *)id);
            } else if (e == RDMA_CM_EVENT_DISCONNECTED) {
                on_disconnected(&g, id);
            } else {
                printf("[server] CM event %d id=%p\n", e, (void *)id);
            }
        }
    }

    return 0;
}
