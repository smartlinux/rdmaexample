// rdma_pool_server.c
// A minimal RDMA CM server that demonstrates:
//
// - Listening and accepting connections via RDMA CM.
// - Creating RC QPs, with a Shared Receive Queue (SRQ) used by all connections.
// - Pre-registering a large "pool" MR and granting a per-client slice (remote addr + rkey).
// - Control plane via SEND/RECV (client sends REQ_ALLOC, server replies RESP_ALLOC).
// - Data plane via RDMA_WRITE_WITH_IMM as a "doorbell" (server gets RECV_RDMA_WITH_IMM WC).
//
// This variant uses a simple "tick loop": it polls CQ periodically to drain completions
// and uses poll() with a short timeout to handle CM events.
//
// Build:
//   gcc -O2 -Wall -std=c11 rdma_pool_server.c -o rdma_pool_server -lrdmacm -libverbs
//
// Run:
//   ./rdma_pool_server


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

// Remote memory pool. Clients receive a slice within this MR.
#define CHUNK_SIZE 4096
#define NUM_CHUNKS 1024
#define POOL_SIZE ((size_t)CHUNK_SIZE * (size_t)NUM_CHUNKS)

// SRQ (Shared Receive Queue) parameters.
// SRQ allows many QPs to share one receive queue, which simplifies recv-buffer provisioning.
#define SRQ_MAX_WR   4096
#define SRQ_MAX_SGE  1

// How many receive buffers to pre-post into the SRQ.
// If this is too small, the server may hit RNR (Receiver Not Ready) when clients send control messages.
#define RECV_DEPTH   2048

// Size per SRQ receive buffer. Must be >= sizeof(ctrl_msg).
#define RECV_BUF_SZ  64

// CQ capacity. Large enough to hold bursts of completions from all connections.
#define CQ_CAP 4096

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

// Per-connection context: tracks which slice was granted to the client.
// For this polling server, we keep a per-connection send buffer. This is safe because we only
// send one response at a time for each request in this demo.
struct conn_ctx {
    struct rdma_cm_id *id;
    uint32_t qp_num;

    struct ctrl_msg *send_msg;
    struct ibv_mr *send_mr;

    int alloc_start;
    int alloc_nchunks;

    struct conn_ctx *next;
};

// Global server resources shared by all connections.
struct global_ctx {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *listen_id;

    int inited;
    struct ibv_context *verbs;
    struct ibv_pd *pd;

    struct ibv_cq *cq;
    struct ibv_srq *srq;

    // Large pre-registered memory pool.
    char *pool_base;
    struct ibv_mr *pool_mr;
    uint8_t used[NUM_CHUNKS];

    // SRQ receive buffers: a single MR holding RECV_DEPTH fixed-size slots.
    uint8_t *recv_region;
    size_t recv_region_bytes;
    struct ibv_mr *recv_mr;

    struct conn_ctx *conns;
};

static struct conn_ctx *find_conn_by_qp(struct global_ctx *g, uint32_t qp_num) {
    for (struct conn_ctx *c = g->conns; c; c = c->next) {
        if (c->qp_num == qp_num) return c;
    }
    return NULL;
}

static void add_conn(struct global_ctx *g, struct conn_ctx *c) {
    c->next = g->conns;
    g->conns = c;
}

static void remove_conn(struct global_ctx *g, struct conn_ctx *c) {
    struct conn_ctx **pp = &g->conns;
    while (*pp) {
        if (*pp == c) {
            *pp = c->next;
            return;
        }
        pp = &(*pp)->next;
    }
}

// A very small first-fit allocator on top of a chunk bitmap.
// This is intentionally simple; real applications likely use a more robust allocator.
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
    // Each SRQ receive buffer is a fixed-size slot inside recv_region.
    uint8_t *buf = g->recv_region + (size_t)idx * RECV_BUF_SZ;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = RECV_BUF_SZ;
    sge.lkey = g->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    // We encode "which receive buffer slot was used" into wr_id so we can repost it on completion.
    wr.wr_id = (uintptr_t)idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_srq_recv(g->srq, &wr, &bad)) die("ibv_post_srq_recv");
}

static void init_global_resources(struct global_ctx *g, struct ibv_context *verbs) {
    g->verbs = verbs;

    g->pd = ibv_alloc_pd(g->verbs);
    if (!g->pd) die("ibv_alloc_pd");

    g->cq = ibv_create_cq(g->verbs, CQ_CAP, NULL, NULL, 0);
    if (!g->cq) die("ibv_create_cq");

    // Create an SRQ shared by all QPs. All incoming SENDs from clients land in this SRQ.
    struct ibv_srq_init_attr sattr;
    memset(&sattr, 0, sizeof(sattr));
    sattr.attr.max_wr = SRQ_MAX_WR;
    sattr.attr.max_sge = SRQ_MAX_SGE;

    g->srq = ibv_create_srq(g->pd, &sattr);
    if (!g->srq) die("ibv_create_srq");

    // Allocate and register the large pool MR.
    g->pool_base = (char *)xmalloc_aligned(4096, POOL_SIZE);
    if (!g->pool_base) die("posix_memalign pool");

    int pool_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    g->pool_mr = ibv_reg_mr(g->pd, g->pool_base, POOL_SIZE, pool_flags);
    if (!g->pool_mr) die("ibv_reg_mr pool");

    memset(g->used, 0, sizeof(g->used));

    // Allocate and register receive buffers for SRQ.
    g->recv_region_bytes = (size_t)RECV_DEPTH * (size_t)RECV_BUF_SZ;
    size_t bytes = (g->recv_region_bytes + 4095) & ~((size_t)4095);

    g->recv_region = (uint8_t *)xmalloc_aligned(4096, bytes);
    if (!g->recv_region) die("posix_memalign recv_region");

    g->recv_mr = ibv_reg_mr(g->pd, g->recv_region, bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!g->recv_mr) die("ibv_reg_mr recv_mr");

    // Pre-post SRQ receives. This is critical to avoid RNR when clients SEND control messages.
    for (uint32_t i = 0; i < RECV_DEPTH; i++) post_one_srq_recv(g, i);

    g->inited = 1;

    printf("[server] pool ready: base=0x%llx size=%zu rkey=0x%x (chunk=%d, n=%d)\n",
           (unsigned long long)(uintptr_t)g->pool_base,
           (size_t)POOL_SIZE,
           g->pool_mr->rkey,
           CHUNK_SIZE,
           NUM_CHUNKS);
}

static struct conn_ctx *create_conn_ctx(struct global_ctx *g, struct rdma_cm_id *id) {
    struct conn_ctx *c = calloc(1, sizeof(*c));
    if (!c) die("calloc conn_ctx");

    c->id = id;
    c->alloc_start = -1;
    c->alloc_nchunks = 0;

    c->send_msg = (struct ctrl_msg *)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    if (!c->send_msg) die("posix_memalign send_msg");

    c->send_mr = ibv_reg_mr(g->pd, c->send_msg, sizeof(*c->send_msg), IBV_ACCESS_LOCAL_WRITE);
    if (!c->send_mr) die("ibv_reg_mr send_mr");

    return c;
}

static void destroy_conn_ctx(struct global_ctx *g, struct conn_ctx *c) {
    if (!c) return;

    // Release pool slice on disconnect.
    pool_free(g, c->alloc_start, c->alloc_nchunks);

    if (c->send_mr) ibv_dereg_mr(c->send_mr);
    free(c->send_msg);

    if (c->id) {
        if (c->id->qp) rdma_destroy_qp(c->id);
        rdma_destroy_id(c->id);
    }
    free(c);
}

static void on_connect_request(struct global_ctx *g, struct rdma_cm_id *id) {
    // Global resources are created using the verbs context from the first incoming connection.
    if (!g->inited) init_global_resources(g, id->verbs);

    // Create a QP for this connection. Its receive side is attached to the shared SRQ.
    struct ibv_qp_init_attr qattr;
    memset(&qattr, 0, sizeof(qattr));
    qattr.send_cq = g->cq;
    qattr.recv_cq = g->cq;
    qattr.srq = g->srq;
    qattr.qp_type = IBV_QPT_RC;
    qattr.cap.max_send_wr = 256;
    qattr.cap.max_send_sge = 1;

    if (rdma_create_qp(id, g->pd, &qattr)) die("rdma_create_qp");

    struct conn_ctx *c = create_conn_ctx(g, id);
    c->qp_num = id->qp->qp_num;
    add_conn(g, c);

    // Accept connection. The client will get ESTABLISHED once the QP transitions.
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.rnr_retry_count = 7;

    if (rdma_accept(id, &param)) die("rdma_accept");

    printf("[server] CONNECT_REQUEST qp=%u id=%p\n", c->qp_num, (void *)id);
}

static void handle_req_alloc(struct global_ctx *g, struct conn_ctx *c, struct ctrl_msg *m) {
    size_t want = (size_t)ntohl(m->size);

    int start = -1;
    int nchunks = 0;
    int st = pool_alloc(g, want, &start, &nchunks);

    memset(c->send_msg, 0, sizeof(*c->send_msg));
    fill_hdr(c->send_msg, RESP_ALLOC);
    c->send_msg->status = htonl(st == 0 ? 0u : (uint32_t)(-st));

    if (st == 0) {
        c->alloc_start = start;
        c->alloc_nchunks = nchunks;

        size_t alloc_sz = (size_t)nchunks * (size_t)CHUNK_SIZE;
        uint64_t remote_addr = (uint64_t)(uintptr_t)(g->pool_base + (size_t)start * (size_t)CHUNK_SIZE);

        c->send_msg->size = htonl((uint32_t)alloc_sz);
        c->send_msg->addr = htonll_u64(remote_addr);
        c->send_msg->rkey = htonl(g->pool_mr->rkey);

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

    // Use the rdma_cm helper wrapper to post a SEND. This will result in an IBV_WC_SEND completion.
    if (rdma_post_send(c->id, NULL, c->send_msg, sizeof(*c->send_msg), c->send_mr, IBV_SEND_SIGNALED)) {
        die("rdma_post_send RESP_ALLOC");
    }
}

static void handle_doorbell(struct global_ctx *g, struct conn_ctx *c, uint32_t seq) {
    // Doorbell is a WRITE_WITH_IMM completion (server sees RECV_RDMA_WITH_IMM with wc.imm_data).
    // The actual data was written directly into the remote slice memory (pool_base + slice_offset).
    if (c->alloc_start < 0) {
        printf("[server][qp %u] doorbell seq=%u but no slice allocated\n", c->qp_num, seq);
        return;
    }

    size_t alloc_sz = (size_t)c->alloc_nchunks * (size_t)CHUNK_SIZE;
    char *slice = g->pool_base + (size_t)c->alloc_start * (size_t)CHUNK_SIZE;

    size_t off = (size_t)(seq - 1) * sizeof(uint32_t);
    if (off + sizeof(uint32_t) > alloc_sz) {
        printf("[server][qp %u] doorbell seq=%u out of range (off=%zu alloc=%zu)\n",
               c->qp_num, seq, off, alloc_sz);
        return;
    }

    uint32_t v = 0;
    memcpy(&v, slice + off, sizeof(v));
    printf("[server][qp %u] doorbell seq=%u mem=%u\n", c->qp_num, seq, v);
}

static void drain_cq(struct global_ctx *g) {
    // Drain all CQ entries currently available.
    // This is important to keep SRQ receive buffers replenished and to make progress.
    struct ibv_wc wcs[32];

    while (1) {
        int n = ibv_poll_cq(g->cq, (int)(sizeof(wcs) / sizeof(wcs[0])), wcs);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) break;

        for (int i = 0; i < n; i++) {
            struct ibv_wc *wc = &wcs[i];
            if (wc->status != IBV_WC_SUCCESS) {
                fprintf(stderr, "[server] WC error: status=%d opcode=%d qp=%u vendor_err=%u\n",
                        wc->status, wc->opcode, wc->qp_num, wc->vendor_err);
                continue;
            }

            // SEND completions: nothing special to do in this demo.
            if (wc->opcode == IBV_WC_SEND) continue;

            // SRQ receive completions:
            // - IBV_WC_RECV: payload is a client SEND (REQ_ALLOC).
            // - IBV_WC_RECV_RDMA_WITH_IMM: doorbell notification for WRITE_WITH_IMM.
            if (wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                uint32_t idx = (uint32_t)wc->wr_id;
                if (idx >= RECV_DEPTH) {
                    fprintf(stderr, "[server] bad wr_id idx=%u\n", idx);
                    continue;
                }

                struct conn_ctx *c = find_conn_by_qp(g, wc->qp_num);
                uint8_t *buf = g->recv_region + (size_t)idx * (size_t)RECV_BUF_SZ;

                if (!c) {
                    fprintf(stderr, "[server] unknown qp_num=%u (dropping msg)\n", wc->qp_num);
                    post_one_srq_recv(g, idx);
                    continue;
                }

                if (wc->opcode == IBV_WC_RECV) {
                    struct ctrl_msg *m = (struct ctrl_msg *)buf;
                    if (check_hdr(m, REQ_ALLOC) == 0) {
                        handle_req_alloc(g, c, m);
                    }
                } else {
                    if ((wc->wc_flags & IBV_WC_WITH_IMM) != 0) {
                        uint32_t seq = ntohl(wc->imm_data);
                        handle_doorbell(g, c, seq);
                    }
                }

                // Repost the SRQ buffer to keep receive credits available.
                post_one_srq_recv(g, idx);
                continue;
            }

            // Other opcodes are ignored in this demo.
        }
    }
}

static void on_disconnected(struct global_ctx *g, struct rdma_cm_id *id) {
    struct conn_ctx *c = NULL;
    for (struct conn_ctx *p = g->conns; p; p = p->next) {
        if (p->id == id) { c = p; break; }
    }

    if (!c) {
        printf("[server] DISCONNECTED unknown id=%p\n", (void *)id);
        if (id->qp) rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return;
    }

    printf("[server] DISCONNECTED qp=%u id=%p\n", c->qp_num, (void *)id);
    remove_conn(g, c);
    destroy_conn_ctx(g, c);
}

int main(void) {
    struct global_ctx g;
    memset(&g, 0, sizeof(g));

    g.ec = rdma_create_event_channel();
    if (!g.ec) die("rdma_create_event_channel");

    if (rdma_create_id(g.ec, &g.listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id listen");

    // Bind to 0.0.0.0:LISTEN_PORT.
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LISTEN_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (rdma_bind_addr(g.listen_id, (struct sockaddr *)&addr)) die("rdma_bind_addr");
    if (rdma_listen(g.listen_id, BACKLOG)) die("rdma_listen");

    printf("[server] listening on 0.0.0.0:%d\n", LISTEN_PORT);

    // A simple loop that (a) drains CQ frequently and (b) processes CM events.
    // Using a short timeout makes the loop responsive but still simple.
    while (1) {
        if (g.inited) drain_cq(&g);

        struct pollfd pfd;
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = g.ec->fd;
        pfd.events = POLLIN;

        int pr = poll(&pfd, 1, 10);  // 10ms "tick"
        if (pr < 0) die("poll");
        if (pr == 0) continue;
        if ((pfd.revents & POLLIN) == 0) continue;

        struct rdma_cm_event *ev = NULL;
        if (rdma_get_cm_event(g.ec, &ev)) die("rdma_get_cm_event");

        enum rdma_cm_event_type e = ev->event;
        struct rdma_cm_id *id = ev->id;

        // ACK the event after we have read the fields we need.
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

    return 0;
}
