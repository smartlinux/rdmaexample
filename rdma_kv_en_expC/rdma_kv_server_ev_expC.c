
// rdma_kv_server_ev.c
//
// A compact RDMA key-value server for learning purposes.
//
// The design follows a common "control-plane two-sided + data-plane one-sided" pattern:
//
//   Control plane (two-sided SEND/RECV):
//     - Client sends KV_PUT_REQ or KV_GET_REQ.
//     - Server replies KV_PUT_RESP or KV_GET_RESP.
//
//   Data plane (one-sided RDMA on the same RC QP):
//     - For PUT, the server allocates a slice from a pre-registered memory pool and
//       returns (remote_addr, rkey, length, token, version) to the client.
//     - Client performs RDMA_WRITE to fill the value into that slice.
//     - Client then performs RDMA_WRITE_WITH_IMM (doorbell) with imm_data = token,
//       which notifies the server that the PUT payload is ready (and also provides
//       ordering relative to the earlier RDMA_WRITE on an RC QP).
//     - For GET, the server returns (remote_addr, rkey, length), and the client uses RDMA_READ.
//
// The server uses:
//   - rdma_cm for connection management
//   - one shared receive queue (SRQ) for two-sided receives across all connections
//   - a completion channel (event-driven CQ) for completions
//   - a send-buffer pool for safe concurrent SEND replies
//
// Build:
//   gcc -O2 -Wall -std=c11 rdma_kv_server_ev.c -o rdma_kv_server_ev -lrdmacm -libverbs
//
// Run (RXE / RoCE / IB):
//   ./rdma_kv_server_ev [listen_port]
//   Default port is 7471.


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

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))
#endif

// ============================ Configuration ============================

#define LISTEN_PORT_DEFAULT 7471
#define BACKLOG             64

// Pool: the server pre-registers a big MR and carves it into fixed-size chunks.
#define CHUNK_SIZE 4096
#define NUM_CHUNKS 2048
#define POOL_SIZE  ((size_t)CHUNK_SIZE * (size_t)NUM_CHUNKS)

// SRQ: receive buffers are taken from a global region and reposted after each RECV completion.
#define SRQ_MAX_WR  4096
#define SRQ_MAX_SGE 1
#define DEFAULT_RECV_DEPTH  2048
#define RECV_BUF_SZ 256

// CQ: capacity must cover your worst-case outstanding completions.
#define CQ_CAP 8192

// Send buffer pool: server replies are SENDs, so the server must not overwrite a buffer
// until it receives the corresponding SEND completion.
#define SEND_POOL_SZ 4096

// KV table: a simple open addressing hash table.
#define KEY_MAX    32
#define KV_CAP     4096   // power-of-two for masking

// ============================ Utilities ============================

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

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

static uint32_t hash_u32(uint32_t x) {
    // A small integer mixer.
    x ^= x >> 16;
    x *= 0x7feb352dU;
    x ^= x >> 15;
    x *= 0x846ca68bU;
    x ^= x >> 16;
    return x;
}

static uint32_t hash_bytes(const uint8_t *p, size_t n) {
    // FNV-1a 32-bit.
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

// ============================ Protocol ============================

#define KV_MAGIC   0x4b565244u  // 'KVRD'
#define KV_VERSION 1

enum kv_type {
    KV_PUT_REQ  = 1,
    KV_PUT_RESP = 2,
    KV_GET_REQ  = 3,
    KV_GET_RESP = 4
};

// All multi-byte fields are in network byte order on the wire.
struct kv_msg {
    uint32_t magic;       // KV_MAGIC
    uint16_t ver;         // KV_VERSION
    uint16_t type;        // enum kv_type

    uint32_t status;      // 0 = OK, otherwise errno-style positive value
    uint32_t key_len;     // <= KEY_MAX
    uint32_t value_len;   // PUT: requested length; RESP: granted/stored length

    uint32_t token;       // PUT: server-chosen token for WRITE_WITH_IMM commit
    uint32_t version;     // server version counter (monotonic per server)

    uint64_t addr;        // server pool remote address (for READ/WRITE)
    uint32_t rkey;        // server pool rkey
    uint32_t _reserved;

    uint8_t  key[KEY_MAX];
    uint8_t  _pad[RECV_BUF_SZ - 4 - 2 - 2 - 4 * 6 - 8 - 4 - 4 - KEY_MAX];
};
_Static_assert(sizeof(struct kv_msg) == RECV_BUF_SZ, "kv_msg must fit in RECV_BUF_SZ");


struct kv_prev_slice {
    int         valid;      /* 0 = no previous value (new insert), 1 = replaced */
    int         start;
    int         nchunks;
    uint32_t    value_len;
    uint32_t    version;
};

static void kv_fill_hdr(struct kv_msg *m, uint16_t type) {
    m->magic = htonl(KV_MAGIC);
    m->ver   = htons(KV_VERSION);
    m->type  = htons(type);
}

static int kv_check_hdr(const struct kv_msg *m, uint16_t expect_type) {
    if (ntohl(m->magic) != KV_MAGIC) return -1;
    if (ntohs(m->ver) != KV_VERSION) return -2;
    if (ntohs(m->type) != expect_type) return -3;
    return 0;
}

// ============================ Pool Allocator ============================

struct pool {
    char *base;
    struct ibv_mr *mr;
    uint8_t used[NUM_CHUNKS]; // 0 free, 1 used
};

static int pool_alloc(struct pool *p, size_t want_bytes, int *out_start, int *out_nchunks) {
    if (want_bytes == 0) return -EINVAL;

    int nchunks = (int)((want_bytes + CHUNK_SIZE - 1) / CHUNK_SIZE);
    if (nchunks <= 0 || nchunks > NUM_CHUNKS) return -ENOMEM;

    int run = 0;
    int start = -1;
    for (int i = 0; i < NUM_CHUNKS; i++) {
        if (!p->used[i]) {
            if (run == 0) start = i;
            run++;
            if (run == nchunks) {
                for (int j = start; j < start + nchunks; j++) p->used[j] = 1;
                *out_start = start;
                *out_nchunks = nchunks;
                return 0;
            }
        } else {
            run = 0;
            start = -1;
        }
    }
    return -ENOMEM;
}

static void pool_free(struct pool *p, int start, int nchunks) {
    if (start < 0 || nchunks <= 0) return;
    if (start + nchunks > NUM_CHUNKS) return;
    for (int i = start; i < start + nchunks; i++) p->used[i] = 0;
}

static inline uint64_t pool_remote_addr(const struct pool *p, int start_chunk) {
    return (uint64_t)(uintptr_t)(p->base + (size_t)start_chunk * CHUNK_SIZE);
}

// ============================ KV Table ============================

struct kv_entry {
    uint8_t used;             // 0 empty, 1 used, 2 tombstone
    uint8_t key_len;
    uint8_t key[KEY_MAX];

    // Location of the value in the pool.
    int start;
    int nchunks;
    uint32_t value_len;

    uint32_t version;
};

struct kv_table {
    struct kv_entry tab[KV_CAP];
};

static int kv_key_equal(const uint8_t *a, uint8_t alen, const uint8_t *b, uint8_t blen) {
    if (alen != blen) return 0;
    if (alen == 0) return 1;
    return memcmp(a, b, alen) == 0;
}

static int kv_find_slot(struct kv_table *t, const uint8_t *key, uint8_t key_len, int for_insert) {
    uint32_t h = hash_bytes(key, key_len);
    uint32_t mask = KV_CAP - 1;
    int first_tomb = -1;

    for (uint32_t i = 0; i < KV_CAP; i++) {
        uint32_t idx = (h + i) & mask;
        struct kv_entry *e = &t->tab[idx];

        if (e->used == 0) {
            if (for_insert && first_tomb >= 0) return first_tomb;
            return (int)idx;
        }
        if (e->used == 2) {
            if (for_insert && first_tomb < 0) first_tomb = (int)idx;
            continue;
        }
        if (kv_key_equal(e->key, e->key_len, key, key_len)) {
            return (int)idx;
        }
    }
    return -1;
}

static struct kv_entry *kv_get(struct kv_table *t, const uint8_t *key, uint8_t key_len) {
    int idx = kv_find_slot(t, key, key_len, 0);
    if (idx < 0) return NULL;
    struct kv_entry *e = &t->tab[idx];
    if (e->used != 1) return NULL;
    if (!kv_key_equal(e->key, e->key_len, key, key_len)) return NULL;
    return e;
}

static struct kv_entry *kv_upsert(struct kv_table *t,
                                  const uint8_t *key, uint8_t key_len,
                                  int start, int nchunks,
                                  uint32_t value_len, uint32_t version,
                                  struct kv_prev_slice *prev)
{
    if (prev) {
        memset(prev, 0, sizeof(*prev));
        prev->valid = 0;
    }

    uint32_t h = hash_bytes(key, key_len);
    uint32_t mask = KV_CAP - 1;
    int first_tomb = -1;

    for (uint32_t i = 0; i < KV_CAP; i++) {
        uint32_t idx = (h + i) & mask;
        struct kv_entry *e = &t->tab[idx];

        if (e->used == 0) {
            /* Empty slot ends the probe chain. Insert either here or at first tombstone. */
            if (first_tomb >= 0) {
                e = &t->tab[first_tomb];
                idx = (uint32_t)first_tomb;
            }
            /* New insert: prev->valid remains 0 */
            e->used = 1;
            e->key_len = key_len;
            memset(e->key, 0, KEY_MAX);
            memcpy(e->key, key, key_len);

            e->start = start;
            e->nchunks = nchunks;
            e->value_len = value_len;
            e->version = version;
            return e;
        }

        if (e->used == 2) {
            /* Tombstone: record first tombstone for possible insertion, but keep probing. */
            if (first_tomb < 0) first_tomb = (int)idx;
            continue;
        }

        /* used == 1: occupied */
        if (kv_key_equal(e->key, e->key_len, key, key_len)) {
            /* Update existing key: capture old slice metadata as a VALUE COPY. */
            if (prev) {
                prev->valid = 1;
                prev->start = e->start;
                prev->nchunks = e->nchunks;
                prev->value_len = e->value_len;
                prev->version = e->version;
            }

            /* Overwrite with the new value's slice metadata. */
            e->start = start;
            e->nchunks = nchunks;
            e->value_len = value_len;
            e->version = version;
            return e;
        }
    }

    /* Table full (or pathological probe): no slot found. */
    return NULL;
}


// ============================ QP map: qp_num -> conn_ctx ============================

#define QPMAP_CAP 8192  // power-of-two

struct conn_ctx; // forward

struct qpmap_entry {
    uint32_t key;            // qp_num
    struct conn_ctx *val;
    uint8_t used;            // 0 empty, 1 used, 2 tombstone
};
struct qpmap {
    struct qpmap_entry *tab;
    size_t cap;
};

static void qpmap_init(struct qpmap *m, size_t cap_pow2) {
    m->cap = cap_pow2;
    m->tab = calloc(m->cap, sizeof(m->tab[0]));
    if (!m->tab) die("calloc qpmap");
}

static void qpmap_put(struct qpmap *m, uint32_t key, struct conn_ctx *val) {
    uint32_t h = hash_u32(key);
    uint32_t mask = (uint32_t)(m->cap - 1);
    int first_tomb = -1;

    for (uint32_t i = 0; i < m->cap; i++) {
        uint32_t idx = (h + i) & mask;
        if (m->tab[idx].used == 0) {
            if (first_tomb >= 0) idx = (uint32_t)first_tomb;
            m->tab[idx].key = key;
            m->tab[idx].val = val;
            m->tab[idx].used = 1;
            return;
        }
        if (m->tab[idx].used == 2 && first_tomb < 0) {
            first_tomb = (int)idx;
            continue;
        }
        if (m->tab[idx].used == 1 && m->tab[idx].key == key) {
            m->tab[idx].val = val;
            return;
        }
    }
    die("qpmap_put: full");
}

static struct conn_ctx *qpmap_get(struct qpmap *m, uint32_t key) {
    uint32_t h = hash_u32(key);
    uint32_t mask = (uint32_t)(m->cap - 1);

    for (uint32_t i = 0; i < m->cap; i++) {
        uint32_t idx = (h + i) & mask;
        if (m->tab[idx].used == 0) return NULL;
        if (m->tab[idx].used == 1 && m->tab[idx].key == key) return m->tab[idx].val;
    }
    return NULL;
}

static void qpmap_del(struct qpmap *m, uint32_t key) {
    uint32_t h = hash_u32(key);
    uint32_t mask = (uint32_t)(m->cap - 1);

    for (uint32_t i = 0; i < m->cap; i++) {
        uint32_t idx = (h + i) & mask;
        if (m->tab[idx].used == 0) return;
        if (m->tab[idx].used == 1 && m->tab[idx].key == key) {
            m->tab[idx].used = 2;
            m->tab[idx].val = NULL;
            return;
        }
    }
}

// ============================ Send buffer pool ============================

enum wrid_type {
    WRID_SEND = 1
};

static inline uint64_t WRID_MAKE(uint32_t type, uint32_t idx) {
    return ((uint64_t)type << 32) | (uint64_t)idx;
}
static inline uint32_t WRID_TYPE(uint64_t wrid) { return (uint32_t)(wrid >> 32); }
static inline uint32_t WRID_IDX(uint64_t wrid) { return (uint32_t)(wrid & 0xffffffffu); }

// ============================ Per-connection context ============================

struct pending_put {
    int active;
    uint32_t token;
    uint32_t version;

    uint8_t key[KEY_MAX];
    uint8_t key_len;

    int start;
    int nchunks;
    uint32_t value_len;
};

struct conn_ctx {
    struct rdma_cm_id *id;
    uint32_t qp_num;
    uint32_t next_token;
    struct pending_put put;
};

// ============================ Global server context ============================

struct global_ctx {
    int inited;

    struct ibv_context *verbs;
    struct ibv_pd *pd;

    struct ibv_comp_channel *comp_ch;
    struct ibv_cq *cq;

    struct ibv_srq *srq;

    // SRQ recv region
    uint8_t *recv_region;
    size_t recv_region_bytes;
    struct ibv_mr *recv_mr;
    // Experiment C knobs: receiver resources (SRQ pre-post depth) and repost behavior
    uint32_t recv_depth;           // number of SRQ RECV WQEs to pre-post
    int repost_recv;              // 1 = repost on each RECV completion (normal); 0 = leak to reproduce RNR
    uint64_t recv_posted;         // total posted to SRQ (for logging only)
    uint64_t recv_reposted;       // number of reposts performed

    // Send pool region
    uint8_t *send_region;
    size_t send_region_bytes;
    struct ibv_mr *send_mr;
    int send_free[SEND_POOL_SZ];
    int send_top;

    // Memory pool for values
    struct pool p;

    // KV state
    struct kv_table kv;
    uint32_t version_ctr;

    // QP map
    struct qpmap qpm;
};

static int sendbuf_alloc(struct global_ctx *g, struct kv_msg **out) {
    if (g->send_top == 0) return -1;
    int idx = g->send_free[--g->send_top];
    *out = (struct kv_msg *)(g->send_region + (size_t)idx * sizeof(struct kv_msg));
    return idx;
}

static void sendbuf_free(struct global_ctx *g, int idx) {
    if (idx < 0 || idx >= SEND_POOL_SZ) return;
    g->send_free[g->send_top++] = idx;
}

// ============================ SRQ receive repost ============================

static void post_one_srq_recv(struct global_ctx *g, uint32_t idx) {
    uint8_t *buf = g->recv_region + (size_t)idx * RECV_BUF_SZ;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = RECV_BUF_SZ;
    sge.lkey   = g->recv_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = (uintptr_t)idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_srq_recv(g->srq, &wr, &bad)) die("ibv_post_srq_recv");
}

// ============================ Initialization ============================

static void init_global_resources(struct global_ctx *g, struct ibv_context *verbs) {
    g->verbs = verbs;

    // Receiver resource knobs (Experiment C).
    if (g->recv_depth == 0) g->recv_depth = DEFAULT_RECV_DEPTH;
    if (g->recv_depth > SRQ_MAX_WR) {
        fprintf(stderr, "recv_depth=%u exceeds SRQ_MAX_WR=%d\n", g->recv_depth, SRQ_MAX_WR);
        exit(2);
    }
    if (g->repost_recv == 0) {
        printf("[server] WARNING: repost disabled; SRQ WQEs will leak and peers may hit RNR / hang.\n");
    }

    g->pd = ibv_alloc_pd(g->verbs);
    if (!g->pd) die("ibv_alloc_pd");

    // Completion channel + CQ (event-driven)
    g->comp_ch = ibv_create_comp_channel(g->verbs);
    if (!g->comp_ch) die("ibv_create_comp_channel");

    g->cq = ibv_create_cq(g->verbs, CQ_CAP, NULL, g->comp_ch, 0);
    if (!g->cq) die("ibv_create_cq");

    if (ibv_req_notify_cq(g->cq, 0)) die("ibv_req_notify_cq");

    // SRQ
    struct ibv_srq_init_attr sattr;
    memset(&sattr, 0, sizeof(sattr));
    sattr.attr.max_wr  = g->recv_depth;
    sattr.attr.max_sge = SRQ_MAX_SGE;

    g->srq = ibv_create_srq(g->pd, &sattr);
    if (!g->srq) die("ibv_create_srq");

    // Value pool
    g->p.base = (char *)xmalloc_aligned(4096, POOL_SIZE);
    if (!g->p.base) die("posix_memalign pool");

    int pool_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    g->p.mr = ibv_reg_mr(g->pd, g->p.base, POOL_SIZE, pool_flags);
    if (!g->p.mr) die("ibv_reg_mr pool");

    memset(g->p.used, 0, sizeof(g->p.used));

    // SRQ recv region
    g->recv_region_bytes = (size_t)g->recv_depth * RECV_BUF_SZ;
    size_t rbytes = (g->recv_region_bytes + 4095) & ~((size_t)4095);
    g->recv_region = (uint8_t *)xmalloc_aligned(4096, rbytes);
    if (!g->recv_region) die("posix_memalign recv_region");

    g->recv_mr = ibv_reg_mr(g->pd, g->recv_region, rbytes, IBV_ACCESS_LOCAL_WRITE);
    if (!g->recv_mr) die("ibv_reg_mr recv_mr");

    for (uint32_t i = 0; i < g->recv_depth; i++) {
        post_one_srq_recv(g, i);
        g->recv_posted++;
    }

    // Send buffer pool region
    g->send_region_bytes = (size_t)SEND_POOL_SZ * sizeof(struct kv_msg);
    size_t sbytes = (g->send_region_bytes + 4095) & ~((size_t)4095);
    g->send_region = (uint8_t *)xmalloc_aligned(4096, sbytes);
    if (!g->send_region) die("posix_memalign send_region");

    g->send_mr = ibv_reg_mr(g->pd, g->send_region, sbytes, IBV_ACCESS_LOCAL_WRITE);
    if (!g->send_mr) die("ibv_reg_mr send_mr");

    g->send_top = 0;
    for (int i = 0; i < SEND_POOL_SZ; i++) g->send_free[g->send_top++] = i;

    // KV state
    memset(&g->kv, 0, sizeof(g->kv));
    g->version_ctr = 1;

    // qp map
    qpmap_init(&g->qpm, QPMAP_CAP);

    g->inited = 1;

    printf("[server] global init: pool_base=0x%llx pool_size=%zu rkey=0x%x, SRQ depth=%u repost=%d, CQch fd=%d\n",
           (unsigned long long)(uintptr_t)g->p.base, (size_t)POOL_SIZE, g->p.mr->rkey,
           g->recv_depth, g->repost_recv, g->comp_ch->fd);
}

// ============================ Connection management ============================

static struct conn_ctx *create_conn_ctx(struct rdma_cm_id *id) {
    struct conn_ctx *c = calloc(1, sizeof(*c));
    if (!c) die("calloc conn_ctx");
    c->id = id;
    c->qp_num = id->qp->qp_num;
    c->next_token = 1;
    c->put.active = 0;
    return c;
}

static void destroy_conn_ctx(struct global_ctx *g, struct conn_ctx *c) {
    if (!c) return;

    // If a PUT was pending and never committed, return its allocation to the pool.
    if (c->put.active) {
        pool_free(&g->p, c->put.start, c->put.nchunks);
        c->put.active = 0;
    }

    // Destroy QP and CM ID.
    if (c->id) {
        if (c->id->qp) rdma_destroy_qp(c->id);
        rdma_destroy_id(c->id);
    }

    free(c);
}

static void on_connect_request(struct global_ctx *g, struct rdma_cm_id *id) {
    if (!g->inited) init_global_resources(g, id->verbs);

    // Create an RC QP bound to the shared SRQ and the shared CQ.
    struct ibv_qp_init_attr qattr;
    memset(&qattr, 0, sizeof(qattr));
    qattr.send_cq = g->cq;
    qattr.recv_cq = g->cq;
    qattr.srq     = g->srq;
    qattr.qp_type = IBV_QPT_RC;
    qattr.cap.max_send_wr  = 256;
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

static void on_established(struct rdma_cm_event *ev) {
    if (!ev || !ev->id || !ev->id->qp) return;
    printf("[server] ESTABLISHED qp=%u id=%p\n", ev->id->qp->qp_num, (void *)ev->id);
}

static void on_disconnected(struct global_ctx *g, struct rdma_cm_event *ev) {
    if (!ev || !ev->id || !ev->id->qp) return;

    uint32_t qp_num = ev->id->qp->qp_num;
    struct conn_ctx *c = qpmap_get(&g->qpm, qp_num);
    if (c) {
        qpmap_del(&g->qpm, qp_num);
        destroy_conn_ctx(g, c);
    }

    printf("[server] DISCONNECTED qp=%u\n", qp_num);
}

// ============================ Request handling ============================

static void post_send_reply(struct global_ctx *g, struct conn_ctx *c, struct kv_msg *reply, int send_slot_idx) {
    // Encode send_slot_idx into wr_id so we can recycle it on SEND completion.
    uint64_t wrid = WRID_MAKE(WRID_SEND, (uint32_t)send_slot_idx);

    if (rdma_post_send(c->id,
                       (void *)(uintptr_t)wrid,
                       reply, sizeof(*reply),
                       g->send_mr,
                       IBV_SEND_SIGNALED)) {
        die("rdma_post_send");
    }
}

static void handle_put_req(struct global_ctx *g, struct conn_ctx *c, const struct kv_msg *req) {
    uint32_t key_len = ntohl(req->key_len);
    uint32_t value_len = ntohl(req->value_len);

    struct kv_msg *resp = NULL;
    int sidx = sendbuf_alloc(g, &resp);
    if (sidx < 0) {
        fprintf(stderr, "[server] send pool exhausted\n");
        return;
    }
    memset(resp, 0, sizeof(*resp));
    kv_fill_hdr(resp, KV_PUT_RESP);

    if (key_len == 0 || key_len > KEY_MAX) {
        resp->status = htonl((uint32_t)EINVAL);
        post_send_reply(g, c, resp, sidx);
        return;
    }
    if (value_len == 0 || value_len > (uint32_t)POOL_SIZE) {
        resp->status = htonl((uint32_t)EINVAL);
        post_send_reply(g, c, resp, sidx);
        return;
    }
    if (c->put.active) {
        // Keep the demo simple: only one outstanding PUT per connection.
        resp->status = htonl((uint32_t)EBUSY);
        post_send_reply(g, c, resp, sidx);
        return;
    }

    int start = -1, nchunks = 0;
    int rc = pool_alloc(&g->p, (size_t)value_len, &start, &nchunks);
    if (rc != 0) {
        resp->status = htonl((uint32_t)(-rc));
        post_send_reply(g, c, resp, sidx);
        return;
    }

    // Record pending PUT until we receive the WRITE_WITH_IMM commit.
    c->put.active = 1;
    c->put.token = c->next_token++;
    c->put.version = g->version_ctr++;
    c->put.key_len = (uint8_t)key_len;
    memset(c->put.key, 0, KEY_MAX);
    memcpy(c->put.key, req->key, key_len);
    c->put.start = start;
    c->put.nchunks = nchunks;
    c->put.value_len = value_len;

    uint64_t remote_addr = pool_remote_addr(&g->p, start);
    uint32_t alloc_len = (uint32_t)((size_t)nchunks * CHUNK_SIZE);

    resp->status = htonl(0);
    resp->key_len = htonl(key_len);
    resp->value_len = htonl(alloc_len);             // granted size (rounded up)
    resp->token = htonl(c->put.token);
    resp->version = htonl(c->put.version);
    resp->addr = htonll_u64(remote_addr);
    resp->rkey = htonl(g->p.mr->rkey);
    memcpy(resp->key, req->key, key_len);

    printf("[server][qp %u] PUT_REQ key_len=%u value_len=%u -> start=%d nchunks=%d addr=0x%llx rkey=0x%x token=%u ver=%u\n",
           c->qp_num, key_len, value_len, start, nchunks,
           (unsigned long long)remote_addr, g->p.mr->rkey, c->put.token, c->put.version);

    post_send_reply(g, c, resp, sidx);
}

static void handle_get_req(struct global_ctx *g, struct conn_ctx *c, const struct kv_msg *req) {
    uint32_t key_len = ntohl(req->key_len);

    struct kv_msg *resp = NULL;
    int sidx = sendbuf_alloc(g, &resp);
    if (sidx < 0) {
        fprintf(stderr, "[server] send pool exhausted\n");
        return;
    }
    memset(resp, 0, sizeof(*resp));
    kv_fill_hdr(resp, KV_GET_RESP);

    if (key_len == 0 || key_len > KEY_MAX) {
        resp->status = htonl((uint32_t)EINVAL);
        post_send_reply(g, c, resp, sidx);
        return;
    }

    struct kv_entry *e = kv_get(&g->kv, req->key, (uint8_t)key_len);
    if (!e) {
        resp->status = htonl((uint32_t)ENOENT);
        resp->key_len = htonl(key_len);
        memcpy(resp->key, req->key, key_len);
        post_send_reply(g, c, resp, sidx);
        return;
    }

    uint64_t remote_addr = pool_remote_addr(&g->p, e->start);

    resp->status = htonl(0);
    resp->key_len = htonl(key_len);
    resp->value_len = htonl(e->value_len);
    resp->version = htonl(e->version);
    resp->addr = htonll_u64(remote_addr);
    resp->rkey = htonl(g->p.mr->rkey);
    memcpy(resp->key, req->key, key_len);

    printf("[server][qp %u] GET_REQ key_len=%u -> addr=0x%llx len=%u rkey=0x%x ver=%u\n",
           c->qp_num, key_len, (unsigned long long)remote_addr, e->value_len, g->p.mr->rkey, e->version);

    post_send_reply(g, c, resp, sidx);
}


static void handle_put_commit(struct global_ctx *g, struct conn_ctx *c, uint32_t token_host_order) {
    if (!c->put.active) {
        printf("[server][qp %u] COMMIT token=%u but no pending PUT\n", c->qp_num, token_host_order);
        return;
    }
    if (c->put.token != token_host_order) {
        printf("[server][qp %u] COMMIT token=%u mismatch (pending=%u)\n",
               c->qp_num, token_host_order, c->put.token);
        return;
    }

   struct kv_prev_slice prev;
   struct kv_entry *e = kv_upsert(&g->kv,
                                  c->put.key, c->put.key_len,
                                  c->put.start, c->put.nchunks,
                                  c->put.value_len, c->put.version,
                                  &prev);
   
   if (!e) {
       /* KV table full: rollback allocation (best effort). */
       pool_free(&g->p, c->put.start, c->put.nchunks);
       c->put.active = 0;
       fprintf(stderr, "[server] KV table full; dropping key\n");
       return;
   }
   
   /* If we replaced an old value, free its slice back to the pool. */
   if (prev.valid) {
       pool_free(&g->p, prev.start, prev.nchunks);
   }

    printf("[server][qp %u] PUT_COMMIT token=%u key_len=%u start=%d nchunks=%d len=%u ver=%u\n",
           c->qp_num, token_host_order, (unsigned)c->put.key_len,
           c->put.start, c->put.nchunks, c->put.value_len, c->put.version);

    c->put.active = 0;
}

// ============================ CQ handling ============================

static void drain_cq(struct global_ctx *g) {
    struct ibv_wc wcs[64];

    while (1) {
        int n = ibv_poll_cq(g->cq, (int)ARRAY_SIZE(wcs), wcs);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) break;

        for (int i = 0; i < n; i++) {
            struct ibv_wc *wc = &wcs[i];


            /*
             * IMPORTANT:
             * Even on errors (e.g., WR_FLUSH_ERR during disconnect), we must still
             * perform resource bookkeeping:
             *   - recycle send buffers on SEND completions
             *   - repost SRQ receive slots on RECV completions
             * Otherwise we can silently "leak" SRQ WQEs and eventually hit RNR,
             * which makes the peer appear to hang forever.
             */
            int ok = (wc->status == IBV_WC_SUCCESS);

            if (!ok) {
                fprintf(stderr, "[server] WC error status=%s(%d) opcode=%d qp=%u\n",
                        ibv_wc_status_str(wc->status), wc->status, wc->opcode, wc->qp_num);
						 
            }

            if (wc->opcode == IBV_WC_SEND) {
                uint64_t wrid = (uint64_t)(uintptr_t)wc->wr_id;
                if (WRID_TYPE(wrid) == WRID_SEND) {
                    sendbuf_free(g, (int)WRID_IDX(wrid));
                }
                continue;
            }

            if (wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                uint32_t ridx = (uint32_t)wc->wr_id;
                if (ridx >= g->recv_depth) {
                    fprintf(stderr, "[server] bad recv wr_id idx=%u\n", ridx);
                    continue;
                }

                /*
                 * Always repost the SRQ slot, even if the WC is an error.
                 * (e.g., WR_FLUSH_ERR when a connection is destroyed)
                 */
                if (!ok) {
                    if (g->repost_recv) { post_one_srq_recv(g, ridx); g->recv_reposted++; }
                    continue;
                }
                struct conn_ctx *c = qpmap_get(&g->qpm, wc->qp_num);
                if (!c) {
                    // The connection might have been removed; just repost the buffer.
                    if (g->repost_recv) { post_one_srq_recv(g, ridx); g->recv_reposted++; }
                    continue;
                }

                uint8_t *buf = g->recv_region + (size_t)ridx * RECV_BUF_SZ;

                if (wc->opcode == IBV_WC_RECV) {
                    struct kv_msg *m = (struct kv_msg *)buf;
                    uint16_t type = ntohs(m->type);

                    if (type == KV_PUT_REQ && kv_check_hdr(m, KV_PUT_REQ) == 0) {
                        handle_put_req(g, c, m);
                    } else if (type == KV_GET_REQ && kv_check_hdr(m, KV_GET_REQ) == 0) {
                        handle_get_req(g, c, m);
                    } else {
                        fprintf(stderr, "[server][qp %u] unexpected RECV type=%u\n", c->qp_num, (unsigned)type);
                    }
                } else {
                    if (wc->wc_flags & IBV_WC_WITH_IMM) {
                        uint32_t token = ntohl(wc->imm_data);
                        handle_put_commit(g, c, token);
                    }
                }

                // Repost SRQ receive buffer slot for reuse.
                if (g->repost_recv) { post_one_srq_recv(g, ridx); g->recv_reposted++; }
            }
        }
    }
}

static void handle_cq_event(struct global_ctx *g) {
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;

    if (ibv_get_cq_event(g->comp_ch, &ev_cq, &ev_ctx)) die("ibv_get_cq_event");
    ibv_ack_cq_events(ev_cq, 1);

    // Re-arm CQ notifications before draining to reduce missed-edge windows.
    if (ibv_req_notify_cq(ev_cq, 0)) die("ibv_req_notify_cq");

    drain_cq(g);
}

// ============================ CM event handling ============================

static void handle_cm_event(struct global_ctx *g, struct rdma_cm_event *ev) {
    switch (ev->event) {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        on_connect_request(g, ev->id);
        break;
    case RDMA_CM_EVENT_ESTABLISHED:
        on_established(ev);
        break;
    case RDMA_CM_EVENT_DISCONNECTED:
        on_disconnected(g, ev);
        break;
    default:
        fprintf(stderr, "[server] CM event: %s (%d)\n", rdma_event_str(ev->event), ev->event);
        break;
    }
}

// ============================ Main ============================

int main(int argc, char **argv) {
    int port = LISTEN_PORT_DEFAULT;
    int opt_start = 1;

    // If the first argument is not an option (doesn't start with '-'), treat it as a port.
    if (argc >= 2 && argv[1][0] != '-') {
        port = atoi(argv[1]);
        opt_start = 2;
    }
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Usage: %s [listen_port] [--recv-depth N] [--no-repost]\n", argv[0]);
        return 2;
    }

    struct global_ctx g;
    memset(&g, 0, sizeof(g));

    // Defaults for Experiment C knobs.
    g.recv_depth  = DEFAULT_RECV_DEPTH;
    g.repost_recv = 1;

    // Optional flags: --recv-depth N, --no-repost
    for (int i = opt_start; i < argc; i++) {
        if (strcmp(argv[i], "--recv-depth") == 0 && i + 1 < argc) {
            g.recv_depth = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no-repost") == 0) {
            g.repost_recv = 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            fprintf(stderr, "Usage: %s [listen_port] [--recv-depth N] [--no-repost]\n", argv[0]);
            return 2;
        }
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (rdma_bind_addr(listen_id, (struct sockaddr *)&addr)) die("rdma_bind_addr");
    if (rdma_listen(listen_id, BACKLOG)) die("rdma_listen");

    printf("[server] listening on 0.0.0.0:%d ...\n", port);

    // Event loop: wait for either CM events (ec->fd) or CQ events (comp_ch->fd).
    while (1) {
        struct pollfd fds[2];
        nfds_t nfds = 0;

        fds[nfds].fd = ec->fd;
        fds[nfds].events = POLLIN;
        nfds++;

        if (g.inited) {
            fds[nfds].fd = g.comp_ch->fd;
            fds[nfds].events = POLLIN;
            nfds++;
        }

        int rc = poll(fds, nfds, -1);
        if (rc < 0) {
            if (errno == EINTR) continue;
            die("poll");
        }

        if (fds[0].revents & POLLIN) {
            /*
             * IMPORTANT (librdmacm lifetime rule-of-thumb):
             * A CM event must be ACKed before we destroy the rdma_cm_id or its QP.
             *
             * If we destroy an id/QP inside the event handler *before* rdma_ack_cm_event(),
             * some providers will stop delivering further CM events (new connections appear
             * to "hang"), because the event object still references internal resources.
             *
             * To avoid this class of bug, copy the event, ACK it immediately, then handle
             * the copy. The handler must not keep pointers to the original event object.
             */
            struct rdma_cm_event *ev = NULL;
            if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");

            struct rdma_cm_event ev_copy = *ev;
            rdma_ack_cm_event(ev);

            handle_cm_event(&g, &ev_copy);        }

        if (g.inited && nfds >= 2 && (fds[1].revents & POLLIN)) {
            handle_cq_event(&g);
        }
    }

    return 0;
}