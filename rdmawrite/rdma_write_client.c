// rdma_alloc_client.c
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SERVER_PORT 7471
#define CQ_CAP 32

#define MAGIC 0x52444d41u  // 'RDMA'
#define VERSION 1

static inline uint64_t htonll_u64(uint64_t x) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(x & 0xffffffffULL)) << 32) | htonl((uint32_t)(x >> 32));
#else
    return x;
#endif
}
static inline uint64_t ntohll_u64(uint64_t x) { return htonll_u64(x); }

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

enum msg_type { REQ_ALLOC = 1, RESP_ALLOC = 2, MSG_DONE = 3 };

struct ctrl_msg {
    uint32_t magic;   // network
    uint16_t ver;     // network
    uint16_t type;    // network
    uint32_t size;    // network
    uint64_t addr;    // network
    uint32_t rkey;    // network
    uint32_t status;  // network
};

struct resources {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    char *data_buf;
    size_t data_cap;
    struct ibv_mr *data_mr;

    struct ctrl_msg *send_msg;
    struct ctrl_msg *recv_msg;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
};

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

static void poll_one(struct ibv_cq *cq) {
    struct ibv_wc wc;
    while (1) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) continue;
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC error: status=%d opcode=%d\n", wc.status, wc.opcode);
            exit(1);
        }
        return;
    }
}

static void build_qp(struct resources *r) {
    r->pd = ibv_alloc_pd(r->id->verbs);
    if (!r->pd) die("ibv_alloc_pd");

    r->cq = ibv_create_cq(r->id->verbs, CQ_CAP, NULL, NULL, 0);
    if (!r->cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = r->cq;
    qp_attr.recv_cq = r->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 32;
    qp_attr.cap.max_recv_wr = 32;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(r->id, r->pd, &qp_attr)) die("rdma_create_qp");
    r->qp = r->id->qp;
}

static void reg_memory(struct resources *r, size_t local_buf_cap) {
    r->data_cap = local_buf_cap;
    r->data_buf = (char*)xmalloc_aligned(4096, r->data_cap);
    if (!r->data_buf) die("posix_memalign data_buf");

    // 本地 data_buf 将作为 RDMA_WRITE 的源 -> NIC DMA 读它
    r->data_mr = ibv_reg_mr(r->pd, r->data_buf, r->data_cap, IBV_ACCESS_LOCAL_WRITE);
    if (!r->data_mr) die("ibv_reg_mr data_mr");

    r->send_msg = (struct ctrl_msg*)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    r->recv_msg = (struct ctrl_msg*)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    if (!r->send_msg || !r->recv_msg) die("posix_memalign ctrl_msg");

    int flags = IBV_ACCESS_LOCAL_WRITE;
    r->send_mr = ibv_reg_mr(r->pd, r->send_msg, sizeof(*r->send_msg), flags);
    r->recv_mr = ibv_reg_mr(r->pd, r->recv_msg, sizeof(*r->recv_msg), flags);
    if (!r->send_mr || !r->recv_mr) die("ibv_reg_mr ctrl_msg");
}

static void fill_hdr(struct ctrl_msg *m, uint16_t type) {
    m->magic = htonl(MAGIC);
    m->ver   = htons(VERSION);
    m->type  = htons(type);
}

static int check_hdr(const struct ctrl_msg *m, uint16_t expect_type) {
    if (ntohl(m->magic) != MAGIC) return -1;
    if (ntohs(m->ver) != VERSION) return -2;
    if (ntohs(m->type) != expect_type) return -3;
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <server_ip> <alloc_size>\n", argv[0]);
        return 1;
    }
    const char *server_ip = argv[1];
    size_t want = (size_t)strtoul(argv[2], NULL, 10);
    if (want == 0) {
        fprintf(stderr, "alloc_size must be > 0\n");
        return 1;
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct resources r;
    memset(&r, 0, sizeof(r));

    if (rdma_create_id(ec, &r.id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, server_ip, &dst.sin_addr) != 1) {
        fprintf(stderr, "inet_pton failed for ip: %s\n", server_ip);
        return 1;
    }

    if (rdma_resolve_addr(r.id, NULL, (struct sockaddr*)&dst, 2000)) die("rdma_resolve_addr");
    struct rdma_cm_event *ev;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event ADDR");
    if (ev->event != RDMA_CM_EVENT_ADDR_RESOLVED) { fprintf(stderr, "Unexpected %d\n", ev->event); return 1; }
    rdma_ack_cm_event(ev);

    if (rdma_resolve_route(r.id, 2000)) die("rdma_resolve_route");
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event ROUTE");
    if (ev->event != RDMA_CM_EVENT_ROUTE_RESOLVED) { fprintf(stderr, "Unexpected %d\n", ev->event); return 1; }
    rdma_ack_cm_event(ev);

    build_qp(&r);

    // 本地 buffer cap 至少要 >= want（因为 RDMA_WRITE 的源在本地）
    reg_memory(&r, (want + 4095) & ~((size_t)4095));

    // 先准备接收 RESP_ALLOC（避免 server send 时你没 recv）
    if (rdma_post_recv(r.id, NULL, r.recv_msg, sizeof(*r.recv_msg), r.recv_mr))
        die("rdma_post_recv (for RESP_ALLOC)");

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_connect(r.id, &param)) die("rdma_connect");

    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event EST");
    if (ev->event != RDMA_CM_EVENT_ESTABLISHED) { fprintf(stderr, "Unexpected %d\n", ev->event); return 1; }
    rdma_ack_cm_event(ev);
    printf("[client] established.\n");

    // 发送 REQ_ALLOC
    memset(r.send_msg, 0, sizeof(*r.send_msg));
    fill_hdr(r.send_msg, REQ_ALLOC);
    r.send_msg->size = htonl((uint32_t)want);

    if (rdma_post_send(r.id, NULL, r.send_msg, sizeof(*r.send_msg), r.send_mr, IBV_SEND_SIGNALED))
        die("rdma_post_send REQ_ALLOC");
    poll_one(r.cq); // send completion

    // 等 RESP_ALLOC
    poll_one(r.cq); // recv completion
    if (check_hdr(r.recv_msg, RESP_ALLOC) != 0) {
        fprintf(stderr, "[client] bad RESP_ALLOC\n");
        return 1;
    }

    uint32_t status = ntohl(r.recv_msg->status);
    if (status != 0) {
        fprintf(stderr, "[client] server alloc failed, status=%u\n", status);
        return 1;
    }

    size_t rsize = (size_t)ntohl(r.recv_msg->size);
    uint64_t raddr = ntohll_u64(r.recv_msg->addr);
    uint32_t rrkey = ntohl(r.recv_msg->rkey);

    printf("[client] got remote buf: addr=0x%llx rkey=0x%x size=%zu\n",
           (unsigned long long)raddr, rrkey, rsize);

    // 准备写入数据（写 want 字节内）
    const char *payload =
        "Hello dynamic RDMA buffer!\n"
        "Client requested server to allocate remote memory, then RDMA_WRITE into it.\n";
    size_t len = strlen(payload) + 1;
    if (len > want) len = want;
    memcpy(r.data_buf, payload, len);

    // one-sided RDMA_WRITE：本地 data_buf -> 远端 raddr
    if (rdma_post_write(r.id, NULL, r.data_buf, len, r.data_mr,
                        IBV_SEND_SIGNALED, raddr, rrkey))
        die("rdma_post_write");
    poll_one(r.cq);
    printf("[client] RDMA_WRITE completed.\n");

    // 发送 DONE
    memset(r.send_msg, 0, sizeof(*r.send_msg));
    fill_hdr(r.send_msg, MSG_DONE);

    if (rdma_post_send(r.id, NULL, r.send_msg, sizeof(*r.send_msg), r.send_mr, IBV_SEND_SIGNALED))
        die("rdma_post_send DONE");
    poll_one(r.cq);

    rdma_disconnect(r.id);
    if (rdma_get_cm_event(ec, &ev) == 0) rdma_ack_cm_event(ev);

    // cleanup
    if (r.recv_mr) ibv_dereg_mr(r.recv_mr);
    if (r.send_mr) ibv_dereg_mr(r.send_mr);
    if (r.data_mr) ibv_dereg_mr(r.data_mr);
    if (r.qp) rdma_destroy_qp(r.id);
    if (r.cq) ibv_destroy_cq(r.cq);
    if (r.pd) ibv_dealloc_pd(r.pd);

    free(r.data_buf);
    free(r.send_msg);
    free(r.recv_msg);

    rdma_destroy_id(r.id);
    rdma_destroy_event_channel(ec);

    printf("[client] done.\n");
    return 0;
}
