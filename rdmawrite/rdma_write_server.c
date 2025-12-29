// rdma_alloc_server.c
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

#define LISTEN_PORT 7471
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
    uint32_t size;    // network (requested/allocated size)
    uint64_t addr;    // network (remote addr in RESP)
    uint32_t rkey;    // network (remote rkey in RESP)
    uint32_t status;  // network (0 ok, else error)
};

struct resources {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct ctrl_msg *send_msg;
    struct ctrl_msg *recv_msg;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;

    // dynamically allocated remote buffer
    char *data_buf;
    size_t data_sz;
    struct ibv_mr *data_mr;
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

static void reg_ctrl_msgs(struct resources *r) {
    r->send_msg = (struct ctrl_msg*)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    r->recv_msg = (struct ctrl_msg*)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    if (!r->send_msg || !r->recv_msg) die("posix_memalign ctrl_msg");

    int flags = IBV_ACCESS_LOCAL_WRITE;
    r->send_mr = ibv_reg_mr(r->pd, r->send_msg, sizeof(*r->send_msg), flags);
    r->recv_mr = ibv_reg_mr(r->pd, r->recv_msg, sizeof(*r->recv_msg), flags);
    if (!r->send_mr || !r->recv_mr) die("ibv_reg_mr ctrl_msg");
}

static int alloc_and_reg_remote_buf(struct resources *r, size_t sz) {
    // 4KB 对齐，真实系统通常会做 pool，这里简单 malloc
    size_t aligned = (sz + 4095) & ~((size_t)4095);
    r->data_buf = (char*)xmalloc_aligned(4096, aligned);
    if (!r->data_buf) return -1;
    r->data_sz = aligned;

    int data_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    r->data_mr = ibv_reg_mr(r->pd, r->data_buf, r->data_sz, data_flags);
    if (!r->data_mr) return -2;
    return 0;
}

static void free_remote_buf(struct resources *r) {
    if (r->data_mr) { ibv_dereg_mr(r->data_mr); r->data_mr = NULL; }
    free(r->data_buf); r->data_buf = NULL;
    r->data_sz = 0;
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

int main(void) {
    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id listen");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LISTEN_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (rdma_bind_addr(listen_id, (struct sockaddr*)&addr)) die("rdma_bind_addr");
    if (rdma_listen(listen_id, 1)) die("rdma_listen");
    printf("[server] listening on 0.0.0.0:%d ...\n", LISTEN_PORT);

    struct rdma_cm_event *ev;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    if (ev->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        fprintf(stderr, "Unexpected event %d\n", ev->event);
        return 1;
    }

    struct resources r;
    memset(&r, 0, sizeof(r));
    r.id = ev->id;
    rdma_ack_cm_event(ev);

    build_qp(&r);
    reg_ctrl_msgs(&r);

    // 先准备好 RECV：我们将会先收 REQ_ALLOC，之后再收 DONE
    if (rdma_post_recv(r.id, NULL, r.recv_msg, sizeof(*r.recv_msg), r.recv_mr))
        die("rdma_post_recv (for REQ_ALLOC)");

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.rnr_retry_count = 7;

    if (rdma_accept(r.id, &param)) die("rdma_accept");

    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event established");
    if (ev->event != RDMA_CM_EVENT_ESTABLISHED) {
        fprintf(stderr, "Unexpected event %d\n", ev->event);
        return 1;
    }
    rdma_ack_cm_event(ev);
    printf("[server] established.\n");

    // 1) 等 client 的 REQ_ALLOC
    poll_one(r.cq); // recv completion
    if (check_hdr(r.recv_msg, REQ_ALLOC) != 0) {
        fprintf(stderr, "[server] bad REQ_ALLOC\n");
        return 1;
    }
    size_t req_sz = (size_t)ntohl(r.recv_msg->size);
    printf("[server] got REQ_ALLOC size=%zu\n", req_sz);

    // 2) 分配并注册 remote buffer
    int st = alloc_and_reg_remote_buf(&r, req_sz);

    // 3) 发送 RESP_ALLOC（带 addr/rkey/size/status）
    memset(r.send_msg, 0, sizeof(*r.send_msg));
    fill_hdr(r.send_msg, RESP_ALLOC);
    r.send_msg->size = htonl((uint32_t)r.data_sz);
    r.send_msg->status = htonl((st == 0) ? 0u : (uint32_t)(-st));

    if (st == 0) {
        r.send_msg->addr = htonll_u64((uint64_t)(uintptr_t)r.data_buf);
        r.send_msg->rkey = htonl(r.data_mr->rkey);
        printf("[server] allocated remote buf: addr=0x%llx rkey=0x%x size=%zu\n",
               (unsigned long long)(uintptr_t)r.data_buf, r.data_mr->rkey, r.data_sz);
    }

    if (rdma_post_send(r.id, NULL, r.send_msg, sizeof(*r.send_msg), r.send_mr, IBV_SEND_SIGNALED))
        die("rdma_post_send RESP_ALLOC");
    poll_one(r.cq); // send completion

    if (st != 0) {
        fprintf(stderr, "[server] alloc/reg failed, status=%d\n", st);
        rdma_disconnect(r.id);
        goto cleanup;
    }

    // 4) repost recv 等 DONE
    memset(r.recv_msg, 0, sizeof(*r.recv_msg));
    if (rdma_post_recv(r.id, NULL, r.recv_msg, sizeof(*r.recv_msg), r.recv_mr))
        die("rdma_post_recv (for DONE)");

    // 5) 等 DONE
    poll_one(r.cq); // recv completion
    if (check_hdr(r.recv_msg, MSG_DONE) != 0) {
        fprintf(stderr, "[server] bad DONE\n");
    } else {
        printf("[server] got DONE. remote buffer content:\n");
        printf("--------\n%.*s\n--------\n", (int)r.data_sz, r.data_buf);
    }

    rdma_disconnect(r.id);
    if (rdma_get_cm_event(ec, &ev) == 0) rdma_ack_cm_event(ev);

cleanup:
    free_remote_buf(&r);

    if (r.recv_mr) ibv_dereg_mr(r.recv_mr);
    if (r.send_mr) ibv_dereg_mr(r.send_mr);
    if (r.qp) rdma_destroy_qp(r.id);
    if (r.cq) ibv_destroy_cq(r.cq);
    if (r.pd) ibv_dealloc_pd(r.pd);

    free(r.send_msg);
    free(r.recv_msg);

    rdma_destroy_id(r.id);
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);

    printf("[server] done.\n");
    return 0;
}
