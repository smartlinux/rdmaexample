#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * cm_atomic_lock_server.c
 *
 * Multi-client RDMA CM server that exposes a single shared MR:
 *   offset 0:  uint64_t lock_word (0 = unlocked, 1 = locked)
 *   offset 8:  char data[...]
 *
 * Clients acquire the lock via RDMA Atomic Compare-and-Swap (CAS),
 * then write a message into data via RDMA WRITE, then release the lock.
 *
 * This demo is intentionally "one-sided": the server does not receive
 * any application messages after sending the control-plane info.
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 cm_atomic_lock_server.c -o cm_atomic_lock_server -lrdmacm -libverbs
 *
 * Run:
 *   ./cm_atomic_lock_server <port> [bind_ipv4]
 */

#define CQ_DEPTH 256
#define MAX_BACKLOG 32
#define SHARED_MR_SIZE 4096
#define DATA_OFFSET 8
#define DATA_MAX (SHARED_MR_SIZE - DATA_OFFSET)

struct ctrl_msg {
    uint64_t base_addr_be; /* big-endian */
    uint32_t rkey_be;      /* big-endian */
    uint32_t data_off_be;  /* big-endian */
};

struct conn_ctx {
    struct rdma_cm_id *id;
    struct ibv_pd *pd; /* shared PD */
    struct ibv_cq *cq; /* per-connection CQ (could be shared, but simpler here) */
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    struct ctrl_msg *send_msg;
    struct ctrl_msg *recv_msg;
    struct conn_ctx *next;
};

struct global_ctx {
    int inited;
    struct ibv_context *verbs;
    struct ibv_pd *pd;
    void *mr_buf;
    struct ibv_mr *mr;
};

static void die(const char *what) {
    perror(what);
    exit(1);
}

static void die_gai(const char *what, int rc) {
    fprintf(stderr, "%s: %s\n", what, gai_strerror(rc));
    exit(1);
}

static uint64_t htonll_u64(uint64_t x) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(x & 0xffffffffULL)) << 32) | htonl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static void poll_send_done(struct ibv_cq *cq) {
    for (;;) {
        struct ibv_wc wc;
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) continue;

        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC error: %s (%d) opcode=%d\n",
                    ibv_wc_status_str(wc.status), wc.status, wc.opcode);
            exit(1);
        }
        if (wc.opcode == IBV_WC_SEND) return;
    }
}

static void post_recv_ctrl(struct ibv_qp *qp, struct ctrl_msg *buf, struct ibv_mr *mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)sizeof(*buf);
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xC001;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(qp, &wr, &bad)) die("ibv_post_recv(ctrl)");
}

static void post_send_ctrl(struct ibv_qp *qp, struct ctrl_msg *buf, struct ibv_mr *mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)sizeof(*buf);
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xC002;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send(ctrl)");
}

static struct conn_ctx *conn_find(struct conn_ctx *head, struct rdma_cm_id *id) {
    for (struct conn_ctx *c = head; c; c = c->next) {
        if (c->id == id) return c;
    }
    return NULL;
}

static void conn_add(struct conn_ctx **head, struct conn_ctx *c) {
    c->next = *head;
    *head = c;
}

static void conn_remove(struct conn_ctx **head, struct conn_ctx *c) {
    struct conn_ctx **pp = head;
    while (*pp) {
        if (*pp == c) {
            *pp = c->next;
            return;
        }
        pp = &(*pp)->next;
    }
}

static void global_init_if_needed(struct global_ctx *g, struct ibv_context *verbs) {
    if (g->inited) return;

    g->verbs = verbs;
    g->pd = ibv_alloc_pd(verbs);
    if (!g->pd) die("ibv_alloc_pd");

    if (posix_memalign(&g->mr_buf, 4096, SHARED_MR_SIZE)) die("posix_memalign(mr_buf)");
    memset(g->mr_buf, 0, SHARED_MR_SIZE);

    int access = IBV_ACCESS_LOCAL_WRITE |
                 IBV_ACCESS_REMOTE_READ |
                 IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_ATOMIC;

    g->mr = ibv_reg_mr(g->pd, g->mr_buf, SHARED_MR_SIZE, access);
    if (!g->mr) die("ibv_reg_mr(shared_mr)");

    printf("[server] shared MR ready: base=%p size=%d rkey=0x%x lock@+0 data@+%d\n",
           g->mr_buf, SHARED_MR_SIZE, g->mr->rkey, DATA_OFFSET);

    g->inited = 1;
}

static struct conn_ctx *conn_create(struct global_ctx *g, struct rdma_cm_id *id) {
    struct conn_ctx *c = calloc(1, sizeof(*c));
    if (!c) die("calloc(conn_ctx)");
    c->id = id;

    global_init_if_needed(g, id->verbs);
    c->pd = g->pd;

    c->cq = ibv_create_cq(id->verbs, CQ_DEPTH, NULL, NULL, 0);
    if (!c->cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));
    qia.qp_type = IBV_QPT_RC;
    qia.send_cq = c->cq;
    qia.recv_cq = c->cq;
    qia.cap.max_send_wr = 128;
    qia.cap.max_recv_wr = 128;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;

    if (rdma_create_qp(id, c->pd, &qia)) die("rdma_create_qp");

    if (posix_memalign((void **)&c->send_msg, 4096, sizeof(*c->send_msg))) die("posix_memalign(send_msg)");
    if (posix_memalign((void **)&c->recv_msg, 4096, sizeof(*c->recv_msg))) die("posix_memalign(recv_msg)");
    memset(c->send_msg, 0, sizeof(*c->send_msg));
    memset(c->recv_msg, 0, sizeof(*c->recv_msg));

    c->send_mr = ibv_reg_mr(c->pd, c->send_msg, sizeof(*c->send_msg), IBV_ACCESS_LOCAL_WRITE);
    c->recv_mr = ibv_reg_mr(c->pd, c->recv_msg, sizeof(*c->recv_msg), IBV_ACCESS_LOCAL_WRITE);
    if (!c->send_mr || !c->recv_mr) die("ibv_reg_mr(ctrl)");

    post_recv_ctrl(id->qp, c->recv_msg, c->recv_mr);

    return c;
}

static void conn_destroy(struct conn_ctx *c) {
    if (!c) return;

    if (c->id && c->id->qp) rdma_destroy_qp(c->id);

    if (c->send_mr) ibv_dereg_mr(c->send_mr);
    if (c->recv_mr) ibv_dereg_mr(c->recv_mr);
    free(c->send_msg);
    free(c->recv_msg);

    if (c->cq) ibv_destroy_cq(c->cq);

    if (c->id) rdma_destroy_id(c->id);
    free(c);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port> [bind_ipv4]\n", argv[0]);
        return 2;
    }

    const char *port = argv[1];
    const char *bind_ip = (argc >= 3) ? argv[2] : NULL;

    struct global_ctx g;
    memset(&g, 0, sizeof(g));

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_family = AF_INET;
    hints.ai_flags = RAI_PASSIVE;

    int rc = rdma_getaddrinfo(bind_ip, port, &hints, &res);
    if (rc) die_gai("rdma_getaddrinfo", rc);

    if (rdma_bind_addr(listen_id, res->ai_src_addr)) die("rdma_bind_addr");
    rdma_freeaddrinfo(res);

    if (rdma_listen(listen_id, MAX_BACKLOG)) die("rdma_listen");

    printf("[server] listening on %s:%s\n", bind_ip ? bind_ip : "0.0.0.0", port);

    struct conn_ctx *conns = NULL;

    for (;;) {
        struct rdma_cm_event *ev = NULL;
        if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");

        enum rdma_cm_event_type et = ev->event;
        struct rdma_cm_id *id = ev->id;
        int st = ev->status;

        if (et == RDMA_CM_EVENT_CONNECT_REQUEST) {
            printf("[server] CONNECT_REQUEST id=%p\n", (void *)id);
            rdma_ack_cm_event(ev);

            struct conn_ctx *c = conn_create(&g, id);
            conn_add(&conns, c);

            struct rdma_conn_param cp;
            memset(&cp, 0, sizeof(cp));
            cp.initiator_depth = 8;
            cp.responder_resources = 8;
            cp.retry_count = 7;
            cp.rnr_retry_count = 7;

            if (rdma_accept(id, &cp)) die("rdma_accept");
            continue;
        }

        if (et == RDMA_CM_EVENT_ESTABLISHED) {
            rdma_ack_cm_event(ev);

            struct conn_ctx *c = conn_find(conns, id);
            if (!c) {
                fprintf(stderr, "[server] ESTABLISHED for unknown id=%p\n", (void *)id);
                continue;
            }

            uint64_t base = (uint64_t)(uintptr_t)g.mr_buf;
            c->send_msg->base_addr_be = htonll_u64(base);
            c->send_msg->rkey_be = htonl(g.mr->rkey);
            c->send_msg->data_off_be = htonl(DATA_OFFSET);

            post_send_ctrl(id->qp, c->send_msg, c->send_mr);
            poll_send_done(c->cq);

            printf("[server] ESTABLISHED id=%p sent base=0x%016" PRIx64 " rkey=0x%x\n",
                   (void *)id, base, g.mr->rkey);
            continue;
        }

        if (et == RDMA_CM_EVENT_DISCONNECTED) {
            printf("[server] DISCONNECTED id=%p status=%d\n", (void *)id, st);
            rdma_ack_cm_event(ev);

            uint64_t lockv = *(volatile uint64_t *)g.mr_buf;
            char snapshot[128];
            memset(snapshot, 0, sizeof(snapshot));
            memcpy(snapshot, (char *)g.mr_buf + DATA_OFFSET, sizeof(snapshot) - 1);
            printf("[server] shared lock=%" PRIu64 " data='%s'\n", lockv, snapshot);

            struct conn_ctx *c = conn_find(conns, id);
            if (c) {
                conn_remove(&conns, c);
                conn_destroy(c);
            } else {
                rdma_destroy_id(id);
            }
            continue;
        }

        printf("[server] cm event=%s status=%d id=%p\n", rdma_event_str(et), st, (void *)id);
        rdma_ack_cm_event(ev);
    }
    return 0;
}
