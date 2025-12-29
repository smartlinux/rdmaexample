#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * cm_pingpong_client_inline_v2.c
 *
 * Fixes v1's "opaque" ADDR_ERROR by:
 * - printing ev->status for unexpected cm events
 * - calling rdma_resolve_addr(id, res->ai_src_addr, res->ai_dst_addr, ...)
 *   (this follows the pattern used in rdma-core's examples and helps when
 *   the kernel must bind the cm_id to a specific local address).
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 cm_pingpong_client_inline_v2.c -o cm_pp_client_v2 -lrdmacm -libverbs
 *
 * Run:
 *   ./cm_pp_client_v2 <server_ip> <port> [iters] [print_every] [local_ip]
 *
 * Notes:
 * - local_ip is optional. If you have multiple NICs / routes, specifying local_ip
 *   often fixes RDMA_CM_EVENT_ADDR_ERROR.
 */

#define MSG_SIZE 64
#define CQ_DEPTH 256
#define TIMEOUT_MS 2000

static void die(const char *what) {
    perror(what);
    exit(1);
}

static void die_gai(const char *what, int rc) {
    fprintf(stderr, "%s: %s\n", what, gai_strerror(rc));
    exit(1);
}

static void require_inline(struct ibv_qp *qp, uint32_t need) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init;
    memset(&attr, 0, sizeof(attr));
    memset(&init, 0, sizeof(init));
    if (ibv_query_qp(qp, &attr, IBV_QP_CAP, &init)) die("ibv_query_qp");
    if (attr.cap.max_inline_data < need) {
        fprintf(stderr, "QP max_inline_data=%u < need=%u\n", attr.cap.max_inline_data, need);
        exit(1);
    }
}

static void post_one_recv(struct ibv_qp *qp, void *buf, struct ibv_mr *mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = MSG_SIZE;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(qp, &wr, &bad)) die("ibv_post_recv");
}

static void post_inline_send(struct ibv_qp *qp, const void *buf, uint32_t len) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = len;
    sge.lkey = 0;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_INLINE;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send");
}

static void poll_one_recv(struct ibv_cq *cq) {
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
        if (wc.opcode == IBV_WC_RECV) return;
    }
}

static void print_cm_mismatch(enum rdma_cm_event_type want,
                             enum rdma_cm_event_type got,
                             int status) {
    fprintf(stderr, "Expected cm event %s, got %s (status=%d",
            rdma_event_str(want), rdma_event_str(got), status);
    if (status) fprintf(stderr, " errno=%d '%s'", -status, strerror(-status));
    fprintf(stderr, ")\n");
}

static void wait_for_event(struct rdma_event_channel *ec, enum rdma_cm_event_type want) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    enum rdma_cm_event_type got = ev->event;
    int st = ev->status;
    rdma_ack_cm_event(ev);
    if (got != want) {
        print_cm_mismatch(want, got, st);
        exit(1);
    }
}

struct client_ctx {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    void *recv_buf;
    struct ibv_mr *recv_mr;
};

static void client_destroy(struct client_ctx *c) {
    if (!c) return;
    if (c->id && c->id->qp) rdma_destroy_qp(c->id);
    if (c->recv_mr) ibv_dereg_mr(c->recv_mr);
    if (c->recv_buf) free(c->recv_buf);
    if (c->cq) ibv_destroy_cq(c->cq);
    if (c->pd) ibv_dealloc_pd(c->pd);
    if (c->id) rdma_destroy_id(c->id);
    if (c->ec) rdma_destroy_event_channel(c->ec);
    memset(c, 0, sizeof(*c));
}

static void setup_qp(struct client_ctx *c) {
    c->pd = ibv_alloc_pd(c->id->verbs);
    if (!c->pd) die("ibv_alloc_pd");

    c->cq = ibv_create_cq(c->id->verbs, CQ_DEPTH, NULL, NULL, 0);
    if (!c->cq) die("ibv_create_cq");

    if (posix_memalign(&c->recv_buf, 4096, MSG_SIZE)) die("posix_memalign");
    memset(c->recv_buf, 0, MSG_SIZE);

    c->recv_mr = ibv_reg_mr(c->pd, c->recv_buf, MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!c->recv_mr) die("ibv_reg_mr");

    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));
    qia.qp_type = IBV_QPT_RC;
    qia.send_cq = c->cq;
    qia.recv_cq = c->cq;
    qia.cap.max_send_wr = 128;
    qia.cap.max_recv_wr = 128;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;
    qia.cap.max_inline_data = MSG_SIZE;

    if (rdma_create_qp(c->id, c->pd, &qia)) die("rdma_create_qp");
    require_inline(c->id->qp, MSG_SIZE);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <server_ip> <port> [iters] [print_every] [local_ip]\n", argv[0]);
        return 2;
    }

    const char *server = argv[1];
    const char *port = argv[2];
    int iters = (argc >= 4) ? atoi(argv[3]) : 1000;
    int print_every = (argc >= 5) ? atoi(argv[4]) : 100;
    const char *local_ip = (argc >= 6) ? argv[5] : NULL;

    struct client_ctx c;
    memset(&c, 0, sizeof(c));

    c.ec = rdma_create_event_channel();
    if (!c.ec) die("rdma_create_event_channel");

    if (rdma_create_id(c.ec, &c.id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;

    /*
     * If local_ip is given, rdma_getaddrinfo will return an ai_src_addr that
     * binds the cm_id to that local address. This is often needed when the host
     * has multiple NICs / routes.
     */
    int rc = rdma_getaddrinfo(server, port, &hints, &res);
    if (rc) die_gai("rdma_getaddrinfo(dst)", rc);

    struct sockaddr *src = NULL;
    if (local_ip) {
        struct rdma_addrinfo *res2 = NULL;
        rc = rdma_getaddrinfo(local_ip, NULL, &hints, &res2);
        if (rc) die_gai("rdma_getaddrinfo(src)", rc);
        src = res2->ai_dst_addr; /* yes: ai_dst_addr holds the parsed sockaddr of 'local_ip' */
        /* keep res2 until after resolve_addr */
        if (rdma_resolve_addr(c.id, src, res->ai_dst_addr, TIMEOUT_MS)) die("rdma_resolve_addr");
        rdma_freeaddrinfo(res2);
    } else {
        if (rdma_resolve_addr(c.id, res->ai_src_addr, res->ai_dst_addr, TIMEOUT_MS)) die("rdma_resolve_addr");
    }
    wait_for_event(c.ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(c.id, TIMEOUT_MS)) die("rdma_resolve_route");
    wait_for_event(c.ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    setup_qp(&c);

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;

    if (rdma_connect(c.id, &cp)) die("rdma_connect");
    wait_for_event(c.ec, RDMA_CM_EVENT_ESTABLISHED);

    printf("[client] ESTABLISHED\n");

    char ping[MSG_SIZE];

    for (int i = 0; i < iters; i++) {
        post_one_recv(c.id->qp, c.recv_buf, c.recv_mr);

        snprintf(ping, sizeof(ping), "PING %d", i);
        post_inline_send(c.id->qp, ping, MSG_SIZE);

        poll_one_recv(c.cq);

        if ((i % print_every) == 0) {
            printf("[client] iter=%d recv='%s'\n", i, (char *)c.recv_buf);
        }
    }

    rdma_disconnect(c.id);
    wait_for_event(c.ec, RDMA_CM_EVENT_DISCONNECTED);

    rdma_freeaddrinfo(res);
    client_destroy(&c);
    return 0;
}
