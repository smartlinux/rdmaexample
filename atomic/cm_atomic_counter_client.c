#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
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
 * Connect to cm_atomic_counter_server and perform RDMA Atomic Fetch-and-Add.
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 cm_atomic_counter_client.c -o cm_atomic_client -lrdmacm -libverbs
 *
 * Run:
 *   ./cm_atomic_client <server_ip> <port> [iters] [add] [local_ip]
 */

#define CQ_DEPTH 256
#define TIMEOUT_MS 2000

struct ctrl_msg {
    uint64_t addr_be;
    uint32_t rkey_be;
    uint32_t _pad;
};

static void die(const char *what) {
    perror(what);
    exit(1);
}

static void die_gai(const char *what, int rc) {
    fprintf(stderr, "%s: %s\n", what, gai_strerror(rc));
    exit(1);
}

static uint64_t ntohll_u64(uint64_t x) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)ntohl((uint32_t)(x & 0xffffffffULL)) << 32) | ntohl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static void poll_one(struct ibv_cq *cq, enum ibv_wc_opcode want) {
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
        if (want == 0 || wc.opcode == want) return;
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
    wr.wr_id = 0xC101;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(qp, &wr, &bad)) die("ibv_post_recv(ctrl)");
}

static void wait_cm_event(struct rdma_event_channel *ec, enum rdma_cm_event_type want) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    enum rdma_cm_event_type got = ev->event;
    int st = ev->status;
    rdma_ack_cm_event(ev);
    if (got != want) {
        fprintf(stderr, "Expected %s got %s (status=%d errno=%d '%s')\n",
                rdma_event_str(want), rdma_event_str(got), st,
                st ? -st : 0, st ? strerror(-st) : "OK");
        exit(1);
    }
}

static void query_atomic_caps(struct ibv_context *ctx) {
    struct ibv_device_attr attr;
    memset(&attr, 0, sizeof(attr));
    if (ibv_query_device(ctx, &attr)) die("ibv_query_device");
    printf("[client] max_qp_rd_atom=%u max_qp_init_rd_atom=%u atomic_cap=%d\n",
           attr.max_qp_rd_atom, attr.max_qp_init_rd_atom, attr.atomic_cap);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <server_ip> <port> [iters] [add] [local_ip]\n", argv[0]);
        return 2;
    }

    const char *server_ip = argv[1];
    const char *port = argv[2];
    int iters = (argc >= 4) ? atoi(argv[3]) : 16;
    uint64_t add = (argc >= 5) ? (uint64_t)strtoull(argv[4], NULL, 0) : 1;
    const char *local_ip = (argc >= 6) ? argv[5] : NULL;

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
    struct sockaddr *src = dst->ai_src_addr;
    if (local_ip) {
        rc = rdma_getaddrinfo(local_ip, NULL, &hints, &src_ai);
        if (rc) die_gai("rdma_getaddrinfo(src)", rc);
        src = src_ai->ai_dst_addr;
    }

    if (rdma_resolve_addr(id, src, dst->ai_dst_addr, TIMEOUT_MS)) die("rdma_resolve_addr");
    wait_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(id, TIMEOUT_MS)) die("rdma_resolve_route");
    wait_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    query_atomic_caps(id->verbs);

    struct ibv_pd *pd = ibv_alloc_pd(id->verbs);
    if (!pd) die("ibv_alloc_pd");

    struct ibv_cq *cq = ibv_create_cq(id->verbs, CQ_DEPTH, NULL, NULL, 0);
    if (!cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));
    qia.qp_type = IBV_QPT_RC;
    qia.send_cq = cq;
    qia.recv_cq = cq;
    qia.cap.max_send_wr = 128;
    qia.cap.max_recv_wr = 128;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;

    if (rdma_create_qp(id, pd, &qia)) die("rdma_create_qp");

    struct ctrl_msg *recv_msg = NULL;
    if (posix_memalign((void **)&recv_msg, 4096, sizeof(*recv_msg))) die("posix_memalign(recv_msg)");
    memset(recv_msg, 0, sizeof(*recv_msg));

    struct ibv_mr *recv_mr = ibv_reg_mr(pd, recv_msg, sizeof(*recv_msg), IBV_ACCESS_LOCAL_WRITE);
    if (!recv_mr) die("ibv_reg_mr(recv_msg)");

    post_recv_ctrl(id->qp, recv_msg, recv_mr);

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.initiator_depth = 4;
    cp.responder_resources = 4;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;

    if (rdma_connect(id, &cp)) die("rdma_connect");
    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED);
    printf("[client] ESTABLISHED\n");

    poll_one(cq, IBV_WC_RECV);

    uint64_t remote_addr = ntohll_u64(recv_msg->addr_be);
    uint32_t remote_rkey = ntohl(recv_msg->rkey_be);
    printf("[client] got counter info: addr=0x%016" PRIx64 " rkey=0x%x\n", remote_addr, remote_rkey);

    uint64_t *result = NULL;
    if (posix_memalign((void **)&result, 8, sizeof(uint64_t))) die("posix_memalign(result)");
    *result = 0;

    struct ibv_mr *result_mr = ibv_reg_mr(pd, result, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    if (!result_mr) die("ibv_reg_mr(result)");

    for (int i = 0; i < iters; i++) {
        struct ibv_sge sge;
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)result;
        sge.length = (uint32_t)sizeof(uint64_t);
        sge.lkey = result_mr->lkey;

        struct ibv_send_wr wr;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = 0xA000 + (uint64_t)i;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = remote_addr;
        wr.wr.atomic.rkey = remote_rkey;
        wr.wr.atomic.compare_add = add;

        struct ibv_send_wr *bad = NULL;
        if (ibv_post_send(id->qp, &wr, &bad)) die("ibv_post_send(atomic_faa)");

        poll_one(cq, IBV_WC_FETCH_ADD);

        uint64_t old = *result;
        printf("[client] FAA add=%" PRIu64 " -> old=%" PRIu64 " new=%" PRIu64 "\n", add, old, old + add);
    }

    rdma_disconnect(id);
    wait_cm_event(ec, RDMA_CM_EVENT_DISCONNECTED);
    printf("[client] DISCONNECTED\n");

    ibv_dereg_mr(result_mr);
    free(result);

    ibv_dereg_mr(recv_mr);
    free(recv_msg);

    rdma_destroy_qp(id);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    if (src_ai) rdma_freeaddrinfo(src_ai);
    rdma_freeaddrinfo(dst);
    rdma_destroy_id(id);
    rdma_destroy_event_channel(ec);
    return 0;
}
