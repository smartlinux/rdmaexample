#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include <arpa/inet.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
 * cm_ticket_lock_client.c
 *
 * Client uses a ticket lock in remote memory.
 *
 * Acquire:
 *   my = FAA(next_ticket, 1) -> returns old value (my ticket)
 *   while (READ(now_serving) != my) spin/backoff
 *
 * Critical section:
 *   RDMA_WRITE message to shared data region
 *
 * Release:
 *   FAA(now_serving, 1)
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 cm_ticket_lock_client.c -o cm_ticket_lock_client -lrdmacm -libverbs
 *
 * Run:
 *   ./cm_ticket_lock_client <server_ip> <port> [message] [hold_ms] [local_ip]
 */

#define CQ_DEPTH 256
#define TIMEOUT_MS 2000

#define NEXT_TICKET_OFF 0
#define NOW_SERVING_OFF 8

struct ctrl_msg {
    uint64_t base_addr_be;
    uint32_t rkey_be;
    uint32_t data_off_be;
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

static void post_recv_ctrl(struct ibv_qp *qp, struct ctrl_msg *buf, struct ibv_mr *mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)sizeof(*buf);
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xD011;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(qp, &wr, &bad)) die("ibv_post_recv(ctrl)");
}

static uint64_t atomic_fetch_add_u64(struct ibv_qp *qp, struct ibv_cq *cq,
                                    uint64_t remote_addr, uint32_t rkey,
                                    uint64_t add,
                                    uint64_t *out_old, struct ibv_mr *out_mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)out_old;
    sge.length = (uint32_t)sizeof(uint64_t);
    sge.lkey = out_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xA211;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = remote_addr;
    wr.wr.atomic.rkey = rkey;
    wr.wr.atomic.compare_add = add;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send(FAA)");

    poll_one(cq, IBV_WC_FETCH_ADD);
    return *out_old;
}

static uint64_t rdma_read_u64(struct ibv_qp *qp, struct ibv_cq *cq,
                             uint64_t remote_addr, uint32_t rkey,
                             uint64_t *dst, struct ibv_mr *dst_mr) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)dst;
    sge.length = (uint32_t)sizeof(uint64_t);
    sge.lkey = dst_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xB311;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send(READ)");

    poll_one(cq, IBV_WC_RDMA_READ);
    return *dst;
}

static void rdma_write_buf(struct ibv_qp *qp, struct ibv_cq *cq,
                           const void *buf, uint32_t len, struct ibv_mr *mr,
                           uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = len;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0xC411;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send(WRITE)");

    poll_one(cq, IBV_WC_RDMA_WRITE);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <server_ip> <port> [message] [hold_ms] [local_ip]\n", argv[0]);
        return 2;
    }

    const char *server_ip = argv[1];
    const char *port = argv[2];
    const char *msg_arg = (argc >= 4) ? argv[3] : "hello";
    int hold_ms = (argc >= 5) ? atoi(argv[4]) : 0;
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

    struct ibv_pd *pd = ibv_alloc_pd(id->verbs);
    if (!pd) die("ibv_alloc_pd");

    struct ibv_cq *cq = ibv_create_cq(id->verbs, CQ_DEPTH, NULL, NULL, 0);
    if (!cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));
    qia.qp_type = IBV_QPT_RC;
    qia.send_cq = cq;
    qia.recv_cq = cq;
    qia.cap.max_send_wr = 512;
    qia.cap.max_recv_wr = 64;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;

    if (rdma_create_qp(id, pd, &qia)) die("rdma_create_qp");

    struct ctrl_msg *ctrl = NULL;
    if (posix_memalign((void **)&ctrl, 4096, sizeof(*ctrl))) die("posix_memalign(ctrl)");
    memset(ctrl, 0, sizeof(*ctrl));

    struct ibv_mr *ctrl_mr = ibv_reg_mr(pd, ctrl, sizeof(*ctrl), IBV_ACCESS_LOCAL_WRITE);
    if (!ctrl_mr) die("ibv_reg_mr(ctrl)");

    post_recv_ctrl(id->qp, ctrl, ctrl_mr);

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.initiator_depth = 8;
    cp.responder_resources = 8;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;

    if (rdma_connect(id, &cp)) die("rdma_connect");
    wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED);

    poll_one(cq, IBV_WC_RECV);

    uint64_t base = ntohll_u64(ctrl->base_addr_be);
    uint32_t rkey = ntohl(ctrl->rkey_be);
    uint32_t data_off = ntohl(ctrl->data_off_be);

    uint64_t next_addr = base + NEXT_TICKET_OFF;
    uint64_t serving_addr = base + NOW_SERVING_OFF;
    uint64_t data_addr = base + data_off;

    printf("[client] connected: base=0x%016" PRIx64 " rkey=0x%x\n", base, rkey);

    uint64_t *tmp_u64 = NULL;
    if (posix_memalign((void **)&tmp_u64, 8, sizeof(uint64_t))) die("posix_memalign(tmp_u64)");
    *tmp_u64 = 0;
    struct ibv_mr *tmp_mr = ibv_reg_mr(pd, tmp_u64, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    if (!tmp_mr) die("ibv_reg_mr(tmp_u64)");

    char *msg = NULL;
    if (posix_memalign((void **)&msg, 4096, 256)) die("posix_memalign(msg)");
    memset(msg, 0, 256);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    snprintf(msg, 256, "%s pid=%d time=%ld.%09ld",
             msg_arg, getpid(), (long)ts.tv_sec, ts.tv_nsec);

    struct ibv_mr *msg_mr = ibv_reg_mr(pd, msg, 256, IBV_ACCESS_LOCAL_WRITE);
    if (!msg_mr) die("ibv_reg_mr(msg)");

    uint64_t my_ticket = atomic_fetch_add_u64(id->qp, cq, next_addr, rkey, 1, tmp_u64, tmp_mr);
    printf("[client] got ticket=%" PRIu64 "\n", my_ticket);

    int spins = 0;
    for (;;) {
        uint64_t serving = rdma_read_u64(id->qp, cq, serving_addr, rkey, tmp_u64, tmp_mr);
        if (serving == my_ticket) break;

        spins++;
        if ((spins & 0x3f) == 0) usleep(100);
    }
    printf("[client] lock acquired after %d READ polls\n", spins);

    if (hold_ms > 0) usleep((useconds_t)hold_ms * 1000);

    rdma_write_buf(id->qp, cq, msg, 256, msg_mr, data_addr, rkey);
    printf("[client] wrote data: '%s'\n", msg);

    (void)atomic_fetch_add_u64(id->qp, cq, serving_addr, rkey, 1, tmp_u64, tmp_mr);
    printf("[client] lock released (now_serving++)\n");

    rdma_disconnect(id);
    wait_cm_event(ec, RDMA_CM_EVENT_DISCONNECTED);

    ibv_dereg_mr(msg_mr);
    free(msg);

    ibv_dereg_mr(tmp_mr);
    free(tmp_u64);

    ibv_dereg_mr(ctrl_mr);
    free(ctrl);

    rdma_destroy_qp(id);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);

    if (src_ai) rdma_freeaddrinfo(src_ai);
    rdma_freeaddrinfo(dst);

    rdma_destroy_id(id);
    rdma_destroy_event_channel(ec);
    return 0;
}
