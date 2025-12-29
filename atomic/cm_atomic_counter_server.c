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
 * RDMA CM + RC QP: expose a single 64-bit counter.
 * Clients do RDMA Atomic Fetch-and-Add to allocate unique IDs.
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 cm_atomic_counter_server.c -o cm_atomic_server -lrdmacm -libverbs
 *
 * Run:
 *   ./cm_atomic_server <port> [bind_ipv4]
 */

#define CQ_DEPTH 256

struct ctrl_msg {
    uint64_t addr_be; /* big-endian */
    uint32_t rkey_be; /* big-endian */
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

static uint64_t htonll_u64(uint64_t x) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(x & 0xffffffffULL)) << 32) | htonl((uint32_t)(x >> 32));
#else
    return x;
#endif
}

static void poll_one(struct ibv_cq *cq) {
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
        return;
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

static void query_atomic_caps(struct ibv_context *ctx) {
    struct ibv_device_attr attr;
    memset(&attr, 0, sizeof(attr));
    if (ibv_query_device(ctx, &attr)) die("ibv_query_device");

    printf("[server] max_qp_rd_atom=%u max_qp_init_rd_atom=%u atomic_cap=%d\n",
           attr.max_qp_rd_atom, attr.max_qp_init_rd_atom, attr.atomic_cap);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port> [bind_ipv4]\n", argv[0]);
        return 2;
    }

    const char *port = argv[1];
    const char *bind_ip = (argc >= 3) ? argv[2] : NULL;

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) die("rdma_create_event_channel");

    struct rdma_cm_id *listen_id = NULL;
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_family = AF_INET; /* force IPv4 for RXE/VMware sanity */
    hints.ai_flags = RAI_PASSIVE;

    int rc = rdma_getaddrinfo(bind_ip, port, &hints, &res);
    if (rc) die_gai("rdma_getaddrinfo", rc);

    if (rdma_bind_addr(listen_id, res->ai_src_addr)) die("rdma_bind_addr");
    rdma_freeaddrinfo(res);

    if (rdma_listen(listen_id, 8)) die("rdma_listen");
    printf("[server] listening on %s:%s\n", bind_ip ? bind_ip : "0.0.0.0", port);

    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    if (ev->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        fprintf(stderr, "Unexpected event: %s\n", rdma_event_str(ev->event));
        rdma_ack_cm_event(ev);
        return 1;
    }

    struct rdma_cm_id *id = ev->id;
    printf("[server] CONNECT_REQUEST id=%p\n", (void *)id);
    rdma_ack_cm_event(ev);

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

    /* Remote counter MR (must be 8-byte aligned). */
    uint64_t *counter = NULL;
    if (posix_memalign((void **)&counter, 8, sizeof(uint64_t))) die("posix_memalign(counter)");
    *counter = 0;

    int access = IBV_ACCESS_LOCAL_WRITE |
                 IBV_ACCESS_REMOTE_READ |
                 IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_ATOMIC;
    struct ibv_mr *counter_mr = ibv_reg_mr(pd, counter, sizeof(uint64_t), access);
    if (!counter_mr) die("ibv_reg_mr(counter)");

    printf("[server] counter MR: addr=%p rkey=0x%x initial=%" PRIu64 "\n",
           (void *)counter, counter_mr->rkey, *counter);

    /* Control-plane buffers. */
    struct ctrl_msg *send_msg = NULL, *recv_msg = NULL;
    if (posix_memalign((void **)&send_msg, 4096, sizeof(*send_msg))) die("posix_memalign(send_msg)");
    if (posix_memalign((void **)&recv_msg, 4096, sizeof(*recv_msg))) die("posix_memalign(recv_msg)");
    memset(send_msg, 0, sizeof(*send_msg));
    memset(recv_msg, 0, sizeof(*recv_msg));

    struct ibv_mr *send_mr = ibv_reg_mr(pd, send_msg, sizeof(*send_msg), IBV_ACCESS_LOCAL_WRITE);
    struct ibv_mr *recv_mr = ibv_reg_mr(pd, recv_msg, sizeof(*recv_msg), IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr || !recv_mr) die("ibv_reg_mr(ctrl)");

    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.initiator_depth = 4;     /* READ/ATOMIC credits */
    cp.responder_resources = 4;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;

    if (rdma_accept(id, &cp)) die("rdma_accept");

    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    if (ev->event != RDMA_CM_EVENT_ESTABLISHED) {
        fprintf(stderr, "Expected ESTABLISHED, got %s\n", rdma_event_str(ev->event));
        rdma_ack_cm_event(ev);
        return 1;
    }
    printf("[server] ESTABLISHED\n");
    rdma_ack_cm_event(ev);

    /* Post one RECV (optional) */
    post_recv_ctrl(id->qp, recv_msg, recv_mr);

    /* Send (remote_addr, rkey) to client. */
    uint64_t remote_addr = (uint64_t)(uintptr_t)counter;
    send_msg->addr_be = htonll_u64(remote_addr);
    send_msg->rkey_be = htonl(counter_mr->rkey);

    post_send_ctrl(id->qp, send_msg, send_mr);
    poll_one(cq);

    printf("[server] sent counter info: addr=0x%016" PRIx64 " rkey=0x%x\n",
           remote_addr, counter_mr->rkey);
    printf("[server] waiting for disconnect ...\n");

    for (;;) {
        if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
        enum rdma_cm_event_type et = ev->event;
        rdma_ack_cm_event(ev);

        if (et == RDMA_CM_EVENT_DISCONNECTED) {
            printf("[server] DISCONNECTED final_counter=%" PRIu64 "\n", *counter);
            break;
        }
        fprintf(stderr, "[server] unexpected cm event: %s\n", rdma_event_str(et));
    }

    /* Cleanup */
    rdma_destroy_qp(id);
    ibv_dereg_mr(send_mr);
    ibv_dereg_mr(recv_mr);
    ibv_dereg_mr(counter_mr);
    free(send_msg);
    free(recv_msg);
    free(counter);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    rdma_destroy_id(id);
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);
    return 0;
}
