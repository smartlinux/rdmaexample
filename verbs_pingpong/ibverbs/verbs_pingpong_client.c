/*
 * verbs_pingpong_client_v2.c
 *
 * Pure libibverbs RC Send/Recv ping-pong client (NO rdma_cm).
 *
 * Fixes a subtle issue common in "toy" examples:
 *   - Never discard CQEs you didn't expect.
 *
 * Some providers (including software providers like RXE) can deliver CQEs in an
 * order you did not assume. If your code polls for SEND and "skips" a RECV CQE,
 * you have effectively lost that completion forever -> the next wait for RECV hangs.
 *
 * This v2 client waits for BOTH the SEND and RECV completions each iteration,
 * regardless of ordering.
 *
 * Build:
 *   gcc -O2 -Wall -std=c11 verbs_pingpong_client_v2.c -o pp_client_v2 -libverbs
 *
 * Run:
 *   ./pp_client_v2 <server_ip> <tcp_port> [devname] [ib_port] [gid_index|-1] [iters] [print_every]
 */

#define _GNU_SOURCE
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MSG_SIZE 64

static void die(const char *what) {
    perror(what);
    exit(1);
}

struct conn_info_wire {
    uint16_t lid_be;
    uint32_t qpn_be;
    uint32_t psn_be;
    uint8_t  gid[16];
};

struct conn_info {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;
};

static uint32_t rand_psn24(void) {
    return ((uint32_t)lrand48()) & 0x00ffffffu;
}

static int tcp_connect(const char *host, uint16_t port) {
    char service[16];
    snprintf(service, sizeof(service), "%u", (unsigned)port);

    struct addrinfo hints, *res = NULL, *rp = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int rc = getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        exit(1);
    }

    int fd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        fprintf(stderr, "TCP connect failed to %s:%u\n", host, port);
        exit(1);
    }
    return fd;
}

static void tcp_read_full(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("read");
        }
        if (n == 0) {
            fprintf(stderr, "peer closed TCP unexpectedly\n");
            exit(1);
        }
        p += (size_t)n;
        len -= (size_t)n;
    }
}

static void tcp_write_full(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("write");
        }
        p += (size_t)n;
        len -= (size_t)n;
    }
}

static struct ibv_context *open_device_by_name(const char *devname) {
    int num = 0;
    struct ibv_device **list = ibv_get_device_list(&num);
    if (!list) die("ibv_get_device_list");
    if (num == 0) {
        fprintf(stderr, "No RDMA devices found.\n");
        exit(1);
    }

    struct ibv_context *ctx = NULL;
    for (int i = 0; i < num; i++) {
        const char *name = ibv_get_device_name(list[i]);
        if (!devname || strcmp(name, devname) == 0) {
            ctx = ibv_open_device(list[i]);
            if (!ctx) die("ibv_open_device");
            break;
        }
    }
    ibv_free_device_list(list);

    if (!ctx) {
        fprintf(stderr, "Device not found: %s\n", devname ? devname : "(first)");
        exit(1);
    }
    return ctx;
}

static void query_local_addr(struct ibv_context *ctx, int ib_port, int gid_index,
                            uint16_t *out_lid, union ibv_gid *out_gid) {
    struct ibv_port_attr pattr;
    memset(&pattr, 0, sizeof(pattr));
    if (ibv_query_port(ctx, (uint8_t)ib_port, &pattr)) die("ibv_query_port");
    *out_lid = pattr.lid;

    memset(out_gid, 0, sizeof(*out_gid));
    if (gid_index >= 0) {
        if (ibv_query_gid(ctx, (uint8_t)ib_port, gid_index, out_gid)) die("ibv_query_gid");
    }
}

static void modify_qp_init(struct ibv_qp *qp, int ib_port) {
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof(a));
    a.qp_state = IBV_QPS_INIT;
    a.pkey_index = 0;
    a.port_num = (uint8_t)ib_port;
    a.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp, &a, mask)) die("ibv_modify_qp INIT");
}

static void fill_ah_attr(struct ibv_ah_attr *ah,
                         int ib_port, int gid_index,
                         const struct conn_info *remote) {
    memset(ah, 0, sizeof(*ah));
    ah->port_num = (uint8_t)ib_port;

    if (gid_index >= 0) {
        ah->is_global = 1;
        ah->grh.hop_limit = 1;
        ah->grh.sgid_index = (uint8_t)gid_index;
        ah->grh.dgid = remote->gid;
        ah->dlid = 0; /* ignored for RoCE */
    } else {
        ah->is_global = 0;
        ah->dlid = remote->lid; /* InfiniBand LID routing */
    }
}

static void modify_qp_rtr(struct ibv_qp *qp,
                          int ib_port, int gid_index,
                          const struct conn_info *remote) {
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof(a));
    a.qp_state = IBV_QPS_RTR;
    a.path_mtu = IBV_MTU_1024;

    a.dest_qp_num = remote->qpn;
    a.rq_psn = remote->psn;

    a.max_dest_rd_atomic = 1;
    a.min_rnr_timer = 12;

    fill_ah_attr(&a.ah_attr, ib_port, gid_index, remote);

    int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
               IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
               IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    if (ibv_modify_qp(qp, &a, mask)) die("ibv_modify_qp RTR");
}

static void modify_qp_rts(struct ibv_qp *qp, uint32_t local_psn) {
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof(a));
    a.qp_state = IBV_QPS_RTS;
    a.sq_psn = local_psn;

    a.timeout = 14;
    a.retry_cnt = 7;
    a.rnr_retry = 7;

    a.max_rd_atomic = 1;

    int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
               IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    if (ibv_modify_qp(qp, &a, mask)) die("ibv_modify_qp RTS");
}

static void post_one_recv(struct ibv_qp *qp, void *buf, struct ibv_mr *mr) {
    struct ibv_sge sge = {0};
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

static void post_one_send(struct ibv_qp *qp, void *buf, struct ibv_mr *mr) {
    struct ibv_sge sge = {0};
    sge.addr = (uintptr_t)buf;
    sge.length = MSG_SIZE;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(qp, &wr, &bad)) die("ibv_post_send");
}

/* Wait for both SEND and RECV CQEs without assuming any ordering. */
static void wait_send_and_recv(struct ibv_cq *cq) {
    int got_send = 0;
    int got_recv = 0;

    while (!(got_send && got_recv)) {
        struct ibv_wc wcs[4];
        int n = ibv_poll_cq(cq, 4, wcs);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) continue;

        for (int i = 0; i < n; i++) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "WC error: %s (%d) opcode=%d\n",
                        ibv_wc_status_str(wcs[i].status), wcs[i].status, wcs[i].opcode);
                exit(1);
            }
            if (wcs[i].opcode == IBV_WC_SEND) got_send = 1;
            if (wcs[i].opcode == IBV_WC_RECV) got_recv = 1;
        }
    }
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <server_ip> <tcp_port> [devname] [ib_port] [gid_index|-1] [iters] [print_every]\n",
                argv[0]);
        return 2;
    }

    const char *server_ip = argv[1];
    uint16_t tcp_port = (uint16_t)atoi(argv[2]);
    const char *devname = (argc >= 4) ? argv[3] : NULL;
    int ib_port = (argc >= 5) ? atoi(argv[4]) : 1;
    int gid_index = (argc >= 6) ? atoi(argv[5]) : 0;
    int iters = (argc >= 7) ? atoi(argv[6]) : 1000;
    int print_every = (argc >= 8) ? atoi(argv[7]) : 100;

    srand48((long)time(NULL) ^ (long)getpid());

    struct ibv_context *ctx = open_device_by_name(devname);

    struct ibv_pd *pd = ibv_alloc_pd(ctx);
    if (!pd) die("ibv_alloc_pd");

    struct ibv_cq *cq = ibv_create_cq(ctx, 256, NULL, NULL, 0);
    if (!cq) die("ibv_create_cq");

    void *send_buf = NULL;
    void *recv_buf = NULL;
    if (posix_memalign(&send_buf, 4096, MSG_SIZE)) die("posix_memalign send_buf");
    if (posix_memalign(&recv_buf, 4096, MSG_SIZE)) die("posix_memalign recv_buf");
    memset(send_buf, 0, MSG_SIZE);
    memset(recv_buf, 0, MSG_SIZE);

    struct ibv_mr *send_mr = ibv_reg_mr(pd, send_buf, MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr) die("ibv_reg_mr send_mr");

    struct ibv_mr *recv_mr = ibv_reg_mr(pd, recv_buf, MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!recv_mr) die("ibv_reg_mr recv_mr");

    struct ibv_qp_init_attr qia;
    memset(&qia, 0, sizeof(qia));
    qia.send_cq = cq;
    qia.recv_cq = cq;
    qia.qp_type = IBV_QPT_RC;
    qia.cap.max_send_wr = 128;
    qia.cap.max_recv_wr = 128;
    qia.cap.max_send_sge = 1;
    qia.cap.max_recv_sge = 1;

    struct ibv_qp *qp = ibv_create_qp(pd, &qia);
    if (!qp) die("ibv_create_qp");

    struct conn_info local, remote;
    memset(&local, 0, sizeof(local));
    memset(&remote, 0, sizeof(remote));
    query_local_addr(ctx, ib_port, gid_index, &local.lid, &local.gid);
    local.qpn = qp->qp_num;
    local.psn = rand_psn24();

    int sock = tcp_connect(server_ip, tcp_port);

    struct conn_info_wire w_local, w_remote;
    memset(&w_local, 0, sizeof(w_local));
    w_local.lid_be = htons(local.lid);
    w_local.qpn_be = htonl(local.qpn);
    w_local.psn_be = htonl(local.psn);
    memcpy(w_local.gid, local.gid.raw, 16);

    /* Must match server: client writes first, then reads. */
    tcp_write_full(sock, &w_local, sizeof(w_local));
    tcp_read_full(sock, &w_remote, sizeof(w_remote));

    remote.lid = ntohs(w_remote.lid_be);
    remote.qpn = ntohl(w_remote.qpn_be);
    remote.psn = ntohl(w_remote.psn_be);
    memcpy(remote.gid.raw, w_remote.gid, 16);

    printf("[client] local  qpn=%u psn=%u lid=%u gid_index=%d\n",
           local.qpn, local.psn, local.lid, gid_index);
    printf("[client] remote qpn=%u psn=%u lid=%u\n",
           remote.qpn, remote.psn, remote.lid);

    modify_qp_init(qp, ib_port);
    modify_qp_rtr(qp, ib_port, gid_index, &remote);
    modify_qp_rts(qp, local.psn);

    printf("[client] QP is RTS\n");

    for (int i = 0; i < iters; i++) {
        post_one_recv(qp, recv_buf, recv_mr);

        snprintf((char *)send_buf, MSG_SIZE, "PING %d", i);
        post_one_send(qp, send_buf, send_mr);

        wait_send_and_recv(cq);

        if ((i % print_every) == 0) {
            printf("[client] iter=%d recv='%s'\n", i, (char *)recv_buf);
        }
    }

    printf("[client] done.\n");

    close(sock);
    ibv_destroy_qp(qp);
    ibv_dereg_mr(send_mr);
    ibv_dereg_mr(recv_mr);
    free(send_buf);
    free(recv_buf);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    ibv_close_device(ctx);
    return 0;
}
