
// rdma_kv_client.c
//
// A compact RDMA KV client matching rdma_kv_server_ev.c.
//
// Usage:
//   ./rdma_kv_client <server_ip> <port> put <key> <value>
//   ./rdma_kv_client <server_ip> <port> get <key>
//
// Notes:
//   - Key length is limited to 32 bytes (KEY_MAX).
//   - Value length is limited by MAX_VAL and by the server pool size.
//   - Control plane uses SEND/RECV messages.
//   - PUT payload uses RDMA_WRITE + RDMA_WRITE_WITH_IMM (commit token).
//   - GET payload uses RDMA_READ.
//
// Build:
//   gcc -O2 -Wall -std=c11 rdma_kv_client.c -o rdma_kv_client -lrdmacm -libverbs


#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <poll.h>				 
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ============================ Protocol (must match server) ============================

#define KV_MAGIC   0x4b565244u  // 'KVRD'
#define KV_VERSION 1

#define KEY_MAX 32
#define RECV_BUF_SZ 256

#define DOORBELL_SIZE 4					   
enum kv_type {
    KV_PUT_REQ  = 1,
    KV_PUT_RESP = 2,
    KV_GET_REQ  = 3,
    KV_GET_RESP = 4
};

struct kv_msg {
    uint32_t magic;
    uint16_t ver;
    uint16_t type;

    uint32_t status;
    uint32_t key_len;
    uint32_t value_len;

    uint32_t token;
    uint32_t version;

    uint64_t addr;
    uint32_t rkey;
    uint32_t _reserved;

    uint8_t  key[KEY_MAX];
    uint8_t  _pad[RECV_BUF_SZ - 4 - 2 - 2 - 4 * 6 - 8 - 4 - 4 - KEY_MAX];
};
_Static_assert(sizeof(struct kv_msg) == RECV_BUF_SZ, "kv_msg must fit in RECV_BUF_SZ");

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

// ============================ Utilities ============================

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static void *xmalloc_aligned(size_t alignment, size_t size) {
    void *p = NULL;
    int rc = posix_memalign(&p, alignment, size);
    if (rc != 0) { errno = rc; return NULL; }
    memset(p, 0, size);
    return p;
}

static void poll_one_cq(struct ibv_cq *cq, struct ibv_wc *out_wc) {
    while (1) {
        int n = ibv_poll_cq(cq, 1, out_wc);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) continue;
        if (out_wc->status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC error: status=%d opcode=%d\n", out_wc->status, out_wc->opcode);
            exit(1);
        }
        return;
    }
}

static void wait_cm_event(struct rdma_event_channel *ec, enum rdma_cm_event_type expect) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");

    if (ev->event != expect) {
        fprintf(stderr, "Unexpected CM event: got %s (%d), expected %s (%d)\n",
                rdma_event_str(ev->event), ev->event,
                rdma_event_str(expect), expect);
        rdma_ack_cm_event(ev);
        exit(1);
    }
    rdma_ack_cm_event(ev);
}

// ============================ Client resources ============================

#define CQ_CAP 128
#define MAX_VAL (1u << 20) // 1 MiB demo buffer

struct client {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;

    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct kv_msg *send_msg;
    struct kv_msg *recv_msg;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;

    uint8_t *tx_buf;
    uint8_t *rx_buf;
    struct ibv_mr *tx_mr;
    struct ibv_mr *rx_mr;
};

static void build_qp(struct client *c) {
    c->pd = ibv_alloc_pd(c->id->verbs);
    if (!c->pd) die("ibv_alloc_pd");

    c->cq = ibv_create_cq(c->id->verbs, CQ_CAP, NULL, NULL, 0);
    if (!c->cq) die("ibv_create_cq");

    struct ibv_qp_init_attr qattr;
    memset(&qattr, 0, sizeof(qattr));
    qattr.send_cq = c->cq;
    qattr.recv_cq = c->cq;
    qattr.qp_type = IBV_QPT_RC;
    qattr.cap.max_send_wr  = 128;
    qattr.cap.max_recv_wr  = 128;
    qattr.cap.max_send_sge = 1;
    qattr.cap.max_recv_sge = 1;

    if (rdma_create_qp(c->id, c->pd, &qattr)) die("rdma_create_qp");
    c->qp = c->id->qp;
}

static void reg_buffers(struct client *c) {
    c->send_msg = (struct kv_msg *)xmalloc_aligned(64, sizeof(struct kv_msg));
    c->recv_msg = (struct kv_msg *)xmalloc_aligned(64, sizeof(struct kv_msg));
    if (!c->send_msg || !c->recv_msg) die("xmalloc_aligned ctrl msg");

    c->send_mr = ibv_reg_mr(c->pd, c->send_msg, sizeof(*c->send_msg), IBV_ACCESS_LOCAL_WRITE);
    c->recv_mr = ibv_reg_mr(c->pd, c->recv_msg, sizeof(*c->recv_msg), IBV_ACCESS_LOCAL_WRITE);
    if (!c->send_mr || !c->recv_mr) die("ibv_reg_mr ctrl msg");

    c->tx_buf = (uint8_t *)xmalloc_aligned(4096, MAX_VAL);
    c->rx_buf = (uint8_t *)xmalloc_aligned(4096, MAX_VAL);
    if (!c->tx_buf || !c->rx_buf) die("xmalloc_aligned data buf");

    c->tx_mr = ibv_reg_mr(c->pd, c->tx_buf, MAX_VAL, IBV_ACCESS_LOCAL_WRITE);
    c->rx_mr = ibv_reg_mr(c->pd, c->rx_buf, MAX_VAL, IBV_ACCESS_LOCAL_WRITE);
    if (!c->tx_mr || !c->rx_mr) die("ibv_reg_mr data buf");
}

static void post_one_recv_msg(struct client *c) {
    if (rdma_post_recv(c->id, NULL, c->recv_msg, sizeof(*c->recv_msg), c->recv_mr)) {
        die("rdma_post_recv");
    }
}

// ============================ Verbs helpers ============================

static void post_rdma_write(struct client *c,
                            uint64_t remote_addr, uint32_t rkey,
                            void *buf, uint32_t len, struct ibv_mr *mr,
                            int signaled) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = len;
    sge.lkey   = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->qp, &wr, &bad)) die("ibv_post_send RDMA_WRITE");
}

static void post_rdma_write_with_imm(struct client *c,
                                     uint64_t remote_addr, uint32_t rkey,
                                     void *buf, uint32_t len, struct ibv_mr *mr,
                                     uint32_t imm_host_order,
                                     int signaled) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = len;
    sge.lkey   = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.imm_data = htonl(imm_host_order);
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->qp, &wr, &bad)) die("ibv_post_send RDMA_WRITE_WITH_IMM");
}

static void post_rdma_read(struct client *c,
                           uint64_t remote_addr, uint32_t rkey,
                           void *buf, uint32_t len, struct ibv_mr *mr,
                           int signaled) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = len;
    sge.lkey   = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(c->qp, &wr, &bad)) die("ibv_post_send RDMA_READ");
}

// ============================ Operations ============================

static void do_put(struct client *c, const char *key, const char *value) {
    size_t key_len = strlen(key);
    size_t value_len = strlen(value); // demo assumes text (no trailing NUL needed)

    if (key_len == 0 || key_len > KEY_MAX) {
        fprintf(stderr, "Key length must be 1..%u bytes\n", KEY_MAX);
        exit(2);
    }
    if (value_len == 0 || value_len > MAX_VAL) {
        fprintf(stderr, "Value length must be 1..%u bytes\n", MAX_VAL);
        exit(2);
    }

    memcpy(c->tx_buf, value, value_len);

    // Prepare request.
    memset(c->send_msg, 0, sizeof(*c->send_msg));
    kv_fill_hdr(c->send_msg, KV_PUT_REQ);
    c->send_msg->status = htonl(0);
    c->send_msg->key_len = htonl((uint32_t)key_len);
    c->send_msg->value_len = htonl((uint32_t)value_len);
    memcpy(c->send_msg->key, key, key_len);

    // Post recv for response BEFORE sending request.
    post_one_recv_msg(c);

    if (rdma_post_send(c->id, NULL, c->send_msg, sizeof(*c->send_msg), c->send_mr, IBV_SEND_SIGNALED)) {
        die("rdma_post_send PUT_REQ");
    }

    // Wait for SEND completion and RECV completion (order can vary).
    int got_send = 0, got_recv = 0;
    struct ibv_wc wc;
    while (!got_send || !got_recv) {
        poll_one_cq(c->cq, &wc);
        if (wc.opcode == IBV_WC_SEND) got_send = 1;
        else if (wc.opcode == IBV_WC_RECV) got_recv = 1;
    }

    if (kv_check_hdr(c->recv_msg, KV_PUT_RESP) != 0) {
        fprintf(stderr, "Bad PUT_RESP header\n");
        exit(1);
    }

    uint32_t st = ntohl(c->recv_msg->status);
    if (st != 0) {
        fprintf(stderr, "PUT failed: status=%u (%s)\n", st, strerror((int)st));
        exit(1);
    }

    uint64_t remote_addr = ntohll_u64(c->recv_msg->addr);
    uint32_t rkey = ntohl(c->recv_msg->rkey);
    uint32_t granted = ntohl(c->recv_msg->value_len);
    uint32_t token = ntohl(c->recv_msg->token);
    uint32_t ver = ntohl(c->recv_msg->version);

    if (granted < value_len + DOORBELL_SIZE) {
        fprintf(stderr, "Server granted too small slice: granted=%u want>=%zu (payload + doorbell)", 
		granted, (size_t)value_len + DOORBELL_SIZE);
        exit(1);
    }

    printf("[client] PUT granted: addr=0x%llx rkey=0x%x granted=%u token=%u version=%u\n",
           (unsigned long long)remote_addr, rkey, granted, token, ver);

    // Data plane:
    //   1) RDMA_WRITE payload
    //   2) RDMA_WRITE_WITH_IMM as commit (imm_data = token)
    //
    // On an RC QP, these operations are ordered, so the server observing the commit implies
    // the payload is already visible in memory.
    post_rdma_write(c, remote_addr, rkey, c->tx_buf, (uint32_t)value_len, c->tx_mr, 1);

    poll_one_cq(c->cq, &wc); // RDMA_WRITE completion (signaled)
    if (wc.opcode != IBV_WC_RDMA_WRITE) {
        fprintf(stderr, "Unexpected WC opcode after RDMA_WRITE: %d\n", wc.opcode);
        exit(1);
    }

    // A zero-length WRITE_WITH_IMM is allowed by many HCAs, but not all stacks accept it.
    // For portability we write 4 bytes into a *padding* area that the server never reads.
    //
    // The server grants a slice rounded up to CHUNK_SIZE (granted >= value_len). We write the doorbell
    // at the very end of the granted slice so we do not overwrite the actual value bytes.
    uint32_t doorbell_word = htonl(token);
    uint8_t *doorbell_local = c->tx_buf + (MAX_VAL - sizeof(doorbell_word));
    memcpy(doorbell_local, &doorbell_word, sizeof(doorbell_word));

    uint64_t doorbell_remote = remote_addr + (uint64_t)granted - sizeof(doorbell_word);
    post_rdma_write_with_imm(c, doorbell_remote, rkey,
                             doorbell_local, (uint32_t)sizeof(doorbell_word), c->tx_mr,
                             token, 1);

    poll_one_cq(c->cq, &wc); // WRITE_WITH_IMM completion
    if (wc.opcode != IBV_WC_RDMA_WRITE) {
        // Verbs reports WRITE_WITH_IMM completion as IBV_WC_RDMA_WRITE.
        fprintf(stderr, "Unexpected WC opcode after WRITE_WITH_IMM: %d\n", wc.opcode);
        exit(1);
    }

    printf("[client] PUT committed.\n");
}

static void do_get(struct client *c, const char *key) {
    size_t key_len = strlen(key);
    if (key_len == 0 || key_len > KEY_MAX) {
        fprintf(stderr, "Key length must be 1..%u bytes\n", KEY_MAX);
        exit(2);
    }

    memset(c->send_msg, 0, sizeof(*c->send_msg));
    kv_fill_hdr(c->send_msg, KV_GET_REQ);
    c->send_msg->status = htonl(0);
    c->send_msg->key_len = htonl((uint32_t)key_len);
    memcpy(c->send_msg->key, key, key_len);

    post_one_recv_msg(c);

    if (rdma_post_send(c->id, NULL, c->send_msg, sizeof(*c->send_msg), c->send_mr, IBV_SEND_SIGNALED)) {
        die("rdma_post_send GET_REQ");
    }

    int got_send = 0, got_recv = 0;
    struct ibv_wc wc;
    while (!got_send || !got_recv) {
        poll_one_cq(c->cq, &wc);
        if (wc.opcode == IBV_WC_SEND) got_send = 1;
        else if (wc.opcode == IBV_WC_RECV) got_recv = 1;
    }

    if (kv_check_hdr(c->recv_msg, KV_GET_RESP) != 0) {
        fprintf(stderr, "Bad GET_RESP header\n");
        exit(1);
    }

    uint32_t st = ntohl(c->recv_msg->status);
    if (st != 0) {
        fprintf(stderr, "GET failed: status=%u (%s)\n", st, strerror((int)st));
        exit(1);
    }

    uint64_t remote_addr = ntohll_u64(c->recv_msg->addr);
    uint32_t rkey = ntohl(c->recv_msg->rkey);
    uint32_t value_len = ntohl(c->recv_msg->value_len);
    uint32_t ver = ntohl(c->recv_msg->version);

    if (value_len > MAX_VAL) {
        fprintf(stderr, "Value too large for client buffer: len=%u\n", value_len);
        exit(1);
    }

    printf("[client] GET: addr=0x%llx rkey=0x%x len=%u version=%u\n",
           (unsigned long long)remote_addr, rkey, value_len, ver);

    post_rdma_read(c, remote_addr, rkey, c->rx_buf, value_len, c->rx_mr, 1);

    poll_one_cq(c->cq, &wc);
    if (wc.opcode != IBV_WC_RDMA_READ) {
        fprintf(stderr, "Unexpected WC opcode after RDMA_READ: %d\n", wc.opcode);
        exit(1);
    }

    // Print as text for demo.
    printf("-------- value (%u bytes) --------\n", value_len);
    fwrite(c->rx_buf, 1, value_len, stdout);
    printf("\n----------------------------------\n");
}

// ============================ Connect/Disconnect ============================

static void connect_cm(struct client *c, const char *server_ip, const char *port_str) {
    c->ec = rdma_create_event_channel();
    if (!c->ec) die("rdma_create_event_channel");

    if (rdma_create_id(c->ec, &c->id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    // Resolve address using standard getaddrinfo() to obtain a sockaddr for rdma_resolve_addr().
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;      // keep it simple for the demo
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res = NULL;
    int gai = getaddrinfo(server_ip, port_str, &hints, &res);
    if (gai != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai));
        exit(1);
    }

    if (rdma_resolve_addr(c->id, NULL, res->ai_addr, 2000)) die("rdma_resolve_addr");
    wait_cm_event(c->ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(c->id, 2000)) die("rdma_resolve_route");
    wait_cm_event(c->ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    freeaddrinfo(res);

    build_qp(c);
    reg_buffers(c);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_connect(c->id, &param)) die("rdma_connect");
    wait_cm_event(c->ec, RDMA_CM_EVENT_ESTABLISHED);

    printf("[client] connected.\n");
}

static void disconnect_cm(struct client *c) {
    if (!c->id) return;
    rdma_disconnect(c->id);

    /*
     * Best-effort wait for RDMA_CM_EVENT_DISCONNECTED.
     *
     * Some software providers (e.g., RXE) and some teardown races can delay or even
     * skip delivering a DISCONNECTED event to the active side in a timely manner.
     * Blocking forever here makes the demo look "hung" even though the RDMA ops
     * already completed successfully.
     */
    {
        struct pollfd pfd;
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = c->ec->fd;
        pfd.events = POLLIN;

        int prc = poll(&pfd, 1, 1000 /* ms */);
        if (prc > 0 && (pfd.revents & POLLIN)) {
            struct rdma_cm_event *ev = NULL;
            if (rdma_get_cm_event(c->ec, &ev) == 0) {
																		
                rdma_ack_cm_event(ev);
            }
        }
    }

    if (c->id->qp) rdma_destroy_qp(c->id);
    rdma_destroy_id(c->id);

    if (c->send_mr) ibv_dereg_mr(c->send_mr);
    if (c->recv_mr) ibv_dereg_mr(c->recv_mr);
    if (c->tx_mr) ibv_dereg_mr(c->tx_mr);
    if (c->rx_mr) ibv_dereg_mr(c->rx_mr);

    free(c->send_msg);
    free(c->recv_msg);
    free(c->tx_buf);
    free(c->rx_buf);

    if (c->cq) ibv_destroy_cq(c->cq);
    if (c->pd) ibv_dealloc_pd(c->pd);

    rdma_destroy_event_channel(c->ec);

    memset(c, 0, sizeof(*c));
}

// ============================ Main ============================

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr,
                "Usage:\n"
                "  %s <server_ip> <port> put <key> <value>\n"
                "  %s <server_ip> <port> get <key>\n",
                argv[0], argv[0]);
        return 2;
    }

    const char *server_ip = argv[1];
    const char *port_str  = argv[2];
    const char *op        = argv[3];

    struct client c;
    memset(&c, 0, sizeof(c));

    connect_cm(&c, server_ip, port_str);

    if (strcmp(op, "put") == 0) {
        if (argc < 6) {
            fprintf(stderr, "put requires <key> <value>\n");
            disconnect_cm(&c);
            return 2;
        }
        do_put(&c, argv[4], argv[5]);
    } else if (strcmp(op, "get") == 0) {
        do_get(&c, argv[4]);
    } else {
        fprintf(stderr, "Unknown op: %s (expected put/get)\n", op);
        disconnect_cm(&c);
        return 2;
    }

    disconnect_cm(&c);
    return 0;
}
