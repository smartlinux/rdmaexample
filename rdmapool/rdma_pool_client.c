// rdma_pool_client.c
// A small RDMA CM + RC QP demo client:
//
// 1) Connects to the server via RDMA CM.
// 2) Sends a control message (REQ_ALLOC) using SEND/RECV to ask for a remote "slice".
// 3) Receives the server response (RESP_ALLOC) containing: remote_addr, rkey, slice_size.
// 4) Performs a stream of RDMA WRITEs into the granted remote slice.
//    Every DOORBELL_EVERY writes, it uses RDMA_WRITE_WITH_IMM (with immediate data) as a "doorbell".
//    Doorbells are posted as signaled work requests so we can poll CQ and bound outstanding WRs.
//
// This file focuses on: RDMA CM basics, MR registration, RDMA WRITE, WRITE_WITH_IMM, CQ polling,
// and signaled vs unsignaled sends.
//
// Build:
//   gcc -O2 -Wall -std=c11 rdma_pool_client.c -o rdma_pool_client -lrdmacm -libverbs
//
// Run:
//   ./rdma_pool_client <server_ip> <alloc_size_bytes>

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

// CQ capacity. We mostly poll a single completion at a time in this demo.
#define CQ_CAP 256

// Control plane protocol.
#define MAGIC   0x52444d41u  // 'RDMA'
#define VERSION 1

// Data-plane parameters for the write/doorbell loop.
#define TOTAL_WRITES     1000U
#define DOORBELL_EVERY   32U   // Use WRITE_WITH_IMM every N writes (and at the end).
#define TX_RING_SLOTS    DOORBELL_EVERY
#define TX_SLOT_SIZE     64U   // Must be >= sizeof(uint32_t) and reasonably aligned.

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

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

enum msg_type {
    REQ_ALLOC  = 1,
    RESP_ALLOC = 2,
};

struct ctrl_msg {
    // All fields are stored in network byte order on the wire.
    uint32_t magic;
    uint16_t ver;
    uint16_t type;

    // REQ_ALLOC: size = requested bytes.
    // RESP_ALLOC: size = granted bytes, addr/rkey = remote slice.
    uint32_t size;
    uint64_t addr;
    uint32_t rkey;

    // RESP_ALLOC: status = 0 on success, non-zero on failure.
    uint32_t status;
};

static void fill_hdr(struct ctrl_msg *m, uint16_t type) {
    m->magic = htonl(MAGIC);
    m->ver = htons(VERSION);
    m->type = htons(type);
}

static int check_hdr(const struct ctrl_msg *m, uint16_t expect_type) {
    if (ntohl(m->magic) != MAGIC) return -1;
    if (ntohs(m->ver) != VERSION) return -2;
    if (ntohs(m->type) != expect_type) return -3;
    return 0;
}

// A tiny container for the resources we allocate on the client side.
struct resources {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;

    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    // Control messages (SEND/RECV).
    struct ctrl_msg *tx_ctrl;
    struct ctrl_msg *rx_ctrl;
    struct ibv_mr *tx_ctrl_mr;
    struct ibv_mr *rx_ctrl_mr;

    // Transmit ring used for data-plane RDMA WRITEs.
    // We reuse slots in a ring buffer; a signaled doorbell completion guarantees ordering
    // and tells us it's safe to reuse earlier slots.
    uint8_t *tx_ring;
    size_t tx_ring_bytes;
    struct ibv_mr *tx_ring_mr;
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

static void wait_cm_event(struct rdma_event_channel *ec, enum rdma_cm_event_type expect) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) die("rdma_get_cm_event");
    if (ev->event != expect) {
        fprintf(stderr, "Unexpected CM event: got=%d expected=%d\n", ev->event, expect);
        rdma_ack_cm_event(ev);
        exit(1);
    }
    rdma_ack_cm_event(ev);
}

static void poll_cq_one(struct ibv_cq *cq, struct ibv_wc *out) {
    while (1) {
        int n = ibv_poll_cq(cq, 1, out);
        if (n < 0) die("ibv_poll_cq");
        if (n == 0) continue;

        if (out->status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC error: status=%d opcode=%d vendor_err=%u\n",
                    out->status, out->opcode, out->vendor_err);
            exit(1);
        }
        return;
    }
}

static void poll_cq_wait_opcode(struct ibv_cq *cq, enum ibv_wc_opcode opcode) {
    struct ibv_wc wc;
    while (1) {
        poll_cq_one(cq, &wc);
        if (wc.opcode == opcode) return;
        // In this demo we do not expect other opcodes here. If it happens, keep spinning.
    }
}

// Create PD/CQ/QP. We use a plain RC QP with its own receive queue.
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

    // Important: max_send_wr bounds how many outstanding WRs can be in the SQ at once.
    // Doorbells are signaled, unsignaled writes are bounded by DOORBELL_EVERY.
    qp_attr.cap.max_send_wr = 512;
    qp_attr.cap.max_recv_wr = 16;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(r->id, r->pd, &qp_attr)) die("rdma_create_qp");
    r->qp = r->id->qp;
}

static void reg_memory(struct resources *r) {
    // Control messages: small, cache-friendly alignment is enough.
    r->tx_ctrl = (struct ctrl_msg *)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    r->rx_ctrl = (struct ctrl_msg *)xmalloc_aligned(64, sizeof(struct ctrl_msg));
    if (!r->tx_ctrl || !r->rx_ctrl) die("posix_memalign ctrl_msg");

    // Local write access is enough for SEND/RECV buffers.
    int ctrl_flags = IBV_ACCESS_LOCAL_WRITE;
    r->tx_ctrl_mr = ibv_reg_mr(r->pd, r->tx_ctrl, sizeof(*r->tx_ctrl), ctrl_flags);
    r->rx_ctrl_mr = ibv_reg_mr(r->pd, r->rx_ctrl, sizeof(*r->rx_ctrl), ctrl_flags);
    if (!r->tx_ctrl_mr || !r->rx_ctrl_mr) die("ibv_reg_mr ctrl_msg");

    // Transmit ring: align to 4KiB to reduce issues with pinning and to match typical page size.
    // Alignment can be smaller, but 4KiB is a convenient and common choice.
    r->tx_ring_bytes = (size_t)TX_RING_SLOTS * (size_t)TX_SLOT_SIZE;
    size_t bytes = (r->tx_ring_bytes + 4095) & ~((size_t)4095);

    r->tx_ring = (uint8_t *)xmalloc_aligned(4096, bytes);
    if (!r->tx_ring) die("posix_memalign tx_ring");

    r->tx_ring_mr = ibv_reg_mr(r->pd, r->tx_ring, bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!r->tx_ring_mr) die("ibv_reg_mr tx_ring_mr");
}

static void post_recv_ctrl(struct resources *r) {
    // Post one receive for the control response (RESP_ALLOC).
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)r->rx_ctrl;
    sge.length = sizeof(*r->rx_ctrl);
    sge.lkey = r->rx_ctrl_mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 1;  // Arbitrary application cookie.
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(r->qp, &wr, &bad)) die("ibv_post_recv ctrl");
}

static void post_send_ctrl(struct resources *r) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)r->tx_ctrl;
    sge.length = sizeof(*r->tx_ctrl);
    sge.lkey = r->tx_ctrl_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;  // Arbitrary application cookie.
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;  // We poll CQ for this completion.

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(r->qp, &wr, &bad)) die("ibv_post_send ctrl");
}

static void post_rdma_write(struct resources *r,
                            uint64_t remote_addr,
                            uint32_t remote_rkey,
                            void *buf,
                            uint32_t len,
                            int signaled) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = len;
    sge.lkey = r->tx_ring_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 100;  // Not used in this demo.
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(r->qp, &wr, &bad)) die("ibv_post_send RDMA_WRITE");
}

static void post_rdma_write_with_imm(struct resources *r,
                                     uint64_t remote_addr,
                                     uint32_t remote_rkey,
                                     void *buf,
                                     uint32_t len,
                                     uint32_t imm_host_order) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = len;
    sge.lkey = r->tx_ring_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 200;  // Not used in this demo.
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;  // Doorbells are signaled.
    wr.imm_data = htonl(imm_host_order);
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(r->qp, &wr, &bad)) die("ibv_post_send WRITE_WITH_IMM");
}

static void cleanup(struct resources *r) {
    if (!r) return;

    if (r->id) {
        // Best-effort disconnect. If already disconnected, rdma_disconnect may fail with EINVAL.
        rdma_disconnect(r->id);
    }

    if (r->tx_ring_mr) ibv_dereg_mr(r->tx_ring_mr);
    if (r->rx_ctrl_mr) ibv_dereg_mr(r->rx_ctrl_mr);
    if (r->tx_ctrl_mr) ibv_dereg_mr(r->tx_ctrl_mr);

    free(r->tx_ring);
    free(r->rx_ctrl);
    free(r->tx_ctrl);

    if (r->id) {
        if (r->id->qp) rdma_destroy_qp(r->id);
        rdma_destroy_id(r->id);
    }

    if (r->cq) ibv_destroy_cq(r->cq);
    if (r->pd) ibv_dealloc_pd(r->pd);
    if (r->ec) rdma_destroy_event_channel(r->ec);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <server_ip> <alloc_size_bytes>\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    size_t want = (size_t)strtoull(argv[2], NULL, 10);
    if (want == 0) {
        fprintf(stderr, "alloc_size_bytes must be > 0\n");
        return 1;
    }

    struct resources r;
    memset(&r, 0, sizeof(r));

    r.ec = rdma_create_event_channel();
    if (!r.ec) die("rdma_create_event_channel");

    if (rdma_create_id(r.ec, &r.id, NULL, RDMA_PS_TCP)) die("rdma_create_id");

    // Resolve server address.
    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, server_ip, &dst.sin_addr) != 1) {
        fprintf(stderr, "inet_pton failed for %s\n", server_ip);
        cleanup(&r);
        return 1;
    }

    if (rdma_resolve_addr(r.id, NULL, (struct sockaddr *)&dst, 2000)) die("rdma_resolve_addr");
    wait_cm_event(r.ec, RDMA_CM_EVENT_ADDR_RESOLVED);

    if (rdma_resolve_route(r.id, 2000)) die("rdma_resolve_route");
    wait_cm_event(r.ec, RDMA_CM_EVENT_ROUTE_RESOLVED);

    build_qp(&r);
    reg_memory(&r);

    // Pre-post the receive for RESP_ALLOC before we connect and start sending.
    post_recv_ctrl(&r);

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7;

    if (rdma_connect(r.id, &param)) die("rdma_connect");
    wait_cm_event(r.ec, RDMA_CM_EVENT_ESTABLISHED);
    printf("[client] established\n");

    // Send REQ_ALLOC.
    memset(r.tx_ctrl, 0, sizeof(*r.tx_ctrl));
    fill_hdr(r.tx_ctrl, REQ_ALLOC);
    r.tx_ctrl->size = htonl((uint32_t)want);

    post_send_ctrl(&r);

    // Wait for SEND completion and then for RECV completion (RESP_ALLOC).
    poll_cq_wait_opcode(r.cq, IBV_WC_SEND);
    poll_cq_wait_opcode(r.cq, IBV_WC_RECV);

    if (check_hdr(r.rx_ctrl, RESP_ALLOC) != 0) {
        fprintf(stderr, "Bad RESP_ALLOC header\n");
        cleanup(&r);
        return 1;
    }

    uint32_t status = ntohl(r.rx_ctrl->status);
    if (status != 0) {
        fprintf(stderr, "Server allocation failed: status=%u\n", status);
        cleanup(&r);
        return 1;
    }

    size_t slice_size = (size_t)ntohl(r.rx_ctrl->size);
    uint64_t remote_addr = ntohll_u64(r.rx_ctrl->addr);
    uint32_t remote_rkey = ntohl(r.rx_ctrl->rkey);

    printf("[client] got slice: addr=0x%llx rkey=0x%x size=%zu\n",
           (unsigned long long)remote_addr, remote_rkey, slice_size);

    // Data-plane: write 32-bit sequence numbers to different offsets in the remote slice.
    // Every DOORBELL_EVERY writes, ring the "doorbell" via WRITE_WITH_IMM.
    for (uint32_t seq = 1; seq <= TOTAL_WRITES; seq++) {
        uint32_t slot = (seq - 1) % TX_RING_SLOTS;
        uint8_t *slot_ptr = r.tx_ring + (size_t)slot * (size_t)TX_SLOT_SIZE;

        // Payload: a single 32-bit integer (host order). The server will read it back.
        memcpy(slot_ptr, &seq, sizeof(seq));
        uint32_t len = (uint32_t)sizeof(seq);

        uint64_t ra = remote_addr + (uint64_t)(seq - 1) * sizeof(uint32_t);
        int is_doorbell = ((seq % DOORBELL_EVERY) == 0) || (seq == TOTAL_WRITES);

        if (is_doorbell) {
            // The immediate data is delivered to the server in the completion entry (wc.imm_data),
            // in network byte order. The server typically calls ntohl() to obtain host order.
            post_rdma_write_with_imm(&r, ra, remote_rkey, slot_ptr, len, seq);

            // Wait for the signaled RDMA_WRITE completion. This also orders prior unsignaled writes.
            poll_cq_wait_opcode(r.cq, IBV_WC_RDMA_WRITE);
            printf("[client] doorbell complete: seq=%u\n", seq);
        } else {
            // Unsignaled write: reduces CQ load and can improve throughput.
            // The doorbell completion acts as a checkpoint so the SQ does not grow without bound.
            post_rdma_write(&r, ra, remote_rkey, slot_ptr, len, 0);
        }
    }

    printf("[client] done\n");
    cleanup(&r);
    return 0;
}
