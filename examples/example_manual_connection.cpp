#include <infiniband/verbs.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "rdma_util.h"

using namespace std;

int main() {
    void* buffer = malloc(1024);
    string dev_name;
    cout << "Enter device name: " << endl;
    cin >> dev_name;
    auto qp = rdma_util::RcQueuePair::create(dev_name.c_str());
    auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, 1024);
    auto handshake_data = qp->get_handshake_data(3);

    cout << "Buffer address, rkey" << endl;
    cout << uint64_t(mr->get_addr()) << " " << mr->get_rkey() << endl;

    uint64_t remote_mr_addr = 0;
    uint64_t rkey = 0;
    cout << "Enter remote buffer address and rkey: " << endl;
    cin >> remote_mr_addr >> rkey;

    uint64_t raw[2] {};
    memcpy(raw, &(handshake_data.gid), sizeof(ibv_gid));
    uint16_t lid = handshake_data.lid;
    uint32_t qp_num = handshake_data.qp_num;
    cout << "Local data:" << endl;
    cout << raw[0] << " " << raw[1] << " " << lid << " " << qp_num << endl;

    cout << "Enter remote data: " << endl;
    rdma_util::HandshakeData remote_data {};
    cin >> raw[0] >> raw[1] >> lid >> qp_num;
    memcpy(&(remote_data.gid), raw, sizeof(ibv_gid));
    remote_data.lid = lid;
    remote_data.qp_num = qp_num;

    qp->bring_up(remote_data, 3, ibv_rate::IBV_RATE_MAX);

    int sender = 0;
    cout << "Sender? " << endl;
    cin >> sender;

    if (sender) {
        qp->post_send_write_with_imm(
            0,
            reinterpret_cast<uint64_t>(buffer),
            remote_mr_addr,
            1024,
            1,
            mr->get_lkey(),
            rkey,
            true
        );
        std::vector<ibv_wc> wcs;
        wcs.reserve(10);
        while (qp->poll_send_cq_once(10, wcs) == 0) {}
        cout << "Send completed" << endl;
    } else {
        qp->post_recv(0, reinterpret_cast<uint64_t>(buffer), 0, mr->get_lkey());
        std::vector<ibv_wc> wcs;
        wcs.reserve(1);
        while (qp->poll_recv_cq_once(1, wcs) == 0) {}
        cout << "Recv completed" << endl;
    }

    cout << "Press to quit" << endl;
    cin.get();
    free(buffer);
    return 0;
}
