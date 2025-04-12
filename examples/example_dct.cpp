// #include <infiniband/mlx5dv.h>
// #include <infiniband/verbs.h>

// bool initiator = false;

// /// Create DC QP
// int main() {
//     auto list = ibv_get_device_list(nullptr);
//     auto context = ibv_open_device(list[0]);
//     auto send_cq = ibv_create_cq(context, 128, nullptr, nullptr, 0);
//     auto recv_cq = ibv_create_cq(context, 128, nullptr, nullptr, 0);
//     auto pd = ibv_alloc_pd(context);

//     struct mlx5dv_qp_init_attr dv_init_attr;
//     struct ibv_qp_init_attr_ex init_attr;
//     struct ibv_srq_init_attr srq_init_attr;

//     memset(&dv_init_attr, 0, sizeof(dv_init_attr));
//     memset(&init_attr, 0, sizeof(init_attr));
//     memset(&srq_init_attr, 0, sizeof(srq_init_attr));

//     // TODO: set srq attributes

//     auto srq = ibv_create_srq(pd, &srq_init_attr);

//     init_attr.qp_type = IBV_QPT_DRIVER;
//     init_attr.send_cq = send_cq;
//     init_attr.recv_cq = recv_cq;
//     init_attr.pd = pd;

//     if (initiator) {
//         /** DCI **/
//         init_attr.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS | IBV_QP_INIT_ATTR_PD;
//         init_attr.send_ops_flags |= IBV_QP_EX_WITH_SEND;

//         dv_init_attr.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC | MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
//         dv_init_attr.create_flags |= MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
//         dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;
//     } else {
//         /** DCT **/
//         init_attr.comp_mask |= IBV_QP_INIT_ATTR_PD;
//         init_attr.srq = srq;
//         dv_init_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
//         dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
//         dv_init_attr.dc_init_attr.dct_access_key = DC_KEY;
//     }

//     auto qp = mlx5dv_create_qp(context, &init_attr, &dv_init_attr);

//     if (initiator) {
//         auto ex_qp = ibv_qp_to_qp_ex(qp);
//         auto dv_qp = mlx5dv_qp_ex_from_ibv_qp_ex(ex_qp);
//     }

//     return 0;
// }

int main() {
    return 0;
}