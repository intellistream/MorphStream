#include "test_mm.h"

#include <pthread.h>
#include <infiniband/verbs.h>

void MMTest::SetUp() {
    int ret = 0;

    setup_server_conf();
    setup_client_conf();
    client_conf_.num_replication = 1;
    client_conf_.server_base_addr = 0x10000000;
    client_conf_.server_data_len = 2147483648;
    client_conf_.block_size = 67108864;
    client_conf_.subblock_size = 256;
    client_conf_.client_local_size = 1073741824;
    client_conf_.num_coroutines = 8;
    
    server_ = new Server(&server_conf_);
    pthread_create(&server_tid_, NULL, server_main, server_);

    client_ = new Client(&client_conf_);
    polling_tid_ = client_->start_polling_thread();
    
    client_nm_ = client_->get_nm();
    client_mm_ = client_->get_mm();
    printf("===== Initialization finished ====\n");
}

void MMTest::TearDown() {
    server_->stop();
    pthread_join(server_tid_, NULL);
    client_->stop_polling_thread();
    pthread_join(polling_tid_, NULL);
    delete server_;
    delete client_nm_;
    delete client_mm_;
}

TEST_F(MMTest, initialization) {
    ASSERT_TRUE(true);
}

TEST_F(MMTest, mmalloc) {
    ClientMMAllocCtx alloc_ctx;
    client_mm_->mm_alloc(1024 * 1024 * 32, client_nm_, &alloc_ctx);
    printf("%lx\n", alloc_ctx.addr_list[0]);
}

TEST_F(MMTest, mmalloc_multi) {
    for (int i = 0; i < 10; i ++) {
        ClientMMAllocCtx ctx;
        client_mm_->mm_alloc(1024 * 1024 * 32, client_nm_, &ctx);
        printf("%lx\n", ctx.addr_list[0]);
    }
}

TEST_F(MMTest, mmalloc_multi_fiber) {
    boost::fibers::fiber fiber_list[8];
    for (int i = 0; i < 8; i ++) {
        boost::fibers::fiber fb([&](int coro_id) {
            for (int i = 0; i < 2; i ++) {
                ClientMMAllocCtx ctx;
                printf("%d: start alloc\n", coro_id);
                client_mm_->mm_alloc(1024 * 1024 * 32, client_nm_, &ctx);
                printf("%d: %lx\n", coro_id, ctx.addr_list[0]);
                boost::this_fiber::yield();
            }
        }, i);
        fiber_list[i] = std::move(fb);
    }
    for (int i = 0; i < 8; i ++) {
        fiber_list[i].join();
    }
}