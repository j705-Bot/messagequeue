#include "../mqserver/mq_message.hpp"
#include <gtest/gtest.h>


bitmq::MessageManager::ptr mmp;
// 第一轮环境
class MessageTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            mmp = std::make_shared<bitmq::MessageManager>("./data/message/");
            mmp->initQueueMessage("queue1");
        }
        virtual void TearDown() override {
            //mmp->clear();//注释掉！否则数据被删，第二轮恢复不出来
        }
};

//新增消息测试：新增消息，然后观察可获取消息数量，以及持久化消息数量
TEST(message_test, insert_test) {
    bitmq::BasicProperties properties;
    properties.set_id(bitmq::UUIDHelper::uuid());
    properties.set_delivery_mode(bitmq::DeliveryMode::DURABLE);
    properties.set_routing_key("news.music.pop");
    mmp->insert("queue1", &properties, "Hello World-1", bitmq::DeliveryMode::DURABLE);
    mmp->insert("queue1", nullptr, "Hello World-2", bitmq::DeliveryMode::DURABLE);
    mmp->insert("queue1", nullptr, "Hello World-3", bitmq::DeliveryMode::DURABLE);
    mmp->insert("queue1", nullptr, "Hello World-4", bitmq::DeliveryMode::DURABLE);
    mmp->insert("queue1", nullptr, "Hello World-5", bitmq::DeliveryMode::UNDURABLE);
    ASSERT_EQ(mmp->getable_count("queue1"), 5);
    ASSERT_EQ(mmp->total_count("queue1"), 4);
    ASSERT_EQ(mmp->durable_count("queue1"), 4);
    ASSERT_EQ(mmp->waitack_count("queue1"), 0);
}


// //获取消息测试：获取一条消息，然后在不进行确认的情况下，查看可获取消息数量，待确认消息数量，以及测试消息获取的顺序
TEST(message_test, select_test) {
    ASSERT_EQ(mmp->getable_count("queue1"), 5);
    bitmq::MessagePtr msg1 = mmp->front("queue1");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
    ASSERT_EQ(mmp->getable_count("queue1"), 4);
    ASSERT_EQ(mmp->waitack_count("queue1"), 1);

    bitmq::MessagePtr msg2 = mmp->front("queue1");
    ASSERT_NE(msg2.get(), nullptr);
    ASSERT_EQ(msg2->payload().body(), std::string("Hello World-2"));
    ASSERT_EQ(mmp->getable_count("queue1"), 3);
    ASSERT_EQ(mmp->waitack_count("queue1"), 2);

    bitmq::MessagePtr msg3 = mmp->front("queue1");
    ASSERT_NE(msg3.get(), nullptr);
    ASSERT_EQ(msg3->payload().body(), std::string("Hello World-3"));
    ASSERT_EQ(mmp->getable_count("queue1"), 2);
    ASSERT_EQ(mmp->waitack_count("queue1"), 3);

    bitmq::MessagePtr msg4 = mmp->front("queue1");
    ASSERT_NE(msg4.get(), nullptr);
    ASSERT_EQ(msg4->payload().body(), std::string("Hello World-4"));
    ASSERT_EQ(mmp->getable_count("queue1"), 1);
    ASSERT_EQ(mmp->waitack_count("queue1"), 4);

    bitmq::MessagePtr msg5 = mmp->front("queue1");
    ASSERT_NE(msg5.get(), nullptr);
    ASSERT_EQ(msg5->payload().body(), std::string("Hello World-5"));
    ASSERT_EQ(mmp->getable_count("queue1"), 0);
    ASSERT_EQ(mmp->waitack_count("queue1"), 5);
}

// //删除消息测试：确认一条消息，查看持久化消息数量，待确认消息数量
TEST(message_test, delete_test) {
    ASSERT_EQ(mmp->getable_count("queue1"), 5);
    bitmq::MessagePtr msg1 = mmp->front("queue1");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
    ASSERT_EQ(mmp->getable_count("queue1"), 4);
    ASSERT_EQ(mmp->waitack_count("queue1"), 1);
    mmp->ack("queue1", msg1->payload().properties().id());
    ASSERT_EQ(mmp->waitack_count("queue1"), 0);
    ASSERT_EQ(mmp->durable_count("queue1"), 3);
    ASSERT_EQ(mmp->total_count("queue1"), 4);
}

// //销毁测试
//销毁测试
TEST(message_test, clear) {
    mmp->destroyQueueMessage("queue1");
}

//恢复历史数据测试

bitmq::MessageManager::ptr mmp1;
class MessageTest2 : public testing::Environment {
    public:
        virtual void SetUp() override {
            mmp1 = std::make_shared<bitmq::MessageManager>("./data/message/");
        }
        virtual void TearDown() override {
            //mmp->clear();
        }
};

TEST(message_test2, recovery_test) {
    mmp1->initQueueMessage("queue1");

    // 恢复出 3 条有效消息（World-1 已作废，World-5 未持久化）
    ASSERT_EQ(mmp1->getable_count("queue1"), 3);

    bitmq::MessagePtr msg1 = mmp1->front("queue1");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_EQ(msg1->payload().body(), std::string("Hello World-2"));
    ASSERT_EQ(mmp1->getable_count("queue1"), 2);
    ASSERT_EQ(mmp1->waitack_count("queue1"), 1);

    bitmq::MessagePtr msg2 = mmp1->front("queue1");
    ASSERT_NE(msg2.get(), nullptr);
    ASSERT_EQ(msg2->payload().body(), std::string("Hello World-3"));
    ASSERT_EQ(mmp1->getable_count("queue1"), 1);
    ASSERT_EQ(mmp1->waitack_count("queue1"), 2);

    bitmq::MessagePtr msg3 = mmp1->front("queue1");
    ASSERT_NE(msg3.get(), nullptr);
    ASSERT_EQ(msg3->payload().body(), std::string("Hello World-4"));
    ASSERT_EQ(mmp1->getable_count("queue1"), 0);
    ASSERT_EQ(mmp1->waitack_count("queue1"), 3);

    // 队列取空了
    bitmq::MessagePtr msg4 = mmp1->front("queue1");
    ASSERT_EQ(msg4.get(), nullptr);
}




int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new MessageTest);
    testing::AddGlobalTestEnvironment(new MessageTest2);
    return RUN_ALL_TESTS();
}