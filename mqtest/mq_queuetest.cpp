#include "../mqserver/mq_queue.hpp"
#include <gtest/gtest.h>

bitmq::MsgQueueManager::ptr mqmp;

class QueueTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            mqmp = std::make_shared<bitmq::MsgQueueManager>("./data/meta.db");
        }
        virtual void TearDown() override {
            //mqmp->clear();
        }
};

TEST(queue_test, insert_test) {
    google::protobuf::Map<std::string, std::string> map;    
    map["k1"] = "v1";
    mqmp->declareQueue("queue1", true, false, false, map);
    mqmp->declareQueue("queue2", true, false, false, map);
    mqmp->declareQueue("queue3", true, false, false, map);
    mqmp->declareQueue("queue4", true, false, false, map);
    ASSERT_EQ(mqmp->size(), 4u);                           
}

TEST(queue_test, select_test) {
    ASSERT_EQ(mqmp->exists("queue1"), true);              
    ASSERT_EQ(mqmp->exists("queue2"), true);               
    ASSERT_EQ(mqmp->exists("queue3"), true);
    ASSERT_EQ(mqmp->exists("queue4"), true);

    bitmq::MsgQueue::ptr mqp = mqmp->selectQueue("queue3");
    ASSERT_NE(mqp.get(), nullptr);
    ASSERT_EQ(mqp->name, "queue3");
    ASSERT_EQ(mqp->durable, true);
    ASSERT_EQ(mqp->exclusive, false);
    ASSERT_EQ(mqp->auto_delete, false);
    ASSERT_EQ(mqp->getArgs(), std::string("k1=v1&"));      
}

TEST(queue_test, remove_test) {
    mqmp->deleteQueue("queue3");
    ASSERT_EQ(mqmp->exists("queue3"), false);
}

int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new QueueTest);
    return RUN_ALL_TESTS();                                
}