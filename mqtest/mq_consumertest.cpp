#include "../mqserver/mq_consumer.hpp"
#include <gtest/gtest.h>

bitmq::ConsumerManager::ptr cmp;

class ConsumerTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            cmp = std::make_shared<bitmq::ConsumerManager>();
            cmp->initQueueConsumer("queue1");
        }
        virtual void TearDown() override {
            cmp->clear();
        }
};

void cb(const std::string &tag, const bitmq::BasicProperties *bp, const std::string &body) {
    std::cout << tag << " 消费了消息:" << body << std::endl;
}

TEST(consumer_test, insert_test) {
    cmp->create("consumer1", "queue1", false, cb);
    cmp->create("consumer2", "queue1", false, cb);
    cmp->create("consumer3", "queue1", false, cb);

    ASSERT_EQ(cmp->exists("consumer1", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer2", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer3", "queue1"), true);
}
TEST(consumer_test, remove_test) {
    cmp->remove("consumer1", "queue1");

    ASSERT_EQ(cmp->exists("consumer1", "queue1"), false);
    ASSERT_EQ(cmp->exists("consumer2", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer3", "queue1"), true);
}


TEST(consumer_test, choose_test) {
    bitmq::Consumer::ptr cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer2");

    
    cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer3");

    
    cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer2");
}



int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ConsumerTest);
    return RUN_ALL_TESTS();
    
}