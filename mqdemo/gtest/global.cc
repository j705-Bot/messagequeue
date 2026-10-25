#include <iostream>
#include <gtest/gtest.h>
#include <unordered_map>


class MyEnvironment : public testing::Environment {
    public:
        virtual void SetUp() override {
            std::cout << "单元测试执行前的环境初始化！！\n";
        }
        virtual void TearDown() override {
            std::cout << "单元测试执行完毕后的环境清理！！\n";
        }
};

TEST(MyEnvironment, test1) {
    std::cout << "单元测试1\n";
}

TEST(MyEnvironment, test2) {
    std::cout << "单元测试2\n";
}

std::unordered_map<std::string, std::string> mymap;
class MyMapTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            std::cout << "单元测试执行前的环境初始化！！\n";
            mymap.insert(std::make_pair("hello", "你好"));
            mymap.insert(std::make_pair("bye", "再见"));
        }
        virtual void TearDown() override {
            std::cout << "单元测试执行完毕后的环境清理！！\n";
            mymap.clear();
        }
};
TEST(MyMapTest, test1) {
    ASSERT_EQ(mymap.size(), 2);
    mymap.erase("hello");
}

TEST(MyMapTest, test2) {
    ASSERT_EQ(mymap.size(), 2);
}


int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new MyEnvironment);
    testing::AddGlobalTestEnvironment(new MyMapTest);
    RUN_ALL_TESTS();
    return 0;
}