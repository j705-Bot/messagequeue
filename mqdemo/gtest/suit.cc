#include <iostream>
#include <gtest/gtest.h>
#include <unordered_map>

class MyTest : public testing::Test {
    public:
        static void SetUpTestCase() {
            std::cout << "所有单元测试前执行，初始化总环境!\n";
            //假设我有一个全局的测试数据容器，在这里插入公共的测试数据
        }
        void SetUp() override {
            //在这里插入每个单元测试所需的独立的测试数据
            std::cout << "单元测试初始化！！\n";
            _mymap.insert(std::make_pair("hello", "你好"));
            _mymap.insert(std::make_pair("bye", "再见"));
        }
        void TearDown() override {
            //在这里清理每个单元测试自己插入的数据
            _mymap.clear();
            std::cout << "单元测试清理！！\n";
        }
        static void TearDownTestCase() { 
            //清理公共的测试数据
            std::cout << "所有单元测试完毕执行，清理总环境!\n";
        }
    public:
        std::unordered_map<std::string, std::string> _mymap;
};

TEST_F(MyTest, insert_test) {
    _mymap.insert(std::make_pair("leihou", "你好"));
    ASSERT_EQ(_mymap.size(), 3);
    
}
TEST_F(MyTest, size_test) {
    ASSERT_EQ(_mymap.size(), 2);
}


int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}