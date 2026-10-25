#include <iostream>
#include<thread>
#include <future>

int Add(int num1,int num2)
{
    std::cout << "加法！！1111\n";
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "加法！！2222\n";
    return num1+num2;
}

int main()
{
    std::cout <<"-------1------\n";
    //std::launch::deferred  在执行get获取异步结果的时候，才会执行异步任务
    //std::launch::async   内部会创建工作线程，异步的完成任务
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::future<int> result = std::async(std::launch::async,Add,11,22);
    std::cout <<"-------2------\n";
    int sum = result.get();
    std::cout <<"-------3------\n";
    std::cout << sum << std::endl;
    return 0;
}