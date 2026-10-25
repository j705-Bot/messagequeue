#include <iostream>
#include <thread>
#include <future>
#include <memory>
int Add(int num1,int num2)
{
     std::this_thread::sleep_for(std::chrono::seconds(1));
    return num1+num2;
}

int main()
{
    // std::packaged_task<int(int,int)> task(Add);
    // std::future<int> fu = task.get_future();  

    //std::async(std::launch::async,task,11,22);
    //task(11,22);

    auto ptask = std::make_shared<std::packaged_task<int(int,int)>>(Add);
    std::future<int> fu = ptask->get_future();  
    std::thread thr([ptask]()
    {
        (*ptask)(11,22);
    });

    int sum = fu.get();
    std::cout << sum << std::endl;
    thr.join();
    return 0;
}