#include <iostream>
#include <thread>
#include <future>

void Add(int num1,int num2,std::promise<int> &prom)
{
    prom.set_value(num1+num2);
    return;
}

int main()
{
    std::promise<int> prom;

    std::future<int> fu = prom.get_future();

    std::thread thr(Add,11,22,std::ref(prom));

    int res = fu.get();

    std::cout<< "sum:" << res << std::endl;

    thr.join();
    return 0;
}