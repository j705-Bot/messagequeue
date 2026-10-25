#ifndef __M_THRPOOL_H__
#define __M_THRPOOL_H__
#include <iostream>
#include <functional>
#include <memory>
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>
#include <vector>

class threadpool {
    public:
        using ptr = std::shared_ptr<threadpool>;
        using Functor = std::function<void(void)>;
        threadpool(int thr_count = 1) : _stop(false){
            for (int i = 0; i < thr_count; i++) {
                _threads.emplace_back(&threadpool::entry, this);
            }
        }
        ~threadpool() {
            stop();
        }
        void stop() {
            if (_stop == true) return;
            _stop = true;
            _cv.notify_all();
            for (auto &thread : _threads) {
                thread.join();
            }
        }
        //push传入的是首先有一个函数--用户要执行的函数， 接下来是不定参，表示要处理的数据也就是要传入到函数中的参数
        //push函数内部，会将这个传入的函数封装成一个异步任务（packaged_task），
        //使用lambda生成一个可调用对象（内部执行异步任务），抛入到任务池中，由工作线程取出进行执行
        template<typename F, typename ...Args>
        auto push(F &&func, Args&& ...args) -> std::future<decltype(func(args...))> {
            //1. 将传入的函数封装成一个packaged_task任务
            using return_type = decltype(func(args...));
            auto tmp_func = std::bind(std::forward<F>(func), std::forward<Args>(args)...);
            auto task = std::make_shared<std::packaged_task<return_type()>>(tmp_func);
            std::future<return_type> fu = task->get_future();
            //2. 构造一个lambda匿名函数（捕获任务对象），函数内执行任务对象
            {
                std::unique_lock<std::mutex> lock(_mutex);
                //3. 将构造出来的匿名函数对象，抛入到任务池中
                _taskpool.push_back(   [task](){ (*task)(); }  );
                _cv.notify_one();
            }
            return fu;
        }

    private:
        //线程入口函数---内部不断的从任务池中取出任务进行执行。
        void entry() {
            while(!_stop){
                std::vector<Functor> tmp_taskpool;
                {
                    //加锁
                    std::unique_lock<std::mutex> lock(_mutex);
                    //等待任务池不为空，或者_stop被置位返回，
                    _cv.wait(lock, [this](){ return _stop || !_taskpool.empty(); });
                    //取出任务进行执行
                    tmp_taskpool.swap(_taskpool);
                }
                for (auto &task : tmp_taskpool) {
                    task();
                }
            }
        }
    private:
        std::atomic<bool> _stop;
        std::vector<Functor> _taskpool;//任务池
        std::mutex _mutex;
        std::condition_variable _cv;
        std::vector<std::thread> _threads;
};
#endif