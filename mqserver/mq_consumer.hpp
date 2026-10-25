#ifndef __M_CONSUMER_H__
#define __M_CONSUMER_H__
#include "../mqcommon/mq_logger.hpp"
#include "../mqcommon/mq_helper.hpp"
#include "../mqcommon/mq_msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <vector>
#include <functional>


namespace bitmq {
    using ConsumerCallback = std::function<void(const std::string, const BasicProperties *bp, const std::string)>;
    struct Consumer {
        using ptr = std::shared_ptr<Consumer>;
        std::string tag;    //消费者标识
        std::string qname;  //消费者订阅的队列名称
        bool auto_ack;      //自动确认标志
        ConsumerCallback callback;

        Consumer(){
            DLOG("new Consumer: %p", this);
        }
        Consumer(const std::string &ctag, const std::string &queue_name,  bool ack_flag, const ConsumerCallback &cb):
            tag(ctag), qname(queue_name), auto_ack(ack_flag), callback(std::move(cb)) {
            DLOG("new Consumer: %p", this);
        }
        ~Consumer() {
            DLOG("del Consumer: %p", this);
        }
    };

    //以队列为单元的消费者管理结构
    class QueueConsumer {
        public:
            using ptr = std::shared_ptr<QueueConsumer>;
            QueueConsumer(const std::string &qname) : _qname(qname), _rr_seq(0){}
            //队列新增消费者
            Consumer::ptr create(const std::string &ctag, const std::string &queue_name,  bool ack_flag, const ConsumerCallback &cb) {
                //1. 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                //2. 判断消费者是否重复
                for (auto &consumer : _consumers) {
                    if (consumer->tag == ctag) {
                        return Consumer::ptr();
                    }
                }
                //3. 没有重复则新增--构造对象
                auto consumer = std::make_shared<Consumer>(ctag, queue_name, ack_flag, cb);
                //4. 添加管理后返回对象
                _consumers.push_back(consumer);
                return consumer;
            }
            //队列移除消费者
            void remove(const std::string &ctag) {
                //1. 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                //2. 遍历查找-删除
                for (auto it = _consumers.begin(); it != _consumers.end(); ++it) {
                    if ((*it)->tag == ctag) {
                        _consumers.erase(it);
                        return ;
                    }
                }
                return;
            }
            //队列获取消费者：RR轮转获取
            Consumer::ptr choose() {
                //1. 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                if (_consumers.size() == 0) {
                    return Consumer::ptr();
                }
                //2. 获取当前轮转到的下标
                int idx = _rr_seq % _consumers.size();
                _rr_seq++;
                //3. 获取对象，返回
                return _consumers[idx];
            }
            //是否为空
            bool empty() {
                std::unique_lock<std::mutex> lock(_mutex);
                return _consumers.size() == 0;
            }
            //判断指定消费者是否存在
            bool exists(const std::string &ctag) {
                std::unique_lock<std::mutex> lock(_mutex);
                //2. 遍历查找
                for (auto it = _consumers.begin(); it != _consumers.end(); ++it) {
                    if ((*it)->tag == ctag) {
                        return true;
                    }
                }
                return false;
            }
            //清理所有消费者
            void clear() {
                std::unique_lock<std::mutex> lock(_mutex);
                _consumers.clear();
                _rr_seq = 0;
            }
        private:
            std::string _qname;
            std::mutex _mutex;
            uint64_t _rr_seq;//轮转序号
            std::vector<Consumer::ptr> _consumers;
    };

    class ConsumerManager {
        public:
            using ptr = std::shared_ptr<ConsumerManager>;
            ConsumerManager(){}
            void initQueueConsumer(const std::string &qname) {
                //1. 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                //2. 重复判断
                auto it = _qconsumers.find(qname);
                if (it != _qconsumers.end()) {
                    return ;
                }
                //3. 新增
                auto qconsumers = std::make_shared<QueueConsumer>(qname);
                _qconsumers.insert(std::make_pair(qname, qconsumers));
            }
            void destroyQueueConsumer(const std::string &qname) {
                std::unique_lock<std::mutex> lock(_mutex);
                _qconsumers.erase(qname);
            }
            Consumer::ptr create(const std::string &ctag, const std::string &queue_name,  bool ack_flag, const ConsumerCallback &cb) {
                //获取队列的消费者管理单元句柄，通过句柄完成新建
                QueueConsumer::ptr qcp;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _qconsumers.find(queue_name);
                    if (it == _qconsumers.end()) {
                        DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                        return Consumer::ptr();
                    }
                    qcp = it->second;
                }
                return qcp->create(ctag, queue_name, ack_flag, cb);
            }
            void remove(const std::string &ctag, const std::string &queue_name) {
                QueueConsumer::ptr qcp;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _qconsumers.find(queue_name);
                    if (it == _qconsumers.end()) {
                        DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                        return ;
                    }
                    qcp = it->second;
                }
                return qcp->remove(ctag);
            }
            Consumer::ptr choose(const std::string &queue_name) {
                QueueConsumer::ptr qcp;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _qconsumers.find(queue_name);
                    if (it == _qconsumers.end()) {
                        DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                        return Consumer::ptr();
                    }
                    qcp = it->second;
                }
                return qcp->choose();
            }
            bool empty(const std::string &queue_name) {
                QueueConsumer::ptr qcp;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _qconsumers.find(queue_name);
                    if (it == _qconsumers.end()) {
                        DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                        return false;
                    }
                    qcp = it->second;
                }
                return qcp->empty();
            }
            bool exists(const std::string &ctag, const std::string &queue_name) {
                QueueConsumer::ptr qcp;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _qconsumers.find(queue_name);
                    if (it == _qconsumers.end()) {
                        DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                        return false;
                    }
                    qcp = it->second;
                }
                return qcp->exists(ctag);
            }
            void clear() {
                std::unique_lock<std::mutex> lock(_mutex);
                _qconsumers.clear();
            }
        private:
            std::mutex _mutex;
            std::unordered_map<std::string, QueueConsumer::ptr> _qconsumers;
    };
}

#endif