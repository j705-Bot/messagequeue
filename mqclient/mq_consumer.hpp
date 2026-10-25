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
}

#endif