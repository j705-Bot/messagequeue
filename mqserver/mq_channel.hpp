
#ifndef __M_CHANNEL_H__
#define __M_CHANNEL_H__
#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
#include "../mqcommon/mq_logger.hpp"
#include "../mqcommon/mq_helper.hpp"
#include "../mqcommon/mq_msg.pb.h"
#include "../mqcommon/mq_proto.pb.h"
#include "../mqcommon/mq_threadpool.hpp"
#include "mq_consumer.hpp"
#include "mq_host.hpp"
#include "mq_route.hpp"

namespace bitmq {
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;
    using openChannelRequestPtr = std::shared_ptr<openChannelRequest>;
    using closeChannelRequestPtr = std::shared_ptr<closeChannelRequest>;
    using declareExchangeRequestPtr = std::shared_ptr<declareExchangeRequest>;
    using deleteExchangeRequestPtr = std::shared_ptr<deleteExchangeRequest>;
    using declareQueueRequestPtr = std::shared_ptr<declareQueueRequest>;
    using deleteQueueRequestPtr = std::shared_ptr<deleteQueueRequest>;
    using queueBindRequestPtr = std::shared_ptr<queueBindRequest>;
    using queueUnBindRequestPtr = std::shared_ptr<queueUnBindRequest>;
    using basicPublishRequestPtr = std::shared_ptr<basicPublishRequest>;
    using basicAckRequestPtr = std::shared_ptr<basicAckRequest>;
    using basicConsumeRequestPtr = std::shared_ptr<basicConsumeRequest>;
    using basicCancelRequestPtr = std::shared_ptr<basicCancelRequest>;
    class Channel {
        public:
            using ptr = std::shared_ptr<Channel>;
            Channel(const std::string &id, 
                const VirtualHost::ptr &host, 
                const ConsumerManager::ptr &cmp, 
                const ProtobufCodecPtr &codec, 
                const muduo::net::TcpConnectionPtr &conn,
                const threadpool::ptr &pool):
                _cid(id),
                _conn(conn),
                _codec(codec),
                _cmp(cmp),
                _host(host),
                _pool(pool){
                DLOG("new Channel: %p", this);
            }
            ~Channel() {
                if (_consumer.get() != nullptr) {
                    _cmp->remove(_consumer->tag, _consumer->qname);
                }
                DLOG("del Channel: %p", this);
            }
            //交换机的声明与删除
            void declareExchange(const declareExchangeRequestPtr &req) {
                bool ret = _host->declareExchange(req->exchange_name(), 
                    req->exchange_type(), req->durable(), 
                    req->auto_delete(), req->args());
                return basicResponse(ret, req->rid(), req->cid());
            }
            void deleteExchange(const deleteExchangeRequestPtr &req) {
                _host->deleteExchange(req->exchange_name());
                return basicResponse(true, req->rid(), req->cid());
            }
            //队列的声明与删除
            void declareQueue(const declareQueueRequestPtr &req) {
                bool ret = _host->declareQueue(req->queue_name(),
                    req->durable(), req->exclusive(),
                    req->auto_delete(), req->args());
                if (ret == false) {
                    return basicResponse(false, req->rid(), req->cid());
                }
                _cmp->initQueueConsumer(req->queue_name());//初始化队列的消费者管理句柄
                return basicResponse(true, req->rid(), req->cid());
            }
            void deleteQueue(const deleteQueueRequestPtr &req) {
                _cmp->destroyQueueConsumer(req->queue_name());
                _host->deleteQueue(req->queue_name());
                return basicResponse(true, req->rid(), req->cid());
            }
            //队列的绑定与解除绑定
            void queueBind(const queueBindRequestPtr &req) {
                bool ret = _host->bind(req->exchange_name(), 
                    req->queue_name(), req->binding_key());
                return basicResponse(ret, req->rid(), req->cid());
            }
            void queueUnBind(const queueUnBindRequestPtr &req) {
                _host->unBind(req->exchange_name(), req->queue_name());
                return basicResponse(true, req->rid(), req->cid());
            }
            //消息的发布
            void basicPublish(const basicPublishRequestPtr &req) {
                //1. 判断交换机是否存在
                auto ep = _host->selectExchange(req->exchange_name());
                if (ep.get() == nullptr) {
                    return basicResponse(false, req->rid(), req->cid());
                }
                //2. 进行交换路由，判断消息可以发布到交换机绑定的哪个队列中
                MsgQueueBindingMap mqbm = _host->exchangeBindings(req->exchange_name());
                BasicProperties *properties = nullptr;
                std::string routing_key;
                if (req->has_properties()) {
                    properties = req->mutable_properties();
                    routing_key = properties->routing_key();
                }
                for (auto &binding : mqbm) {
                    if (Router::route(ep->type, routing_key, binding.second->binding_key)) {
                        //3. 将消息添加到队列中（添加消息的管理）
                        _host->basicPublish(binding.first, properties, req->body());
                        //4. 向线程池中添加一个消息消费任务（向指定队列的订阅者去推送消息--线程池完成）
                        auto task = std::bind(&Channel::consume, this, binding.first);
                        _pool->push(task);
                    }
                }
                return basicResponse(true, req->rid(), req->cid());
            }
            //消息的确认
            void basicAck(const basicAckRequestPtr &req) {
                _host->basicAck(req->queue_name(), req->message_id());
                return basicResponse(true, req->rid(), req->cid());
            }
            //订阅队列消息
            void basicConsume(const basicConsumeRequestPtr &req) {
                //1. 判断队列是否存在
                bool ret = _host->existsQueue(req->queue_name());
                if (ret == false) {
                    return basicResponse(false, req->rid(), req->cid());
                }
                //2. 创建队列的消费者
                auto cb = std::bind(&Channel::callback, this, std::placeholders::_1,
                    std::placeholders::_2, std::placeholders::_3);
                //创建了消费者之后，当前的channel角色就是个消费者
                _consumer = _cmp->create(req->consumer_tag(), req->queue_name(), req->auto_ack(), cb);
                return basicResponse(true, req->rid(), req->cid());
            }
            //取消订阅
            void basicCancel(const basicCancelRequestPtr &req) {
                _cmp->remove(req->consumer_tag(), req->queue_name());
                return basicResponse(true, req->rid(), req->cid());
            }
        private:
            void callback(const std::string tag, const BasicProperties *bp, const std::string &body) {
                //针对参数组织出推送消息请求，将消息推送给channel对应的客户端
                basicConsumeResponse resp;
                resp.set_cid(_cid);
                resp.set_body(body);
                resp.set_consumer_tag(tag);
                if (bp) {
                    resp.mutable_properties()->set_id(bp->id());
                    resp.mutable_properties()->set_delivery_mode(bp->delivery_mode());
                    resp.mutable_properties()->set_routing_key(bp->routing_key());
                }
                _codec->send(_conn, resp);
            }
            void consume(const std::string &qname) {
                //指定队列消费消息
                //1. 从队列中取出一条消息
                MessagePtr mp = _host->basicConsume(qname);
                if (mp.get() == nullptr) {
                    DLOG("执行消费任务失败，%s 队列没有消息！", qname.c_str());
                    return;
                }
                //2. 从队列订阅者中取出一个订阅者
                Consumer::ptr cp = _cmp->choose(qname);
                if (cp.get() == nullptr) {
                    DLOG("执行消费任务失败，%s 队列没有消费者！", qname.c_str());
                    return;
                }
                //3. 调用订阅者对应的消息处理函数，实现消息的推送
                cp->callback(cp->tag, mp->mutable_payload()->mutable_properties(), mp->payload().body());
                //4. 判断如果订阅者是自动确认---不需要等待确认，直接删除消息，否则需要外部收到消息确认后再删除
                if (cp->auto_ack) _host->basicAck(qname, mp->payload().properties().id());
            }
            void basicResponse(bool ok, const std::string &rid, const std::string &cid) {
                basicCommonResponse resp;
                resp.set_rid(rid);
                resp.set_cid(cid);
                resp.set_ok(ok);
                _codec->send(_conn, resp);
            }
        private:
            std::string _cid;
            Consumer::ptr _consumer;
            muduo::net::TcpConnectionPtr _conn;
            ProtobufCodecPtr _codec;
            ConsumerManager::ptr _cmp;
            VirtualHost::ptr _host;
            threadpool::ptr _pool;
    };

    class ChannelManager {
        public:
            using ptr = std::shared_ptr<ChannelManager>;
            ChannelManager(){}
            bool openChannel(const std::string &id, 
                const VirtualHost::ptr &host, 
                const ConsumerManager::ptr &cmp, 
                const ProtobufCodecPtr &codec, 
                const muduo::net::TcpConnectionPtr &conn,
                const threadpool::ptr &pool) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _channels.find(id);
                if (it != _channels.end()) {
                    DLOG("信道：%s 已经存在!", id.c_str());
                    return false;
                }
                auto channel = std::make_shared<Channel>(id, host, cmp, codec, conn, pool);
                _channels.insert(std::make_pair(id, channel));
                return true;
            }
            void closeChannel(const std::string &id){
                std::unique_lock<std::mutex> lock(_mutex);
                _channels.erase(id);
            }
            Channel::ptr getChannel(const std::string &id) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _channels.find(id);
                if (it == _channels.end()) {
                    return Channel::ptr();
                }
                return it->second;
            }
        private:
            std::mutex _mutex;
            std::unordered_map<std::string, Channel::ptr> _channels;
    };
}

#endif