#ifndef __M_CHANNEL_H__
#define __M_CHANNEL_H__
#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
#include "../mqcommon/mq_logger.hpp"
#include "../mqcommon/mq_helper.hpp"
#include "../mqcommon/mq_msg.pb.h"
#include "../mqcommon/mq_proto.pb.h"
#include "mq_consumer.hpp"
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

namespace bitmq {
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;
    using basicConsumeResponsePtr = std::shared_ptr<basicConsumeResponse>;
    using basicCommonResponsePtr = std::shared_ptr<basicCommonResponse>;
    class Channel {
        public:
            using ptr = std::shared_ptr<Channel>;
            Channel(const muduo::net::TcpConnectionPtr& conn, const ProtobufCodecPtr& codec):
                _cid(UUIDHelper::uuid()), _conn(conn), _codec(codec) {}
            ~Channel() { basicCancel(); }
            std::string cid() { return _cid; }
            bool openChannel() {
                std::string rid = UUIDHelper::uuid();
                openChannelRequest req; 
                req.set_rid(rid);
                req.set_cid(_cid);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            void closeChannel() {
                std::string rid = UUIDHelper::uuid();
                closeChannelRequest req; 
                req.set_rid(rid);
                req.set_cid(_cid);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }
            bool declareExchange(
                const std::string &name,
                ExchangeType type, 
                bool durable, 
                bool auto_delete,
                google::protobuf::Map<std::string, std::string> &args) {
                //构造一个声明虚拟机的请求对象，
                std::string rid = UUIDHelper::uuid();
                declareExchangeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(name);
                req.set_exchange_type(type);
                req.set_durable(durable);
                req.set_auto_delete(auto_delete);
                req.mutable_args()->swap(args);
                //然后向服务器发送请求
                _codec->send(_conn, req);
                //等待服务器的响应
                basicCommonResponsePtr resp = waitResponse(rid);
                //返回
                return resp->ok();
            }
            void deleteExchange(const std::string &name) {
                std::string rid = UUIDHelper::uuid();
                deleteExchangeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(name);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }

            bool declareQueue(
                const std::string &qname, 
                bool qdurable, 
                bool qexclusive,
                bool qauto_delete,
                google::protobuf::Map<std::string, std::string> &qargs) {
                std::string rid = UUIDHelper::uuid();
                declareQueueRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(qname);
                req.set_durable(qdurable);
                req.set_auto_delete(qauto_delete);
                req.set_exclusive(qexclusive);
                req.mutable_args()->swap(qargs);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            void deleteQueue(const std::string &qname) {
                std::string rid = UUIDHelper::uuid();
                deleteQueueRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(qname);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }

            bool queueBind(
                const std::string &ename, 
                const std::string &qname, 
                const std::string &key) {
                std::string rid = UUIDHelper::uuid();
                queueBindRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(ename);
                req.set_queue_name(qname);
                req.set_binding_key(key);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            void queueUnBind(const std::string &ename, const std::string &qname) {
                std::string rid = UUIDHelper::uuid();
                queueUnBindRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(ename);
                req.set_queue_name(qname);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }

            void basicPublish(
                const std::string &ename,
                const BasicProperties *bp,
                const std::string &body) {
                std::string rid = UUIDHelper::uuid();
                basicPublishRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_body(body);
                req.set_exchange_name(ename);
                if (bp != nullptr) {
                    req.mutable_properties()->set_id(bp->id());
                    req.mutable_properties()->set_delivery_mode(bp->delivery_mode());
                    req.mutable_properties()->set_routing_key(bp->routing_key());
                }
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }

            void basicAck(const std::string &msgid) {
                if (_consumer.get() == nullptr) {
                    DLOG("消息确认时，找不到消费者信息！");
                    return ;
                }
                std::string rid = UUIDHelper::uuid();
                basicAckRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(_consumer->qname);
                req.set_message_id(msgid);
                _codec->send(_conn, req);
                waitResponse(rid);
                return;
            }
            
            void basicCancel() {
                if (_consumer.get() == nullptr) {
                    return ;
                }
                std::string rid = UUIDHelper::uuid();
                basicCancelRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(_consumer->qname);
                req.set_consumer_tag(_consumer->tag);
                _codec->send(_conn, req);
                waitResponse(rid);
                _consumer.reset();
                return;
            }

            bool basicConsume(
                const std::string &consumer_tag,
                const std::string &queue_name,
                bool auto_ack,
                const ConsumerCallback &cb) {
                if (_consumer.get() != nullptr) {
                    DLOG("当前信道已订阅其他队列消息！");
                    return false;
                }
                std::string rid = UUIDHelper::uuid();
                basicConsumeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(queue_name);
                req.set_consumer_tag(consumer_tag);
                req.set_auto_ack(auto_ack);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp =  waitResponse(rid);
                if (resp->ok() == false) {
                    DLOG("添加订阅失败！");
                    return false;
                }
                _consumer = std::make_shared<Consumer>(consumer_tag, queue_name, auto_ack, cb);
                return true;
            }
        public:   
            //连接收到基础响应后，向hash_map中添加响应
            void putBasicResponse(const basicCommonResponsePtr& resp) {
                std::unique_lock<std::mutex> lock(_mutex);
                _basic_resp.insert(std::make_pair(resp->rid(), resp));
                _cv.notify_all();
            }
            
            //连接收到消息推送后，需要通过信道找到对应的消费者对象，通过回调函数进行消息处理
            void consume(const basicConsumeResponsePtr& resp) {
                if (_consumer.get() == nullptr) {
                    DLOG("消息处理时，未找到订阅者信息！");
                    return;
                }
                if (_consumer->tag != resp->consumer_tag()) {
                    DLOG("收到的推送消息中的消费者标识，与当前信道消费者标识不一致！");
                    return ;
                }
                _consumer->callback(resp->consumer_tag(), resp->mutable_properties(), resp->body());
            }
        private:
            basicCommonResponsePtr waitResponse(const std::string &rid) {
                std::unique_lock<std::mutex> lock(_mutex);
                _cv.wait(lock, [&rid, this](){
                    return _basic_resp.find(rid) != _basic_resp.end();
                });
                //while(condition()) _cv.wait();
                basicCommonResponsePtr basic_resp = _basic_resp[rid];
                _basic_resp.erase(rid);
                return basic_resp;
            }
        private:
            std::string _cid;
            muduo::net::TcpConnectionPtr _conn;
            ProtobufCodecPtr _codec;
            Consumer::ptr _consumer;
            std::mutex _mutex;
            std::condition_variable _cv;
            std::unordered_map<std::string, basicCommonResponsePtr> _basic_resp;
    };

    class ChannelManager {
        public:
            using ptr = std::shared_ptr<ChannelManager>;
            ChannelManager(){}
            Channel::ptr create(const muduo::net::TcpConnectionPtr &conn, 
                const ProtobufCodecPtr &codec) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto channel = std::make_shared<Channel>(conn, codec);
                _channels.insert(std::make_pair(channel->cid(), channel));
                return channel;
            }
            void remove(const std::string &cid) {
                std::unique_lock<std::mutex> lock(_mutex);
                _channels.erase(cid);
            }
            Channel::ptr get(const std::string &cid) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _channels.find(cid);
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