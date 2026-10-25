#ifndef __M_HOST_H__
#define __M_HOST_H__
#include "mq_exchange.hpp"
#include "mq_queue.hpp"
#include "mq_binding.hpp"
#include "mq_message.hpp"


namespace bitmq {

    class VirtualHost {
        public:
            using ptr = std::shared_ptr<VirtualHost>;
            VirtualHost(const std::string &hname, const std::string &basedir, const std::string &dbfile):
                _host_name(hname),
                _emp(std::make_shared<ExchangeManager>(dbfile)),
                _mqmp(std::make_shared<MsgQueueManager>(dbfile)),
                _bmp(std::make_shared<BindingManager>(dbfile)),
                _mmp(std::make_shared<MessageManager>(basedir)) {
                //获取到所有的队列信息，通过队列名称恢复历史消息数据
                QueueMap qm = _mqmp->allQueues();
                for (auto &q : qm) {
                    _mmp->initQueueMessage(q.first);
                }
            }
            bool declareExchange(const std::string &name,
                ExchangeType type, bool durable, bool auto_delete,
                const google::protobuf::Map<std::string, std::string> &args) {
                
                return _emp->declareExchange(name, type, durable, auto_delete, args);
            }
            void deleteExchange(const std::string &name) {
                //删除交换机的时候，需要将交换机相关的绑定信息也删除掉。
                _bmp->removeExchangeBindings(name);
                return _emp->deleteExchange(name);
            }
            bool existsExchange(const std::string &name) {
                return _emp->exists(name);
            }
            Exchange::ptr selectExchange(const std::string &ename) {
                return _emp->selectExchange(ename);
            }

            bool declareQueue(const std::string &qname, 
                bool qdurable, 
                bool qexclusive,
                bool qauto_delete,
                const google::protobuf::Map<std::string, std::string> &qargs) {
                //初始化队列的消息句柄（消息的存储管理）
                //队列的创建
                _mmp->initQueueMessage(qname);
                return _mqmp->declareQueue(qname, qdurable, qexclusive, qauto_delete, qargs);
            }
            void deleteQueue(const std::string &name) {
                //删除的时候队列相关的数据有两个：队列的消息，队列的绑定信息
                _mmp->destroyQueueMessage(name);
                _bmp->removeMsgQueueBindings(name);
                return _mqmp->deleteQueue(name);
            }
            bool existsQueue(const std::string &name) {
                return _mqmp->exists(name);
            }
            QueueMap allQueues() {
                return _mqmp->allQueues();
            }

            bool bind(const std::string &ename, const std::string &qname, const std::string &key) {
                Exchange::ptr ep = _emp->selectExchange(ename);
                if (ep.get() == nullptr) {
                    DLOG("进行队列绑定失败，交换机%s不存在！", ename.c_str());
                    return false;
                }
                MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
                if (mqp.get() == nullptr) {
                    DLOG("进行队列绑定失败，队列%s不存在！", qname.c_str());
                    return false;
                }
                return _bmp->bind(ename, qname, key, ep->durable && mqp->durable);
            }
            void unBind(const std::string &ename, const std::string &qname) {
                return _bmp->unBind(ename, qname);
            }
            MsgQueueBindingMap exchangeBindings(const std::string &ename) {
                return _bmp->getExchangeBindings(ename);
            }
            bool existsBinding(const std::string &ename, const std::string &qname) {
                return _bmp->exists(ename, qname);
            }

            bool basicPublish(const std::string &qname, BasicProperties *bp, const std::string &body)
            {
                MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
                if (mqp.get() == nullptr)
                {
                    DLOG("发布消息失败，队列%s不存在！", qname.c_str());
                    return false;
                }

                // 队列不持久化 -> 强制 UNDURABLE；队列持久化 -> 看消息自己的 delivery_mode
                DeliveryMode mode = DeliveryMode::UNDURABLE;
                if (mqp->durable)
                {
                    if (bp != nullptr)
                    {
                        // 只能比 == DURABLE：proto3 没设置过的字段是 UNKNOWMODE(0)
                        mode = (bp->delivery_mode() == DeliveryMode::DURABLE)
                                   ? DeliveryMode::DURABLE
                                   : DeliveryMode::UNDURABLE;
                    }
                    else
                    {
                        mode = DeliveryMode::DURABLE;
                    }
                }
                return _mmp->insert(qname, bp, body, mode);
            }


            MessagePtr basicConsume(const std::string &qname) {
                return _mmp->front(qname);
            }
            void basicAck(const std::string &qname, const std::string &msgid) {
                return _mmp->ack(qname, msgid);
            }  
            void clear() {
                _emp->clear();
                _mqmp->clear();
                _bmp->clear();
                _mmp->clear();
            } 
        private:
            std::string _host_name;
            ExchangeManager::ptr _emp;
            MsgQueueManager::ptr _mqmp;
            BindingManager::ptr _bmp;
            MessageManager::ptr _mmp;
    };
}

#endif
