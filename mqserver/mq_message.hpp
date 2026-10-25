#ifndef __M_MSG_H__
#define __M_MSG_H__
#include "../mqcommon/mq_logger.hpp"
#include "../mqcommon/mq_helper.hpp"
#include "../mqcommon/mq_msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <list>

namespace bitmq
{
#define DATAFILE_SUBFIX ".mqd"
#define TMPFILE_SUBFIX ".mqd.tmp"
    using MessagePtr = std::shared_ptr<bitmq::Message>;
    /* ========================================================================
     *  MessageMapper —— 文件层
     *  职责：只负责"内存对象 <-> 文件字节"的转换，不懂业务
     *  方法：createMsgFile / removeMsgFile / insert / remove / gc / load
     * ======================================================================*/
    class MessageMapper
    {
    public:
        MessageMapper(std::string &basedir, const std::string &qname) : _qname(qname)
        {
            // 1. 保证 basedir 以 '/' 结尾，方便后面拼路径
            if (basedir.back() != '/')
                basedir.push_back('/');
            // 2. 拼出本队列的两个文件路径
            //    data : ./data/queue1.mqd        (正式数据)
            //    tmp  : ./data/queue1.mqd.tmp    (GC 时的中转文件)
            _datafile = basedir + qname + DATAFILE_SUBFIX;
            _tmpfile = basedir + qname + TMPFILE_SUBFIX;
            // 3. 目录不存在则创建
            //    注意：这里不用 assert —— assert 在 Release 下会被整体删掉，
            //    那样连 createDirectory 都不会执行
            if (FileHelper(basedir).exists() == false)
            {
                if (FileHelper::createDirectory(basedir) == false)
                {
                    DLOG("创建队列数据目录 %s 失败！", basedir.c_str());
                    return;
                }
            }
            // 4. 创建正式数据文件（已存在则什么都不做）
            createMsgFile();
        }
        /* -------- 创建消息存储文件 -------- */
        bool createMsgFile()
        {
            // 已存在就直接返回，避免把已有数据清空
            if (FileHelper(_datafile).exists() == true)
            {
                return true;
            }
            bool ret = FileHelper::createFile(_datafile);
            if (ret == false)
            {
                DLOG("创建队列数据文件 %s 失败！", _datafile.c_str());
                return false;
            }
            return true;
        }
        /* -------- 删除消息存储文件（正式 + 临时）-------- */
        void removeMsgFile()
        {
            FileHelper::removeFile(_datafile);
            FileHelper::removeFile(_tmpfile);
        }
        /* -------- 新增持久化：把一条消息追加到文件末尾 -------- */
        bool insert(MessagePtr &msg)
        {
            return insert(_datafile, msg);
        }
        // -------- 删除持久化：不物理删除，把文件里的 valid 改成 '0' --------
        
        bool remove(MessagePtr &msg)
        {
            // 1. 把有效标志改成 '0'（表示这条数据已作废）
            msg->mutable_payload()->set_valid("0");
            // 2. 序列化（注意：只序列化 payload，offset/length 不进文件）
            std::string body = msg->payload().SerializeAsString();
            // 3. 长度校验：必须和当初存进去时一样长
            if (body.size() != msg->length())
            {
                DLOG("不能修改文件中的数据信息，因为新生成的数据与原数据长度不一致!");
                return false;
            }
            // 4. 写到文件中原位置（覆盖写）
            FileHelper helper(_datafile);
            bool ret = helper.write(body.c_str(), msg->offset(), body.size());
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据失败！");
                return false;
            }
            return true;
        }
        // -------- 垃圾回收：把文件里所有有效消息重写一遍 --------
         
        std::list<MessagePtr> gc()
        {
            bool ret;
            std::list<MessagePtr> result;
            // ① 读文件里所有有效消息
            ret = load(result);
            if (ret == false)
            {
                DLOG("加载有效数据失败！\n");
                return result;
            }
            // ② 逐个写进临时文件
            FileHelper::createFile(_tmpfile);
            for (auto &msg : result)
            {
                DLOG("向临时文件写入数据: %s", msg->payload().body().c_str());
                ret = insert(_tmpfile, msg); // 注意：这里会把 offset/length 改成新文件里的位置
                if (ret == false)
                {
                    DLOG("向临时文件写入消息数据失败！！");
                    return result;
                }
            }
            // ③ 删除源文件
            ret = FileHelper::removeFile(_datafile);
            if (ret == false)
            {
                DLOG("删除源文件失败！");
                return result;
            }
            // ④ 临时文件改名为正式文件
            ret = FileHelper(_tmpfile).rename(_datafile);
            if (ret == false)
            {
                DLOG("修改临时文件名称失败！");
                return result;
            }
            // ⑤ 返回有效消息（此时 offset/length 已经是新文件中的位置）
            return result;
        }

    private:
        // ================== 加载：扫描整个文件 ==================

        bool load(std::list<MessagePtr> &result)
        {
            FileHelper data_file_helper(_datafile);
            size_t offset = 0, msg_size;
            size_t fsize = data_file_helper.size();
            bool ret;
            while (offset < fsize)
            {
                // ---- 第 1 步：读 4 字节长度 ----
                ret = data_file_helper.read((char *)&msg_size, offset, sizeof(size_t));
                if (ret == false)
                {
                    DLOG("读取消息长度失败！");
                    return false;
                }
                offset += sizeof(size_t);
                // ---- 第 2 步：按长度读数据 ----
                std::string msg_body(msg_size, '\0');
                //  一定要把返回值接住，否则读失败会拿一堆 '\0' 去反序列化
                ret = data_file_helper.read(&msg_body[0], offset, msg_size);
                if (ret == false)
                {
                    DLOG("读取消息数据失败！");
                    return false;
                }
                offset += msg_size;
                // ---- 第 3 步：反序列化 ----
                MessagePtr msgp = std::make_shared<Message>();
                if (msgp->mutable_payload()->ParseFromString(msg_body) == false)
                {
                    DLOG("反序列化消息失败，跳过！");
                    continue;
                }
                // ---- 第 4 步：作废消息直接跳过（gc 会把它丢掉）----
                if (msgp->payload().valid() == "0")
                {
                    DLOG("加载到无效消息：%s", msgp->payload().body().c_str());
                    continue;
                }
                // ---- 第 5 步： 记录这条消息在文件中的位置 ----
                //   offset 此时指向"下一条消息的长度字段"，所以本条数据的起点是 offset - msg_size
                //   不写这两行的话 offset/length 会一直是 0，
                //   之后 remove() 就会从文件头开始覆盖写 —— 直接把文件写坏
                msgp->set_offset(offset - msg_size);
                msgp->set_length(msg_size);
                result.push_back(msgp);
            }
            return true;
        }
        // ============ 追加写：把一条消息写到指定文件末尾 ============
         
        bool insert(const std::string &filename, MessagePtr &msg)
        {
            // 1. 序列化
            std::string body = msg->payload().SerializeAsString();
            // 2. 取当前文件大小（就是追加位置）
            FileHelper helper(filename);
            size_t fsize = helper.size();
            size_t msg_size = body.size();
            // 3. 先写长度
            bool ret = helper.write((char *)&msg_size, fsize, sizeof(size_t));
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据长度失败！");
                return false;
            }
            // 4. 再写数据（紧跟在长度后面）
            ret = helper.write(body.c_str(), fsize + sizeof(size_t), body.size());
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据失败！");
                return false;
            }
            // 5.  更新内存中这条消息的位置信息
            //     data 段起点 = 文件原大小 + 长度字段的 4 字节
            msg->set_offset(fsize + sizeof(size_t));
            msg->set_length(body.size());
            return true;
        }

    private:
        std::string _qname;    // 队列名
        std::string _datafile; // 正式数据文件：./data/xxx.mqd
        std::string _tmpfile;  // 临时文件：./data/xxx.mqd.tmp
    };
    //QueueMessage —— 队列层
      
    class QueueMessage
    {
    public:
        using ptr = std::shared_ptr<QueueMessage>;
        // 构造：创建消息存储文件
        QueueMessage(std::string &basedir, const std::string &qname)
            : _mapper(basedir, qname), // 交给 MessageMapper 去建目录/建文件
              _qname(qname),
              _valid_count(0),
              _total_count(0)
        {
        }
        // ---------------- 新增消息---------------
        bool insert(const BasicProperties *bp, const std::string &body, DeliveryMode mode)
        {
            // 1. 构造消息对象
            MessagePtr msg = std::make_shared<Message>();
            msg->mutable_payload()->set_body(body);

            if (bp != nullptr)
            {
                msg->mutable_payload()->mutable_properties()->set_id(bp->id());
                msg->mutable_payload()->mutable_properties()->set_delivery_mode(mode); // ★ 用参数 mode
                msg->mutable_payload()->mutable_properties()->set_routing_key(bp->routing_key());
            }
            else
            {
                msg->mutable_payload()->mutable_properties()->set_id(UUIDHelper::uuid());
                msg->mutable_payload()->mutable_properties()->set_delivery_mode(mode); // ★ 用参数 mode
                msg->mutable_payload()->mutable_properties()->set_routing_key("");
            }

            std::unique_lock<std::mutex> lock(_mutex);

            // 2. 判断是否需要持久化 —— 看"消息自己的 delivery_mode"
            if (msg->payload().properties().delivery_mode() == DeliveryMode::DURABLE)
            {
                msg->mutable_payload()->set_valid("1");
                bool ret = _mapper.insert(msg);
                if (ret == false)
                {
                    DLOG("持久化存储消息：%s 失败了！", body.c_str());
                    return false;
                }
                _valid_count += 1;
                _total_count += 1;
                _durable_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
            }

            // 3. 内存管理（所有消息都进待推送链表）
            _msgs.push_back(msg);
            return true;
        }
        //  ---------------- 获取队首消息----------------
       
        MessagePtr front()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_msgs.size() == 0)
            {
                return MessagePtr(); // 空队列返回空指针，调用方判断即可
            }
            MessagePtr msg = _msgs.front();
            _msgs.pop_front();
            // 记录到待确认表，等 ack 时再删
            _waitack_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
            return msg;
        }
        // ---------------- 确认消息 / 删除待确认----------------
        bool remove(const std::string &msg_id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // 1. 查找待确认消息
            auto it = _waitack_msgs.find(msg_id);
            if (it == _waitack_msgs.end())
            {
                DLOG("没有找到要删除的消息：%s!", msg_id.c_str());
                return true;
            }
            // 2. 持久化消息才需要动文件
            if (it->second->payload().properties().delivery_mode() == DeliveryMode::DURABLE)
            {
                // 3. 文件里把 valid 标成 '0'（原地覆盖，长度不变）
                _mapper.remove(it->second);
                _durable_msgs.erase(msg_id);
                _valid_count -= 1; // 有效消息 -1
                gc();              // 内部自己判断要不要回收
            }
            // 4. 删内存记录
            _waitack_msgs.erase(msg_id);
            return true;
        }
        /* ---------------- 获取待消费数量---------------- */
        size_t getable_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msgs.size();
        }
        /* ---------------- 获取待确认数量---------------- */
        size_t waitack_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _waitack_msgs.size();
        }
        // ---------------- 恢复历史消息----------------
         
        bool recovery()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // gc() 同时完成两件事：读有效消息 + 重写文件
            _msgs = _mapper.gc();
            for (auto &msg : _msgs)
            {
                _durable_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
            }
            // 刚恢复时：文件里的有效消息 == 待推送消息 == 持久化消息
            _valid_count = _total_count = _msgs.size();
            return true;
        }
        /* ---------------- 销毁队列所有消息----------------
           删文件 + 清空三个容器 + 计数归零
          ------------------------------------------------------------*/
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeMsgFile();
            _msgs.clear();
            _durable_msgs.clear();
            _waitack_msgs.clear();
            _valid_count = 0;
            _total_count = 0;
        }
        /* ---------------- 队列是否为空----------------
           "空"的定义：待推送 + 待确认 都算
           因为被 front() 取走但还没 ack 的消息，逻辑上还属于这个队列
           （RabbitMQ 就是这个语义）
          ----------------------------------------------------------*/
        bool empty()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msgs.empty() && _waitack_msgs.empty();
        }
        /* ---------------- 统计接口 ---------------- */
        size_t total_count() // 持久化消息总量
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _total_count;
        }
        size_t durable_count() // 持久化消息 hash 的大小
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _durable_msgs.size();
        }

    private:
        /* ---------------- 是否需要垃圾回收 ----------------
           条件：持久化总量 > 2000 且有效比例 < 50%
           含义：文件里废数据太多（占了一半以上），值得重写一遍
          -------------------------------------------------*/
        bool GCCheck()
        {
            if (_total_count > 2000 && (_valid_count * 10 / _total_count) < 5)
            {
                return true;
            }
            return false;
        }
        // ---------------- 垃圾回收 ----------------
        
        void gc()
        {
            if (GCCheck() == false)
                return;
            // 1. 回收，拿到新的有效消息链表（位置已是新文件里的位置）
            std::list<MessagePtr> msgs = _mapper.gc();
            // 2. 待推送队列：只保留"没被持久化"的内存消息
            //    （持久化消息的重建交给下一步的 msgs）
            std::list<MessagePtr> pending;
            for (auto &msg : _msgs)
            {
                if (_durable_msgs.find(msg->payload().properties().id()) == _durable_msgs.end())
                {
                    pending.push_back(msg);
                }
            }
            _msgs.swap(pending);
            // 3. 重建持久化 hash，并同步 offset / length
            _durable_msgs.clear();
            for (auto &msg : msgs)
            {
                const std::string &id = msg->payload().properties().id();
                _durable_msgs.insert(std::make_pair(id, msg));
                // 如果这条消息正在等待确认，把新偏移同步过去
                // （_waitack_msgs 和 _durable_msgs 存的是同一个 shared_ptr，
                //   正常情况下本已同步，这里显式写一遍让语义更清楚）
                auto wait_it = _waitack_msgs.find(id);
                if (wait_it != _waitack_msgs.end())
                {
                    wait_it->second->set_offset(msg->offset());
                    wait_it->second->set_length(msg->length());
                }
            }
            // 4. 更新计数
            _valid_count = _total_count = msgs.size();
        }

    private:
        std::mutex _mutex;                                         // 保护本队列的所有数据
        std::string _qname;                                        // 队列名称
        std::list<MessagePtr> _msgs;                               // 待推送消息链表
        std::unordered_map<std::string, MessagePtr> _durable_msgs; // 持久化消息 hash
        std::unordered_map<std::string, MessagePtr> _waitack_msgs; // 待确认消息 hash
        size_t _valid_count;                                       // 有效消息数量
        size_t _total_count;                                       // 持久化消息总量
        MessageMapper _mapper;                                     // 持久化管理句柄
    };
       //MessageManager —— 对外总管理
    
    class MessageManager
    {
    public:
        using ptr = std::shared_ptr<MessageManager>;
        MessageManager(const std::string &basedir) : _basedir(basedir) {}
        /* ------------- 初始化队列消息管理结构（文档 3c-i）------------
         *  创建 QueueMessage（会建文件）并恢复历史消息
         *
         *  注意：这里"创建 + recovery + 放入 map"全程持锁。
         *  因为 recovery 要读文件，如果先放进 map 再在锁外恢复，
         *  别的线程会看到一个"还没加载历史消息"的半成品。
         * ------------------------------------------------------------*/
        void initQueueMessage(const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // 已经初始化过就直接返回（幂等）
            if (_queue_msgs.find(qname) != _queue_msgs.end())
            {
                return;
            }
            // QueueMessage 构造要求非 const 引用，所以拷一份
            std::string basedir = _basedir;
            QueueMessage::ptr qmp = std::make_shared<QueueMessage>(basedir, qname);
            // 从磁盘恢复这个队列的历史消息
            qmp->recovery();
            _queue_msgs.insert(std::make_pair(qname, qmp));
        }
        /* ------------- 删除队列结构 + 数据文件------------
           句柄要在锁外销毁：clear() 会删文件，属于慢操作
          ---------------------------------------------------------------*/
        void destroyQueueMessage(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    return;
                }
                qmp = it->second;
                _queue_msgs.erase(it); // 先从 map 摘掉，之后新消息进不来
            }
            qmp->clear(); // 锁外：删文件 + 清内存
        }
        /* ------------- 向指定队列新增消息------------ */
        bool insert(const std::string &qname, BasicProperties *bp,
                    const std::string &body, DeliveryMode mode)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("向队列%s新增消息失败：没有找到消息管理句柄!", qname.c_str());
                    return false;
                }
                qmp = it->second;
            }
            return qmp->insert(bp, body, mode);
        }
        /* ------------- 获取指定队列队首消息----------- */
        MessagePtr front(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s队首消息失败：没有找到消息管理句柄!", qname.c_str());
                    return MessagePtr();
                }
                qmp = it->second;
            }
            return qmp->front();
        }
        /* ------------- 确认消息（文档 3c-v）------------ */
        void ack(const std::string &qname, const std::string &msg_id)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("确认队列%s消息%s失败：没有找到消息管理句柄!", qname.c_str(), msg_id.c_str());
                    return;
                }
                qmp = it->second;
            }
            qmp->remove(msg_id);
        }
        /* ------------- 判断指定队列消息是否为空------------
         *  队列不存在时返回 true（视为"没有消息"），并按你的代码风格打日志
         * -----------------------------------------------------------------*/
        bool empty(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s消息是否为空失败：没有找到消息管理句柄!", qname.c_str());
                    return true;
                }
                qmp = it->second;
            }
            return qmp->empty();
        }
        /* ---------------- 统计接口 ---------------- */
        size_t getable_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s待推送消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->getable_count();
        }
        size_t total_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s总持久化消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->total_count();
        }
        size_t durable_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s有效持久化消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->durable_count();
        }
        size_t waitack_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s待确认消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->waitack_count();
        }
        /* ------------- 清空所有队列的消息（保留管理结构）------------ */
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto &qmsg : _queue_msgs)
            {
                qmsg.second->clear();
            }
        }

    private:
        std::mutex _mutex;                                              // 保护 _queue_msgs 这张表
        std::string _basedir;                                           // 所有队列数据文件的根目录
        std::unordered_map<std::string, QueueMessage::ptr> _queue_msgs; // 队列名 -> 队列消息管理器
    };
}
#endif