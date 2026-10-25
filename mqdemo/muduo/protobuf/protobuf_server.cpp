#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpServer.h"

#include "request.pb.h"

#include <iostream>
#include <unordered_map>
#include <memory>

using namespace std::placeholders;

class Server
{
public:
    // 智能指针别名：必须与 dispatcher.h 内部使用的一致。
    // 若编译报 "参数类型不匹配" 或错误信息里出现 boost::shared_ptr，
    // 把下面所有 std::shared_ptr 改成 boost::shared_ptr，并 #include <boost/shared_ptr.hpp>
    typedef std::shared_ptr<bit::TranslateRequest>     TranslateRequestPtr;
    typedef std::shared_ptr<bit::TranslateResponse>    TranslateResponsePtr;
    typedef std::shared_ptr<bit::AddRequest>           AddRequestPtr;
    typedef std::shared_ptr<bit::AddResponse>          AddResponsePtr;
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;

    Server(int port)
        : _baseloop()
        , _server(&_baseloop,
                  muduo::net::InetAddress("0.0.0.0", port),
                  "Server",
                  muduo::net::TcpServer::kReusePort)
        , _dispatcher(std::bind(&Server::onUnknownMessage, this, _1, _2, _3))
        , _codec(std::bind(&ProtobufDispatcher::onProtobufMessage,
                           &_dispatcher,
                           _1, _2, _3))
    {
        // 注册业务请求处理函数
        _dispatcher.registerMessageCallback<bit::TranslateRequest>(
            std::bind(&Server::onTranslate, this, _1, _2, _3));

        _dispatcher.registerMessageCallback<bit::AddRequest>(
            std::bind(&Server::onAdd, this, _1, _2, _3));

        // 把 Codec 的处理函数装进网络层的消息回调槽
        _server.setMessageCallback(
            std::bind(&ProtobufCodec::onMessage, &_codec, _1, _2, _3));

        _server.setConnectionCallback(
            std::bind(&Server::onConnection, this, _1));
    }

    void start()
    {
        _server.start();
        _baseloop.loop();
    }

private:
    std::string translate(const std::string& str)
    {
        static std::unordered_map<std::string, std::string> dict_map = {
            {"hello", "你好"},
            {"你好",  "hello"},
            {"Hello", "你好"},
            {"吃了吗", "油泼面"}
        };
        auto it = dict_map.find(str);
        if (it == dict_map.end())
        {
            return "没听懂!!!";
        }
        return it->second;
    }

    void onTranslate(const muduo::net::TcpConnectionPtr& conn,
                     const TranslateRequestPtr& message,
                     muduo::Timestamp)
    {
        // 1. 提取需要翻译的内容
        std::string req_msg = message->msg();
        // 2. 翻译
        std::string rsp_msg = translate(req_msg);
        // 3. 组织 protobuf 响应
        bit::TranslateResponse resp;
        resp.set_msg(rsp_msg);
        // 4. 发送响应
        _codec.send(conn, resp);
    }

    void onAdd(const muduo::net::TcpConnectionPtr& conn,
               const AddRequestPtr& message,
               muduo::Timestamp)
    {
        int num1 = message->num1();
        int num2 = message->num2();
        int result = num1 + num2;

        bit::AddResponse resp;
        resp.set_result(result);
        _codec.send(conn, resp);
    }

    void onUnknownMessage(const muduo::net::TcpConnectionPtr& conn,
                          const MessagePtr& message,
                          muduo::Timestamp)
    {
        LOG_INFO << "onUnknownMessage: " << message->GetTypeName();
        conn->shutdown();
    }

    void onConnection(const muduo::net::TcpConnectionPtr& conn)
    {
        if (conn->connected())
        {
            LOG_INFO << "新连接连接成功: " << conn->peerAddress().toIpPort();
        }
        else
        {
            LOG_INFO << "连接即将关闭: " << conn->peerAddress().toIpPort();
        }
    }

private:
    // 声明顺序 = 初始化顺序，被依赖者必须在前
    muduo::net::EventLoop  _baseloop;
    muduo::net::TcpServer  _server;
    ProtobufDispatcher     _dispatcher;
    ProtobufCodec          _codec;
};

int main()
{
    Server server(8085);
    server.start();
    return 0;
}