// ============================================================================
//  TranslateServer —— 基于 muduo 的翻译服务器（带详细注释版）
// ----------------------------------------------------------------------------
//  功能：客户端发一个词，服务端在字典里查它的翻译，把结果发回去
//
//  代码结构（muduo 的典型用法）：
//      1. 定义一个类，里面【持有】EventLoop + TcpServer
//      2. 在构造函数里【注册回调】——告诉 muduo"发生什么事件时调用我的哪个函数"
//      3. 调用 start() —— 启动服务器，进入事件循环（阻塞）
//
//  数据流：
//      客户端连接 ---> onConnection()   （连接建立/断开时触发）
//      客户端发数据 ---> onMessage()      （收到消息时触发）
//                          │
//                          ├─ 1. 从 Buffer 取出请求数据
//                          ├─ 2. 调用 translate() 翻译
//                          └─ 3. conn->send() 把结果发回客户端
// ============================================================================
#include "include/muduo/net/TcpServer.h"        // TcpServer：TCP 服务器封装
#include "include/muduo/net/EventLoop.h"        // EventLoop：epoll 事件循环（Reactor 核心）
#include "include/muduo/net/TcpConnection.h"    // TcpConnection：一条 TCP 连接
#include "include/muduo/net/InetAddress.h"      // InetAddress：IP + 端口
#include "include/muduo/net/Buffer.h"           // Buffer：muduo 的输入输出缓冲区
#include <iostream>
#include <functional>                           // std::bind / std::function
#include <unordered_map>                        // 字典
#include <string>
class TranslateServer {
public:
    // ------------------------------------------------------------------------
    // 构造函数：初始化 EventLoop 和 TcpServer，并注册回调
    // ------------------------------------------------------------------------
    // _server 的 4 个参数：
    //   1. &_baseloop   —— 用哪个事件循环（一个 loop 可以带多个 TcpServer）
    //   2. InetAddress  —— 监听地址，"0.0.0.0" 表示监听本机所有网卡
    //   3. "TranslateServer" —— 服务器名字，日志里会用它标识
    //   4. kReusePort   —— 端口复用：服务器重启时不用等 TIME_WAIT 超时
    TranslateServer(int port)
        : _server(&_baseloop,
                  muduo::net::InetAddress("0.0.0.0", port),
                  "TranslateServer",
                  muduo::net::TcpServer::kReusePort)
    {
        //将我们的类成员函数设置为服务器的回调处理函数
        //std::bind是一个函数适配器，对指定的函数进行参数绑定
        // std::bind 的三个参数：
        //   1. &类名::成员函数名  —— 要绑定的成员函数（注意 & 取地址）
        //   2. this               —— 用哪个对象去调用它（成员函数需要 this）
        //   3. std::placeholders::_N —— 占位符：表示"调用时才传入的第 N 个参数"
        //
        // 为什么需要 this？
        //   成员函数不能直接当 std::function 用，因为它暗含一个隐藏参数 this。
        //   bind 的作用就是把【成员函数 + 对象】打包成一个可以像普通函数一样调用的对象。
        //
        //  注意 muduo 的 setXxxCallback 只接受一个 std::function 参数，
        //    必须用 std::bind(...) 把整件事包成一体，不能写成 (函数, this, _1)。
        _server.setConnectionCallback(
            std::bind(&TranslateServer::onConnection, this,
                      std::placeholders::_1));
        //  这里是 onMessage，不是 onConnection！
        //    ConnectionCallback 参数个数：1 个（连接对象）
        //    MessageCallback   参数个数：3 个（连接对象、缓冲区、时间戳）
        _server.setMessageCallback(
            std::bind(&TranslateServer::onMessage, this,
                      std::placeholders::_1,   // TcpConnectionPtr
                      std::placeholders::_2,   // Buffer*
                      std::placeholders::_3)); // Timestamp
    }
    //开始运行(启动服务器)
    void start()
    {
        _server.start(); //开始事件监控
        // 上面这句做两件事：创建监听 socket、把它注册到 EventLoop 的 epoll 里
        _baseloop.loop(); //开始事件监控，这是一个死循环阻塞接口
        // loop() 内部是一个 while(!quit) 死循环，不断调用 epoll_wait：
        //   有事件 → 分发到对应的 Channel → 调用回调（就是上面注册的 onConnection/onMessage）
        //   没事件 → 阻塞等待
        // 所以这一行会让 main 函数卡在这里不返回，直到服务器被关闭。
    }
private:
    //onconnection应该建立在连接成功以及关闭的时候被调用
    // 触发时机：1. 新客户端连接上来  2. 连接断开
    // 注意：这个回调跑在【事件循环线程】里，不是新开线程
    void onConnection(const muduo::net::TcpConnectionPtr& conn)
    {
        // TcpConnectionPtr 是 shared_ptr<TcpConnection> 的类型别名，
        // 用智能指针管理连接对象的生命周期，避免手动 delete。
        //新连接建立成功时的回调函数
        if (conn->connected() == true)
        {
            // conn->peerAddress().toIpPort() 可以拿到对端 IP:PORT，调试很有用
            std::cout << "新连接建立成功!" << std::endl;
        }
        else
        {
            // 同一个回调负责"断开"，靠 connected() 区分状态
            std::cout << "新连接关闭!" << std::endl;
        }
    }
    // ------------------------------------------------------------------------
    // 翻译逻辑：在字典里查词
    // ------------------------------------------------------------------------
    // static：字典只构建一次，之后所有调用共用，避免每次请求都重建 map
    std::string translate(const std::string& str)
    {
        static std::unordered_map<std::string, std::string> dict_map =
        {
            {"hello",   "你好"},
            {"你好",     "hello"},
            {"Hello",   "你好"},
            {"吃了吗",   "油泼面"}
        };
        auto it = dict_map.find(str);
        if (it == dict_map.end())
        {
            return "没听懂!!!";
        }
        return it->second;
    }
    // ------------------------------------------------------------------------
    // 消息回调：收到客户端数据时触发
    // ------------------------------------------------------------------------
    // 参数说明：
    //   1. conn —— 是哪条连接发来的消息
    //   2. buf  —— muduo 的输入缓冲区，数据已经在里面了
    //   3. Timestamp —— 消息到达时间（这里用不到，所以省略参数名）
    //    是 muduo::Timestamp（大写 T），不是 timestamp
    void onMessage(const muduo::net::TcpConnectionPtr& conn,
                   muduo::net::Buffer* buf,
                   muduo::Timestamp)
    {
        //通信连接收到请求的回调函数
        //1.从buffer取请求的数据
        //   retrieveAllAsString() = 把缓冲区里【所有】可读数据取出来，并清空缓冲区。
        //   muduo 已经帮你处理好了 TCP 粘包/半包问题：
        //   —— 你拿到的是"这一批到达的字节"，按应用层协议自己解析。
        //   （简单 demo 里默认一次请求 = 一次 send 的内容）
        std::string str = buf->retrieveAllAsString();
        std::cout << "[调试] 收到 " << str.size() << " 字节" << std::endl; 
        //2.调用translate接口进行翻译
        std::string resp = translate(str);
        //3.对客户端及进行响应结果
        //   send() 会把数据写进连接的【输出缓冲区】，
        //   由 muduo 负责在 socket 可写时真正发出去（非阻塞 + 自动处理发不完的情况）。
        //   如果这个 demo 里想做简单的回显，可以用 conn->send(str) 直接原样返回。
        conn->send(resp);
    }
private:
    //  成员声明顺序 = 初始化顺序（很重要！）
    //    下面 _baseloop 必须声明在 _server 前面，
    //    因为 _server 的构造函数要用 &_baseloop，
    //    如果 _baseloop 还没构造完，传进去的就是野指针。
    //
    //_baseloop时epoll的事件控制，会进行描述符的事件监控，触发事件后进行io操作
    muduo::net::EventLoop _baseloop;
    //这个server对象，主要用于设置回调函数，用于告诉服务器收到什么请求该如何处理
    muduo::net::TcpServer _server;
};
int main()
{
    TranslateServer server(8085);   // 监听 8085 端口
    server.start();                 // 启动并阻塞在事件循环里
    return 0;
}

