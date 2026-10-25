#include "include/muduo/net/TcpClient.h"          // TcpClient：TCP 客户端封装
#include "include/muduo/net/EventLoopThread.h"    // EventLoopThread：把事件循环放到独立线程跑
#include "include/muduo/net/TcpConnection.h"      // TcpConnection：一条 TCP 连接
#include "include/muduo/net/Buffer.h"             // Buffer：接收缓冲区
#include "include/muduo/net/InetAddress.h"        // InetAddress：目标 IP + 端口
#include "include/muduo/base/CountDownLatch.h"    // CountDownLatch：倒计时门闩（线程同步）
#include <iostream>
#include <functional>                             // std::bind
#include <string>
class TranslateClient
{
public:
    // ------------------------------------------------------------------------
    // 构造函数
    // ------------------------------------------------------------------------
    // 初始化列表这里有个【必须注意的顺序问题】：
    //   1. _latch(1)  —— 计数初始化为 1（后面 wait 要等它减到 0）
    //   2. _client(..., _loopthread.startLoop(), ...)
    //        startLoop() 会：创建一个新线程 → 在新线程里跑 EventLoop → 返回 EventLoop*
    //        所以 _loopthread 必须【先于】_client 构造完成！
    //        这就是下面成员声明把 _loopthread 写在 _client 前面的原因。
    TranslateClient(const std::string& sip, int sport)
        : _latch(1),
          _client(_loopthread.startLoop(),              // 让事件循环跑在独立线程
                  muduo::net::InetAddress(sip, sport),  // 目标地址
                  "TranslateClient")                    // 客户端名字（日志用）
    {
        // 连接建立/断开时的回调
        _client.setConnectionCallback(
            std::bind(&TranslateClient::onConnection, this,
                      std::placeholders::_1));          // TcpConnectionPtr
        // 收到消息时的回调
        //  类名必须是 TranslateClient（照抄服务端会写成 TranslateServer）
        _client.setMessageCallback(
            std::bind(&TranslateClient::onMessage, this,
                      std::placeholders::_1,   // TcpConnectionPtr
                      std::placeholders::_2,   // Buffer*
                      std::placeholders::_3)); // Timestamp
    }
    //连接服务器--->需要阻塞等待连接建立成功之后再返回
    void connect()
    {
        _client.connect();    // 异步发起连接，立即返回
        _latch.wait();        //阻塞等待知道连接建立成功
        // 这里是【主线程】阻塞。等事件循环线程的 onConnection 里
        // 调用 _latch.countDown() 把计数减到 0，wait() 才会返回。
        // 所以 connect() 返回时，一定已经拿到了可用的 _conn。
    }
    bool send(const std::string& msg)
    {
        //   必须先判空！
        //    如果连接还没建立或已经断开，_conn 是空 shared_ptr，
        //    直接 -> 解引用会【段错误崩溃】。
        if (_conn && _conn->connected()) {
            _conn->send(msg);     // 写进连接的输出缓冲区，muduo 负责真正发出去
            return true;
        }
        return false;
    }
private:
    //连接建立成功之后的回调函数，连接建立成功，唤醒边上的阻塞
    // ------------------------------------------------------------------------
    //  这个回调在【事件循环线程】里执行，不是主线程
    //  同一个回调会被调用两次：
    //      第一次：连接建立成功 -> connected() == true
    //      第二次：连接断开     -> connected() == false
    // ------------------------------------------------------------------------
    void onConnection(const muduo::net::TcpConnectionPtr& conn)
    {
        if (conn->connected())      // <- 是 connected()，不是 countDown()
        {
            _latch.countDown();//唤醒主线程中的阻塞
            _conn = conn;           // 保存连接，主线程 send() 时要用
        }
        else
        {
            //连接关闭的操作
            _conn.reset();          // 清空 shared_ptr，避免主线程误用
        }
    }
    //收到消息之后的回调函数
    // ------------------------------------------------------------------------
    // 参数：1. 哪条连接  2. 接收缓冲区  3. 消息到达时间（这里用不到，省略参数名）
    //  是 muduo::Timestamp（大写 T）
    // ------------------------------------------------------------------------
    void onMessage(const muduo::net::TcpConnectionPtr& conn,
                   muduo::net::Buffer* buf,
                   muduo::Timestamp)
    {
        // retrieveAllAsString() = 取出缓冲区全部数据 + 清空缓冲区
        std::cout << "翻译结果: " << buf->retrieveAllAsString() << std::endl;
    }
private:
    //  成员声明顺序 = 初始化顺序（C++ 规则：按声明顺序构造，与初始化列表顺序无关）
    //    下面四个的顺序不能乱：
    //      _latch      先 → 后面 onConnection 要用
    //      _loopthread 先于 _client → 因为 _client 构造要用 startLoop()
    //      _client
    //      _conn
    muduo::CountDownLatch        _latch;        // 不是 muduo::base::CountDownLatch
    muduo::net::EventLoopThread  _loopthread;   // 事件循环跑在它创建的线程里
    muduo::net::TcpClient        _client;       // 注意是 muduo，不是 mudo
    muduo::net::TcpConnectionPtr _conn;         // 保存已建立的连接
};
int main()
{
    TranslateClient client("127.0.0.1", 8085);
    client.connect();     // 阻塞到连接真正建立才返回
    while (1)
    {
        std::string buf;
        //  用 cin >> 而不是 getline：
        //    cin >> 会【跳过空白字符】，读到的 buf 不带换行符 '\n'。
        //    这正好和服务端的字典 key（"hello" 而非 "hello\n"）匹配 ✓
        //    如果换成 getline，就会把 '\n' 一起读进来 → 服务端查不到 → "没听懂!!!"
        std::cin >> buf;
        if (!client.send(buf)) {
            std::cout << "发送失败：连接已断开" << std::endl;
        }
    }
    return 0;
}
