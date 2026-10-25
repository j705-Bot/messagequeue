#include "../mqserver/mq_connection.hpp"

int main()
{
    bitmq::ConnectionManager::ptr cmp = std::make_shared<bitmq::ConnectionManager>();
    cmp->newConnection(std::make_shared<bitmq::VirtualHost>("host1", "./data/host1/message/", "./data/host1/host1.db"),
        std::make_shared<bitmq::ConsumerManager>(),
        bitmq::ProtobufCodecPtr(),
        muduo::net::TcpConnectionPtr(),
        threadpool::ptr());
    return 0;
}