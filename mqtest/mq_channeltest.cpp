#include "../mqserver/mq_channel.hpp"

int main()
{
    bitmq::ChannelManager::ptr cmp = std::make_shared<bitmq::ChannelManager>();

    cmp->openChannel("c1", 
        std::make_shared<bitmq::VirtualHost>("host1", "./data/host1/message/", "./data/host1/host1.db"),
        std::make_shared<bitmq::ConsumerManager>(),
        bitmq::ProtobufCodecPtr(),
        muduo::net::TcpConnectionPtr(),
        threadpool::ptr());
    return 0;
}