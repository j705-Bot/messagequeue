.PHONY: publish_client consume_client
all:publish_client consume_client
publish_client : publish_client.cc ../mqcommon/mq_proto.pb.cc ../mqcommon/mq_msg.pb.cc ../include/muduo/proto/codec.cc
	g++ -g -std=c++11 $^ -o $@ -I../include -L../lib  -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz
consume_client : consume_client.cc ../mqcommon/mq_proto.pb.cc ../mqcommon/mq_msg.pb.cc ../include/muduo/proto/codec.cc
	g++ -g -std=c++11 $^ -o $@ -I../include -L../lib  -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz