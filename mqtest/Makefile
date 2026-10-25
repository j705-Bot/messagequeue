.PHONY: all clean

all: mq_filetest mq_exchangetest mq_queuetest mq_bindingtest mq_messagetest mq_hosttest mq_routetest mq_consumertest mq_connectiontest mq_channeltest

mq_filetest: mq_filetest.cpp
	g++ -g -std=c++11 $^ -o $@

mq_exchangetest: mq_exchangetest.cpp
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_queuetest: mq_queuetest.cpp
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_bindingtest: mq_bindingtest.cpp
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_messagetest: mq_messagetest.cpp ../mqcommon/mq_msg.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_hosttest:mq_hosttest.cpp ../mqcommon/mq_msg.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_routetest:mq_routetest.cpp ../mqcommon/mq_msg.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_consumertest:mq_consumertest.cpp ../mqcommon/mq_msg.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3

mq_connectiontest: mq_connectiontest.cpp ../mqcommon/mq_msg.pb.cc ../mqcommon/mq_proto.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3 -I../include -I../include

mq_channeltest:mq_channeltest.cpp ../mqcommon/mq_msg.pb.cc ../mqcommon/mq_proto.pb.cc
	g++ -g -std=c++11 $^ -o $@ -lgtest -lprotobuf -lsqlite3 -I../include

clean:
	rm -f mq_filetest mq_exchangetest mq_queuetest mq_bindingtest mq_messagetest mq_hosttest mq_routetest mq_consumertest mq_connectiontest mq_channeltest

