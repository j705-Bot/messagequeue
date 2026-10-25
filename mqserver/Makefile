CFLAG = -g -std=c++11 -I../include -I../mqthird/include
LFLAG = -L../mqthird/build/release-cpp11/lib -lprotobuf -lsqlite3 -pthread -lmuduo_net -lmuduo_base -lz

mqserver: mq_server.cc ../mqcommon/mq_msg.pb.cc ../mqcommon/mq_proto.pb.cc ../include/muduo/proto/codec.cc
	g++ $(CFLAG) $^ -o $@ $(LFLAG)

clean:
	rm -f mqserver
