MUDUO := /home/hjq/mq/third/muduo-master

all: client server

client: protobuf_client.cpp request.pb.cc codec.cc
	g++ -std=c++11 protobuf_client.cpp request.pb.cc codec.cc -o client -I$(MUDUO) -I../include -I. -L../lib -lmuduo_net -lmuduo_base -lprotobuf -pthread -lz

server: protobuf_server.cpp request.pb.cc codec.cc
	g++ -std=c++11 protobuf_server.cpp request.pb.cc codec.cc -o server -I$(MUDUO) -I../include -I. -L../lib -lmuduo_net -lmuduo_base -lprotobuf -pthread -lz

clean:
	rm -f client server

.PHONY: all clean