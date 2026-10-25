all: dict_client dict_server
dict_client::dict_client.cpp
	g++ -std=c++11 $^ -o $@ -I./include -L./lib -lmuduo_net -lmuduo_base -pthread
dict_server::dict_server.cpp
	g++ -std=c++11 $^ -o $@ -I./include -L./lib -lmuduo_net -lmuduo_base -pthread