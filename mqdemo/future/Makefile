all: async promise  package_task  threadpool     

async: async.cc
	g++ -std=c++11 $^ -o $@ -pthread

promise: promise.cc
	g++ -std=c++11 $^ -o $@ -pthread

package_task: package_task.cc
	g++ -std=c++11 $^ -o $@ -pthread

threadpool: threadpool.cpp
	g++ -std=c++11 $^ -o $@ -pthread


clean:
	rm -f async promise package_task threadpool

.PHONY: all clean