#include <iostream>
#include <string>
#include "contacts.pb.h"

int main() {
    contacts::contact conn;
    conn.set_sn(10001);
    conn.set_name("xiaoming");
    conn.set_score(99.0f);
    std::string str = conn.SerializeAsString();

    contacts::contact stu;
    if (!stu.ParseFromString(str)) {
        std::cout << "parse failed" << std::endl;
        return -1;
    }
    std::cout << stu.sn() << std::endl;
    std::cout << stu.name() << std::endl;
    std::cout << stu.score() << std::endl;
    return 0;
}
