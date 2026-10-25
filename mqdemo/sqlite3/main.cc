#include "sqlite.hpp"
#include <cassert>

int select_stu_callback(void* arg,int col_count,char** result,char** fields_name) {
    std::vector<std::string> *arry = (std::vector<std::string>*)arg;
    arry->push_back(result[0]);
    return 0; //必须有！！！
}
int main()
{
    SqliteHelper helper("./test.db");
    //1. 创建/打开库文件
    assert(helper.open());
    //2. 创建表（不存在则创建）， 学生信息： 学号，姓名，年龄
    const char *ct = "create table if not exists student(sn int primary key, name varchar(32), age int);";
    assert(helper.exec(ct, nullptr, nullptr));
    //3. 新增数据 ， 修改， 删除， 查询
    const char *insert_sql = "insert into student values(1, '张小明', 18), (2, '小黑', 19), (3, '小红', 18);";
    assert(helper.exec(insert_sql, nullptr, nullptr));
    const char *update_sql = "update student set name='张小明' where sn=1";
    assert(helper.exec(update_sql, nullptr, nullptr));
    const char *delete_sql = "delete from student where sn=3";
    assert(helper.exec(delete_sql, nullptr, nullptr));

    const char *select_sql = "select name from student;";
    std::vector<std::string> arry;
    assert(helper.exec(select_sql, select_stu_callback, &arry));
    for (auto &name : arry) {
        std::cout << name << std::endl;
    }
    //4. 关闭数据库
    helper.close();
    return 0;
}