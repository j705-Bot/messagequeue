/*
    封装实现一个SqliteHelper类，提供简单的sqlite数据库操作接口，完成数据的基础增删改查操作。
        1. 创建/打开数据库文件
        2. 针对打开的数据库执行操作
            1. 表的操作
            2. 数据的操作
        3. 关闭数据库
*/
#include <iostream>
#include <string>
#include <vector>
#include <sqlite3.h>

class SqliteHelper {
    public:
        typedef int(*SqliteCallback)(void*,int,char**,char**);
        SqliteHelper(const std::string &dbfile) : _dbfile(dbfile), _handler(nullptr){}
        
        bool open(int safe_leve = SQLITE_OPEN_FULLMUTEX) {
            //int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs );
            int ret = sqlite3_open_v2(_dbfile.c_str(), &_handler, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | safe_leve, nullptr);
            if (ret != SQLITE_OK) {
                std::cout << "创建/打开sqlite数据库失败: ";
                std::cout << sqlite3_errmsg(_handler) << std::endl;
                return false;
            }
            return true;
        }
        bool exec(const std::string &sql, SqliteCallback cb, void *arg) {
            //int sqlite3_exec(sqlite3*, char *sql, int (*callback)(void*,int,char**,char**), void* arg, char **err)
            int ret = sqlite3_exec(_handler, sql.c_str(), cb, arg, nullptr);
            if (ret != SQLITE_OK) {
                std::cout << sql << std::endl;
                std::cout << "执行语句失败: ";
                std::cout << sqlite3_errmsg(_handler) << std::endl;
                return false;
            }
            return true;
        }
        void close() {
            //int sqlite3_close_v2(sqlite3*);
            if (_handler) sqlite3_close_v2(_handler);
        }
    private:
        std::string _dbfile;
        sqlite3 *_handler;
};