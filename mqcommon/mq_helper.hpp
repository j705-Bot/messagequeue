#ifndef __M_HELPER_H__
#define __M_HELPER_H__
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <random>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <sqlite3.h>
#include <cstring>
#include <cerrno>
#include <sys/stat.h>
#include "mq_logger.hpp"

namespace bitmq{
class SqliteHelper {
    public:
        typedef int(*SqliteCallback)(void*,int,char**,char**);
        SqliteHelper(const std::string &dbfile) : _dbfile(dbfile), _handler(nullptr){}
        
        bool open(int safe_leve = SQLITE_OPEN_FULLMUTEX) {
            //int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs );
            int ret = sqlite3_open_v2(_dbfile.c_str(), &_handler, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | safe_leve, nullptr);
            if (ret != SQLITE_OK) {
                ELOG("创建/打开sqlite数据库失败: %s", sqlite3_errmsg(_handler));
                return false;
            }
            return true;
        }
        bool exec(const std::string &sql, SqliteCallback cb, void *arg) {
            //int sqlite3_exec(sqlite3*, char *sql, int (*callback)(void*,int,char**,char**), void* arg, char **err)
            int ret = sqlite3_exec(_handler, sql.c_str(), cb, arg, nullptr);
            if (ret != SQLITE_OK) {
                ELOG("%s \n语句执行失败: %s", sql.c_str(), sqlite3_errmsg(_handler));
                return false;
            }
            return true;
        }
        void close() {
            if (_handler) sqlite3_close_v2(_handler);
        }
    private:
        std::string _dbfile;
        sqlite3 *_handler;
};

class StrHelper{
    public:
        static size_t split(const std::string &str, const std::string &sep, std::vector<std::string> &result) {
            size_t pos, idx = 0;
            while(idx < str.size()) {
                pos = str.find(sep, idx);
                if (pos == std::string::npos) {
                    result.push_back(str.substr(idx));
                    return result.size();
                }
                if (pos == idx) {
                    idx = pos + sep.size();
                    continue;
                }
                result.push_back(str.substr(idx, pos - idx));
                idx = pos + sep.size();
            }
            return result.size();
        }
};
class UUIDHelper {
    public:
        static std::string uuid() {
            std::random_device rd;
            std::mt19937_64 gernator(rd());
            std::uniform_int_distribution<int> distribution(0, 255);
            std::stringstream ss;
            for (int i = 0; i < 8; i++) {
                ss << std::setw(2) << std::setfill('0') << std::hex << distribution(gernator) ;
                if (i == 3 || i == 5 || i == 7) {
                    ss << "-";
                }
            }
            static std::atomic<size_t> seq(1);
            size_t num = seq.fetch_add(1);
            for (int i = 7; i >= 0; i--) {
                ss << std::setw(2) << std::setfill('0') << std::hex << ((num>>(i*8)) & 0xff);
                if (i == 6) ss << "-";
            }
            return ss.str();
        }
};

class FileHelper {
    public:
        FileHelper(const std::string &filename):_filename(filename){}
        bool exists() {
            struct stat st;
            return (stat(_filename.c_str(), &st) == 0);
        }
        size_t size() {
            struct stat st;
            int ret = stat(_filename.c_str(), &st);
            if (ret < 0) {
                return 0;
            }
            return st.st_size;
        }
        bool read(char *body, size_t offset, size_t len) {
            //1. 打开文件
            std::ifstream ifs(_filename, std::ios::binary | std::ios::in); 
            if (ifs.is_open() == false) {
                ELOG("%s 文件打开失败！", _filename.c_str());
                return false;
            }
            //2. 跳转文件读写位置
            ifs.seekg(offset, std::ios::beg);
            //3. 读取文件数据
            ifs.read(body, len);
            if (ifs.good() == false) {
                ELOG("%s 文件读取数据失败！！", _filename.c_str());
                ifs.close();
                return false;
            }
            //4. 关闭文件
            ifs.close();
            return true;
        }
        bool read(std::string &body) {
            //获取文件大小，根据文件大小调整body的空间
            size_t fsize = this->size();
            body.resize(fsize);
            return read(&body[0], 0, fsize);
        }
        bool write(const char *body, size_t offset, size_t len) {
            //1. 打开文件
            std::fstream fs(_filename, std::ios::binary | std::ios::in | std::ios::out); 
            if (fs.is_open() == false) {
                ELOG("%s 文件打开失败！", _filename.c_str());
                return false;
            }
            //2. 跳转到文件指定位置
            fs.seekp(offset, std::ios::beg);
            //3. 写入数据
            fs.write(body, len);
            if (fs.good() == false) {
                ELOG("%s 文件写入数据失败！！", _filename.c_str());
                fs.close();
                return false;
            }
            //4. 关闭文件
            fs.close();
            return true;
        }
        bool write(const std::string &body) {
            return write(body.c_str(), 0, body.size());
        }
        bool rename(const std::string &nname) {
            return (::rename(_filename.c_str(), nname.c_str()) == 0);
        }
        static std::string parentDirectory(const std::string &filename) {
            // /aaa/bb/ccc/ddd/test.txt
            size_t pos = filename.find_last_of("/");
            if (pos == std::string::npos) {
                // test.txt
                return "./";
            }
            std::string path = filename.substr(0, pos);
            return path;
        }
        static bool createFile(const std::string &filename) {
            std::fstream ofs(filename, std::ios::binary | std::ios::out); 
            if (ofs.is_open() == false) {
                ELOG("%s 文件打开失败！", filename.c_str());
                return false;
            }
            ofs.close();
            return true;
        }
        static bool removeFile(const std::string &filename) {
            return (::remove(filename.c_str()) == 0);
        }
        static bool createDirectory(const std::string &path) {
            //  aaa/bbb/ccc    cccc
            // 在多级路径创建中，我们需要从第一个父级目录开始创建
            size_t pos, idx = 0;
            while(idx < path.size()) {
                pos = path.find("/", idx);
                if (pos == std::string::npos) {
                    return (mkdir(path.c_str(), 0775) == 0);
                }
                std::string subpath = path.substr(0, pos);
                int ret = mkdir(subpath.c_str(), 0775);
                if (ret != 0 && errno != EEXIST) {
                    ELOG("创建目录 %s 失败: %s", subpath.c_str(), strerror(errno));
                    return false;
                }
                idx = pos + 1;
            }
            return true;
        }
        static bool removeDirectory(const std::string &path) {
            // rm -rf path
            // system()
            std::string cmd = "rm -rf " + path;
            return (system(cmd.c_str()) != -1);
        }
    private:
        std::string _filename;
};
}

#endif