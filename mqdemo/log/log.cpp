#include <cstdio>
#include <ctime>

// 日志级别：数值越大越严重
#define DBG_LEVEL 0
#define INF_LEVEL 1
#define ERR_LEVEL 2
#define DEFAULT_LEVEL DBG_LEVEL

// 通用日志宏：自动带上「级别」「时间」「文件名:行号」
#define LOG(lev_str, level, format, ...)                                  \
    do {                                                                  \
        if ((level) >= DEFAULT_LEVEL) {                                   \
            time_t t = time(nullptr);                                     \
            struct tm tm_buf;                                             \
            localtime_r(&t, &tm_buf);                                     \
            char time_str[32];                                            \
            strftime(time_str, sizeof(time_str), "%H:%M:%S", &tm_buf);    \
            printf("[%s][%s][%s:%d]\t" format "\n",                       \
                   lev_str, time_str, __FILE__, __LINE__, ##__VA_ARGS__); \
        }                                                                 \
    } while (0)

#define DLOG(format, ...) LOG("DBG", DBG_LEVEL, format, ##__VA_ARGS__)
#define ILOG(format, ...) LOG("INF", INF_LEVEL, format, ##__VA_ARGS__)
#define ELOG(format, ...) LOG("ERR", ERR_LEVEL, format, ##__VA_ARGS__)

int main()
{
    DLOG("Hello World");
    ILOG("Hello World");
    ELOG("Hello World");
    return 0;
}