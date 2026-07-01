#ifndef LOGGER_H
#define LOGGER_H
#include <pthread.h>
#include <stdarg.h>
#include <string.h>

#define LOG_BUF_LEN 2048
// 外部声明全局日志队列推送函数
extern void log_push(const char* msg);

// 拼接工具宏
#define LOG_FMT_WRAP(lv, fmt, ...) do{ \
    char _log_tmp[LOG_BUF_LEN]; \
    snprintf(_log_tmp, LOG_BUF_LEN, "[%s] " fmt, lv, ##__VA_ARGS__); \
    log_push(_log_tmp); \
}while(0)

#define LOG_DEBUG(fmt,...) LOG_FMT_WRAP("DEBUG",fmt,##__VA_ARGS__)
#define LOG_INFO(fmt,...)  LOG_FMT_WRAP("INFO",fmt,##__VA_ARGS__)
#define LOG_WARN(fmt,...)  LOG_FMT_WRAP("WARN",fmt,##__VA_ARGS__)
#define LOG_ERROR(fmt,...) LOG_FMT_WRAP("ERROR",fmt,##__VA_ARGS__)


#endif