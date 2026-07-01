#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <atomic>
#include <vector>
#include <stdarg.h>
#include "protocol.h"

#define SERVER_IP       "127.0.0.1"
#define SERVER_PORT     8888
#define TOTAL_CLIENTS   1000          // 总客户端数
#define RAMP_UP_RATE    20           // 爬坡：每秒新增连接数
#define STEADY_SEC      30           // 稳定期：满负载持续时间
#define MSG_INTERVAL_US 200000       // 每条消息间隔，200ms ≈ 每秒5条
#define BUF_SIZE        1024
#define LOG_FILE        "stress_test.log"

std::atomic<int> g_connected(0);
std::atomic<int> g_send_total(0);
std::atomic<int> g_send_fail(0);
std::atomic<bool> g_stop(false);

static FILE*      g_log_fp = NULL;
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

static void log_write(const char* fmt, ...) {
    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    printf("%s", buf);

    if (g_log_fp) {
        pthread_mutex_lock(&g_log_mutex);
        time_t now = time(NULL);
        struct tm* t = localtime(&now);
        fprintf(g_log_fp, "[%02d:%02d:%02d] %s", t->tm_hour, t->tm_min, t->tm_sec, buf);
        fflush(g_log_fp);
        pthread_mutex_unlock(&g_log_mutex);
    }
}

void* client_worker(void* arg) {
    long id = (long)arg;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        log_write("[client %ld] socket() failed: %s\n", id, strerror(errno));
        return NULL;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        log_write("[client %ld] connect() failed: %s\n", id, strerror(errno));
        close(sock);
        return NULL;
    }

    g_connected.fetch_add(1);
    log_write("[client %ld] connected  (当前已连接: %d)\n", id, g_connected.load());

    const char* msg = "Hello from stress test!";
    int msg_len = strlen(msg);
    int seq = 0;

    while (!g_stop.load()) {
        int ret = send_protocol(sock, msg, msg_len);
        if (ret == -2) {
            g_send_fail.fetch_add(1);
            log_write("[client %ld] disconnected, stopping\n", id);
            break;
        } else if (ret == -1) {
            g_send_fail.fetch_add(1);
        } else {
            g_send_total.fetch_add(1);
        }
        seq++;
        // 排空接收缓冲区
        char trash[BUF_SIZE];
        while (recv(sock, trash, BUF_SIZE, MSG_DONTWAIT) > 0) {}
        usleep(MSG_INTERVAL_US);
    }

    close(sock);
    log_write("[client %ld] exited, sent %d msgs\n", id, seq);
    return NULL;
}

int main() {
    g_log_fp = fopen(LOG_FILE, "w");
    if (!g_log_fp) {
        perror("fopen stress_test.log");
        return 1;
    }

    log_write("--- 聊天室压测 ---\n");
    log_write("目标: %d 并发 | 爬坡: %d/s | 稳定: %ds\n",
              TOTAL_CLIENTS, RAMP_UP_RATE, STEADY_SEC);
    log_write("-------------------\n\n");

    std::vector<pthread_t> threads(TOTAL_CLIENTS);
    time_t test_start = time(NULL);

    log_write("--- 爬坡开始 ---\n");
    time_t ramp_start = time(NULL);

    int ramp_batches = (TOTAL_CLIENTS + RAMP_UP_RATE - 1) / RAMP_UP_RATE;
    for (int batch = 0; batch < ramp_batches; batch++) {
        int batch_start = batch * RAMP_UP_RATE;
        int batch_end = std::min(batch_start + RAMP_UP_RATE, TOTAL_CLIENTS);

        for (int i = batch_start; i < batch_end; i++) {
            pthread_create(&threads[i], NULL, client_worker, (void*)(long)(i + 1));
        }

        sleep(1);

        int conn = g_connected.load();
        log_write("  爬坡 %d/%d, 已连接: %d/%d\n",
                  batch_end, TOTAL_CLIENTS, conn, TOTAL_CLIENTS);
    }

    time_t ramp_end = time(NULL);
    int ramp_duration = (int)(ramp_end - ramp_start);
    log_write("爬坡完成, 耗时 %ds, 已连接: %d\n\n",
              ramp_duration, g_connected.load());

    log_write("--- 稳定期 %ds ---\n", STEADY_SEC);
    time_t steady_start = time(NULL);

    for (int t = 0; t < STEADY_SEC; t += 5) {
        sleep(5);
        int elapse = (int)(time(NULL) - steady_start);
        int total_send = g_send_total.load();
        int fail = g_send_fail.load();
        log_write("  稳定 %ds/%ds 在线: %d | 发送: %d | 失败: %d | QPS: ~%d\n",
                  elapse, STEADY_SEC, g_connected.load(),
                  total_send, fail,
                  total_send / (elapse > 0 ? elapse : 1));
    }

    g_stop.store(true);
    log_write("稳定期结束\n\n");

    log_write("等待客户端退出...\n");
    for (int i = 0; i < TOTAL_CLIENTS; i++) {
        pthread_join(threads[i], NULL);
    }

    time_t test_end = time(NULL);
    int total_duration = (int)(test_end - test_start);

    log_write("\n--- 压测报告 ---\n");
    log_write("  总耗时:        %d 秒\n", total_duration);
    log_write("  目标并发:      %d\n", TOTAL_CLIENTS);
    log_write("  实际峰值在线:  %d\n", g_connected.load());
    log_write("  总发送消息:    %d\n", g_send_total.load());
    log_write("  发送失败:      %d\n", g_send_fail.load());
    log_write("  平均 QPS:      %d 条/秒\n",
              total_duration > 0 ? g_send_total.load() / total_duration : 0);
    log_write("  错误率:        %.2f%%\n",
              g_send_total.load() > 0
                  ? 100.0 * g_send_fail.load() / g_send_total.load()
                  : 0.0);
    log_write("----------------\n");

    fclose(g_log_fp);
    return 0;
}
