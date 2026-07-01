#include<stdio.h>
#include<pthread.h>
#include<unistd.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<vector>
#include<algorithm>
#include<string>
#include<sys/epoll.h>
#include<signal.h>
#include<time.h>
#include<fcntl.h>
#include<errno.h>
#include "protocol.h"
#include "epoll_thread_logger.h"
#define EPOLL_SIZE 1024
#define BUF_SIZE 1024
#define MAX_CLIENT_NUM 600
#define PORT 8888
#define HEARTBEAT_INTERVAL 30
#define HEART_CHECK_INTERVAL 5
#define MSG_QUEUE_MAX 65536
#define SEND_WORKER_NUM 16
#define LOG_QUEUE_MAX 1024
using namespace std;

struct epoll_event *ep_events;
struct epoll_event event;
int epfd;
void remove_client(int fd);

// 心跳客户端结构体
struct Client{
    int fd;
    time_t last_active;
};

// 消息队列结构
typedef struct{
    char data[BUF_SIZE];
    int len;
    int sender_fd;
}MsgItem;

typedef struct{
    MsgItem buf[MSG_QUEUE_MAX];
    int front;
    int rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
}MsgQueue;
MsgQueue g_msg_queue;
typedef struct{
    char buf[LOG_QUEUE_MAX][LOG_BUF_LEN];
    int front;
    int rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
}LogQueue;

LogQueue g_log_queue;

// 保护客户端vector的互斥锁
pthread_mutex_t g_clnt_mutex;
vector<Client> clnt_socks;

void queue_init(MsgQueue* q);
int queue_push(MsgQueue* q,const char* data,int len,int sender_fd);
int queue_pop(MsgQueue* q,MsgItem* out);
void* send_worker_thread(void* arg);
int set_nonblock(int fd);
int send_protocol(int fd,char* buf,int len);

// 初始化消息队列
void queue_init(MsgQueue* q){
    q->front = q->rear = 0;
    pthread_mutex_init(&q->mutex,nullptr);
    pthread_cond_init(&q->cond,nullptr);
}

// 队列满判断
static int queue_is_full(MsgQueue* q){
    return (q->rear + 1) % MSG_QUEUE_MAX == q->front;
}

static int queue_is_empty(MsgQueue* q){
    return q->front == q->rear;
}

// 生产者入队
int queue_push(MsgQueue* q,const char* data,int len, int sender_fd){
    pthread_mutex_lock(&q->mutex);
    if(queue_is_full(q)){
        pthread_mutex_unlock(&q->mutex);
        LOG_ERROR("MSG_QUEUE if full,drop this msg");
        return -1;
    }
    MsgItem& item = q->buf[q->rear];
    memcpy(item.data, data, len);
    item.len = len;
    item.sender_fd = sender_fd;
    q->rear = (q->rear + 1) % MSG_QUEUE_MAX;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

// 消费者出队
int queue_pop(MsgQueue* q,MsgItem* out){
    pthread_mutex_lock(&q->mutex);
    // while处理虚假唤醒
    while(queue_is_empty(q)){
        pthread_cond_wait(&q->cond,&q->mutex);
    }
    *out = q->buf[q->front];
    q->front = (q->front + 1) % MSG_QUEUE_MAX;
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

void log_queue_init(){
    memset(&g_log_queue,0,sizeof(g_log_queue));
    pthread_mutex_init(&g_log_queue.mutex,NULL);
    pthread_cond_init(&g_log_queue.cond,NULL);
}

void log_push(const char* msg){
    pthread_mutex_lock(&g_log_queue.mutex);

    if((g_log_queue.rear + 1) % LOG_QUEUE_MAX == g_log_queue.front){
        pthread_mutex_unlock(&g_log_queue.mutex);
        return;
    }
    strcpy(g_log_queue.buf[g_log_queue.rear],msg);
    g_log_queue.rear = (g_log_queue.rear + 1) % LOG_QUEUE_MAX;
    pthread_cond_signal(&g_log_queue.cond);
    pthread_mutex_unlock(&g_log_queue.mutex);
}

void* log_worker_thread(void* arg){
    FILE* fp = fopen("server.log","a");
    if(!fp){return NULL;}
    while(1){
        pthread_mutex_lock(&g_log_queue.mutex);
        while(g_log_queue.front == g_log_queue.rear){
            pthread_cond_wait(&g_log_queue.cond,&g_log_queue.mutex);
        }
        char tmp[LOG_BUF_LEN];
        strcpy(tmp,g_log_queue.buf[g_log_queue.front]);
        g_log_queue.front = (g_log_queue.front + 1) % LOG_QUEUE_MAX;
        pthread_mutex_unlock(&g_log_queue.mutex);

        time_t now = time(NULL);
        struct tm* t = localtime(&now);
        char time_buf[32];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", t);
        fprintf(fp, "[%s] %s\n", time_buf, tmp);
        fflush(fp);
    }
    fclose(fp);
    return NULL;
}


// 发送工作线程
void* send_worker_thread(void* arg){
    (void)arg;
    MsgItem msg;
    while(true){
        queue_pop(&g_msg_queue,&msg);
        vector<int> fd_list;
        // 加锁访问共享客户端vector
        pthread_mutex_lock(&g_clnt_mutex);
        for(auto& client : clnt_socks){
            fd_list.push_back(client.fd);
        }
        pthread_mutex_unlock(&g_clnt_mutex);
        // 清理断线
        vector<int> offline_fds;
        for(int fd : fd_list){
            if(fd == msg.sender_fd){continue;}
            int ret = send_protocol(fd,(const char*)msg.data,msg.len);
            if(ret == -2){
                offline_fds.push_back(fd);
            }
        }
        for(int fd : offline_fds){
            remove_client(fd);
        }
    }
    return nullptr;
}

// 设置fd非阻塞
int set_nonblock(int fd){
    int flag = fcntl(fd, F_GETFL,0);
    return fcntl(fd,F_SETFL,flag | O_NONBLOCK);
}

// 移除客户端、清理epoll、vector
void remove_client(int fd) {
    pthread_mutex_lock(&g_clnt_mutex);
    for(auto it = clnt_socks.begin();it != clnt_socks.end(); ++it){
        if(it->fd == fd){
            epoll_ctl(epfd,EPOLL_CTL_DEL, fd,NULL);
            close(fd);
            clnt_socks.erase(it);
            LOG_INFO("客户端 %d 断开，当前在线：%zu", fd, clnt_socks.size());
            pthread_mutex_unlock(&g_clnt_mutex);
            return;
        }
    }
    pthread_mutex_unlock(&g_clnt_mutex);
}

int main() {
    signal(SIGPIPE,SIG_IGN);
    // 初始化客户端列表锁
    pthread_mutex_init(&g_clnt_mutex, nullptr);

    socklen_t clnt_adr_sz;
    int serv_sock, clnt_sock;
    struct sockaddr_in serv_adr, clnt_adr;
    int event_cnt, i;
    int opt = 1;

    // 创建监听socket
    serv_sock = socket(PF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (serv_sock == -1){
        LOG_ERROR("socket create failed");
        exit(1);
    }
    setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&serv_adr, 0, sizeof(serv_adr));
    serv_adr.sin_family = AF_INET;
    serv_adr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_adr.sin_port = htons(PORT);

    if (bind(serv_sock, (struct sockaddr*)&serv_adr, sizeof(serv_adr)) == -1){
        LOG_ERROR("bind failed");
        exit(1);
    }
    if (listen(serv_sock, 8192) == -1){
        LOG_ERROR("listen failed");
        exit(1);
    }

    // 初始化消息队列 + 启动发送线程
    queue_init(&g_msg_queue);
    pthread_t send_tids[SEND_WORKER_NUM];
    for(int i = 0; i < SEND_WORKER_NUM; i++)
    {
        pthread_create(&send_tids[i], nullptr, send_worker_thread, nullptr);
        pthread_detach(send_tids[i]);
    }
    log_queue_init();
    pthread_t log_tid;
    pthread_create(&log_tid, nullptr, log_worker_thread, nullptr);
    pthread_detach(log_tid);

    // epoll初始化
    epfd = epoll_create1(0);
    ep_events = (struct epoll_event*)malloc(sizeof(struct epoll_event) * EPOLL_SIZE);

    event.events = EPOLLIN;
    event.data.fd = serv_sock;
    epoll_ctl(epfd, EPOLL_CTL_ADD, serv_sock, &event);

    LOG_INFO("聊天室服务启动，端口：%d", PORT);
    time_t now = time(NULL);
    time_t last_check = now;

    while (1) {
        event_cnt = epoll_wait(epfd, ep_events, EPOLL_SIZE, 1000);
        if (event_cnt == -1){
            LOG_ERROR("epoll_wait failed: %s", strerror(errno));
            continue;
        }
        now = time(NULL);
        // 心跳超时检测
        if(now - last_check >= HEART_CHECK_INTERVAL ){
            last_check = now;
            vector<int> to_remove;
            pthread_mutex_lock(&g_clnt_mutex);
            for(auto& client : clnt_socks){
                if(now - client.last_active > HEARTBEAT_INTERVAL){
                    LOG_INFO("client %d timeout,disconnect",client.fd);
                    to_remove.push_back(client.fd);
                }
            }
            pthread_mutex_unlock(&g_clnt_mutex);
            for(int fd : to_remove){
                remove_client(fd);
            }
        }

        for (i = 0; i < event_cnt; i++) {
            int cur_fd = ep_events[i].data.fd;
            uint32_t revents = ep_events[i].events;
            // 新连接
            if (cur_fd == serv_sock) {
                while (1) {
                    clnt_adr_sz = sizeof(clnt_adr);
                    clnt_sock = accept4(serv_sock, (struct sockaddr*)&clnt_adr, &clnt_adr_sz, SOCK_NONBLOCK);
                    if (clnt_sock == -1) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            LOG_ERROR("accept error: %s",strerror(errno));
                        }
                        break;
                    }

                    event.events = EPOLLIN;
                    event.data.fd = clnt_sock;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, clnt_sock, &event);

                    // 加锁插入客户端
                    pthread_mutex_lock(&g_clnt_mutex);
                    clnt_socks.push_back({clnt_sock,time(NULL)});
                    size_t online = clnt_socks.size();
                    pthread_mutex_unlock(&g_clnt_mutex);
                    LOG_INFO("新客户端连接：%d，在线人数：%zu", clnt_sock, online);
                }
            } else {
                if(revents & (EPOLLHUP | EPOLLERR)){
                    remove_client(cur_fd);
                    continue;
                }
                // 客户端读事件
                if(revents & EPOLLIN){
                    char buffer[BUF_SIZE];
                    int ret = recv_protocol(cur_fd,buffer,BUF_SIZE);
                    if(ret == -1){
                        // 非阻塞 EAGAIN，等下次 EPOLLIN
                        continue;
                    }
                    if(ret <= 0){
                        remove_client(cur_fd);
                        continue;
                    }
                    // 更新活跃时间
                    pthread_mutex_lock(&g_clnt_mutex);
                    for(auto& client : clnt_socks){
                        if(client.fd == cur_fd){
                            client.last_active = time(NULL);
                            break;
                        }
                    }
                    pthread_mutex_unlock(&g_clnt_mutex);

                    // 心跳包单独回复，不广播
                    if(strncmp(buffer,"ping",4) == 0){
                        send_protocol(cur_fd,"pong",4);
                        continue;
                    }
                    LOG_INFO("客户端%d：%s", cur_fd, buffer);
                    // 消息推入队列，交给发送线程广播
                    queue_push(&g_msg_queue,buffer,ret,cur_fd);
                }
                
            }
        }
    }

    free(ep_events);
    close(serv_sock);
    close(epfd);
    pthread_mutex_destroy(&g_clnt_mutex);
    pthread_mutex_destroy(&g_msg_queue.mutex);
    pthread_cond_destroy(&g_msg_queue.cond);
    return 0;
}