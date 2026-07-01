#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <vector>
#include <string>
#include <sys/epoll.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include "protocol.h"
#include "epoll_thread_logger.h"

#define EPOLL_SIZE 1024
#define BUF_SIZE 1024
#define MAX_CLIENT_NUM 600
#define PORT 8888
#define HEARTBEAT_INTERVAL 30
#define HEART_CHECK_INTERVAL 5
#define WORKER_NUM 4
#define MSG_QUEUE_MAX 65536
#define MAX_OUT_QUEUE 512


using namespace std;

struct OutFrame {
    string data;
    int offset;
};

struct Client{
    int fd;
    time_t last_active;
    vector<OutFrame> out_queue;
    bool out_registered;
    string recv_buf;
};

struct MsgItem{
    char data[BUF_SIZE];
    int len;
    int sender_fd;
};

struct MsgQueue{
    MsgItem buf[MSG_QUEUE_MAX];
    int front;
    int rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

struct Worker{
    int epfd;
    int id;
    vector<Client> clients;
    pthread_mutex_t clnt_mutex;
    MsgQueue broadcast_q;
    pthread_t tid;
};

Worker g_workers[WORKER_NUM];

static void queue_init(MsgQueue* q){
    q->front = q->rear = 0;
    pthread_mutex_init(&q->mutex,nullptr);
    pthread_cond_init(&q->cond,nullptr);
}

#define LOG_QUEUE_MAX 1024
typedef struct{
    char buf[LOG_QUEUE_MAX][LOG_BUF_LEN];
    int front;
    int rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
}LogQueue;
static LogQueue g_log_queue;

static void log_queue_init(){
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

static void* log_worker_thread(void* arg){
    (void)arg;
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

static int queue_is_full(MsgQueue* q){
    return (q->rear + 1) % MSG_QUEUE_MAX == q->front;
}

static int queue_is_empty(MsgQueue* q){
    return q->front == q->rear;
}

static int queue_push(MsgQueue* q,const char* data,int len,int sender_fd){
    pthread_mutex_lock(&q->mutex);
    if(queue_is_full(q)){
        pthread_mutex_unlock(&q->mutex);
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

static int queue_pop(MsgQueue* q,MsgItem* out){
    pthread_mutex_lock(&q->mutex);
    while(queue_is_empty(q)){
        pthread_cond_wait(&q->cond,&q->mutex);
    }
    *out = q->buf[q->front];
    q->front = (q->front + 1) % MSG_QUEUE_MAX;
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static int set_nonblock(int fd){
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int queue_try_pop(MsgQueue* q,MsgItem* out){
    pthread_mutex_lock(&q->mutex);
    if(queue_is_empty(q)){
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }
    *out = q->buf[q->front];
    q->front = (q->front + 1) % MSG_QUEUE_MAX;
    pthread_mutex_unlock(&q->mutex);
    return 1;
}

//在Worker内部，移除客户端，清理epoll和vector
static void worker_remove_client(Worker* w,int fd){
    pthread_mutex_lock(&w->clnt_mutex);
    for(auto it = w->clients.begin(); it != w->clients.end(); ++it){
        if(it->fd == fd){
            epoll_ctl(w->epfd, EPOLL_CTL_DEL, fd, nullptr);
            close(fd);
            w->clients.erase(it);
            LOG_INFO("客户端 %d 断开，Worker%d 在线：%zu", fd, w->id, w->clients.size());
            break;
        }
    }
    pthread_mutex_unlock(&w->clnt_mutex);
}

// 向客户端out_queue推帧（调用前需持有w->clnt_mutex）
static void worker_push_frame(Worker* w, Client& c, const OutFrame& of){
    if(c.out_queue.size() >= MAX_OUT_QUEUE){
        c.out_queue.erase(c.out_queue.begin());
    }
    c.out_queue.push_back(of);
    if(!c.out_registered){
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.fd = c.fd;
        epoll_ctl(w->epfd, EPOLL_CTL_MOD, c.fd, &ev);
        c.out_registered = true;
    }
}

// 心跳检测：踢掉超时客户端，更新 last_check
static void worker_check_heartbeat(Worker* w, time_t now, time_t* last_check){
    if(now - *last_check < HEART_CHECK_INTERVAL) return;
    *last_check = now;
    vector<int> to_remove;
    pthread_mutex_lock(&w->clnt_mutex);
    for(auto& client : w->clients){
        if(now - client.last_active > HEARTBEAT_INTERVAL){
            LOG_INFO("client %d timeout,disconnect", client.fd);
            to_remove.push_back(client.fd);
        }
    }
    pthread_mutex_unlock(&w->clnt_mutex);
    for(int fd : to_remove){
        worker_remove_client(w, fd);
    }
}

// 消费广播队列：编码协议帧，分发给本Worker所有客户端
static void worker_consume_broadcast(Worker* w){
    MsgItem msg;
    while(queue_try_pop(&w->broadcast_q, &msg)){
        int net_len = htonl(msg.len);
        string frame((char*)&net_len, 4);
        frame.append(msg.data, msg.len);
        OutFrame of = {frame, 0};
        pthread_mutex_lock(&w->clnt_mutex);
        for(auto& c : w->clients){
            if(c.fd == msg.sender_fd) continue;
            worker_push_frame(w, c, of);
        }
        pthread_mutex_unlock(&w->clnt_mutex);
    }
}

//Worker线程主函数
void* worker_thread(void* arg){
    Worker* w = (Worker*)arg;

    w->epfd = epoll_create1(0);
    struct epoll_event* ep_events = (struct epoll_event*)malloc(sizeof(struct epoll_event) * EPOLL_SIZE);
    
    time_t last_check = time(NULL);

    while(true){
        int n = epoll_wait(w->epfd, ep_events, EPOLL_SIZE, 10);
        time_t now = time(NULL);

        // 心跳检测 + 广播消费
        worker_check_heartbeat(w, now, &last_check);
        worker_consume_broadcast(w);

        for(int i = 0; i < n; i++){
            int fd = ep_events[i].data.fd;
            uint32_t revents = ep_events[i].events;

            if(revents & (EPOLLHUP | EPOLLERR)){
                worker_remove_client(w,fd);
                continue;
            }

            if(revents & EPOLLIN){
                char tmp[4096];
                bool connection_dead = false;
                while(true){
                    int n = recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
                    if(n > 0){
                        pthread_mutex_lock(&w->clnt_mutex);
                        for(auto& c : w->clients){
                            if(c.fd == fd){ c.recv_buf.append(tmp, n); break; }
                        }
                        pthread_mutex_unlock(&w->clnt_mutex);
                    } else if(n == 0){
                        connection_dead = true;
                        break;
                    } else {
                        if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                        connection_dead = true;
                        break;
                    }
                }
                if(connection_dead){
                    worker_remove_client(w, fd);
                    continue;
                }

                bool client_removed = false;
                pthread_mutex_lock(&w->clnt_mutex);
                for(auto& c : w->clients){
                    if(c.fd != fd) continue;
                    while(c.recv_buf.size() >= 4){
                        int msg_len = ntohl(*(int*)c.recv_buf.data());
                        if(msg_len <= 0 || msg_len >= BUF_SIZE){
                            LOG_INFO("客户端 %d 协议错误(len=%d), 断开", fd, msg_len);
                            pthread_mutex_unlock(&w->clnt_mutex);
                            worker_remove_client(w, fd);
                            client_removed = true;
                            break;
                        }
                        if(c.recv_buf.size() < (size_t)(4 + msg_len)){
                            break;
                        }
                        string msg = c.recv_buf.substr(4, msg_len);
                        c.recv_buf.erase(0, 4 + msg_len);
                        c.last_active = now;

                        // 解锁处理消息（避免死锁：消息处理会锁 broadcast_q）
                        pthread_mutex_unlock(&w->clnt_mutex);

                        // 心跳包单独回复 — 走out_queue+EPOLLOUT，避免非阻塞socket上直接send
                        if(msg_len == 4 && strncmp(msg.c_str(), "ping", 4) == 0){
                            int net_len = htonl(4);
                            string frame((char*)&net_len, 4);
                            frame.append("pong", 4);
                            OutFrame of = {frame, 0};
                            pthread_mutex_lock(&w->clnt_mutex);
                            for(auto& cc : w->clients){
                                if(cc.fd == fd){
                                    worker_push_frame(w, cc, of);
                                    break;
                                }
                            }
                            pthread_mutex_unlock(&w->clnt_mutex);
                        } else {
                            LOG_INFO("客户端 %d: %s", fd, msg.c_str());
                            for(int j = 0; j < WORKER_NUM; j++){
                                int ret = queue_push(&g_workers[j].broadcast_q, msg.c_str(), msg_len, fd);
                                if(ret != 0){
                                    LOG_INFO("广播队列满 Worker%d，消息丢弃(fd=%d)", j, fd);
                                }
                            }
                        }

                        pthread_mutex_lock(&w->clnt_mutex);
                        bool found = false;
                        for(auto& cc : w->clients){
                            if(cc.fd == fd){ found = true; break; }
                        }
                        if(!found){
                            client_removed = true;
                            break;
                        }
                    }
                    break;
                }
                if(!client_removed){
                    pthread_mutex_unlock(&w->clnt_mutex);
                }
            }

            // EPOLLOUT：每条发一点，不阻塞
            if(revents & EPOLLOUT){
                int max_sends = 8;  // 每次 EPOLLOUT 最多 send() 8 次
                bool should_remove = false;
                pthread_mutex_lock(&w->clnt_mutex);
                for(auto& c : w->clients){
                    if(c.fd != fd) continue;
                    while(!c.out_queue.empty() && max_sends-- > 0){
                        OutFrame& f = c.out_queue.front();
                        int ret = send(fd, f.data.data() + f.offset,
                                       f.data.size() - f.offset, MSG_NOSIGNAL);
                        if(ret > 0){
                            f.offset += ret;
                            if(f.offset == (int)f.data.size()){
                                c.out_queue.erase(c.out_queue.begin());
                            }
                        } else if(errno == EAGAIN || errno == EWOULDBLOCK){
                            break;
                        } else {
                            LOG_INFO("客户端 %d EPOLLOUT发送失败(errno=%d:%s)", fd, errno, strerror(errno));
                            c.out_queue.clear();
                            should_remove = true;
                            break;
                        }
                    }
                    if(!should_remove && c.out_queue.empty() && c.out_registered){
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.fd = fd;
                        epoll_ctl(w->epfd, EPOLL_CTL_MOD, fd, &ev);
                        c.out_registered = false;
                    }
                    break;
                }
                pthread_mutex_unlock(&w->clnt_mutex);
                if(should_remove){
                    worker_remove_client(w, fd);
                    continue;
                }
            }
        }
    }
}

//accept线程：轮询accept新连接，分配给Worker
void* accept_thread(void* arg){
    int serv_sock = *(int*)arg;
    
    int round = 0;
    while(true){
        struct sockaddr_in clnt_addr;
        socklen_t addr_len = sizeof(clnt_addr);
        int fd = accept4(serv_sock, (struct sockaddr*)&clnt_addr, &addr_len, SOCK_NONBLOCK);
        if(fd == -1){
            if(errno != EAGAIN && errno != EWOULDBLOCK){
                LOG_ERROR("accept error: %s",strerror(errno));              
            }
            usleep(1000);
            continue;
        }

        Worker* w = &g_workers[round % WORKER_NUM];
        round++;

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(w->epfd, EPOLL_CTL_ADD, fd, &ev);

        pthread_mutex_lock(&w->clnt_mutex);
        w->clients.push_back({fd, time(NULL), {}, false});
        size_t online = w->clients.size();
        pthread_mutex_unlock(&w->clnt_mutex);
        LOG_INFO("新客户端 %d 连接，分配给Worker %d，当前在线：%zu", fd, w->id, online);

    }
    return nullptr;
}

int main(){
    signal(SIGPIPE,SIG_IGN);

    int serv_sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if(serv_sock == -1){
        fprintf(stderr, "socket failed\n");
        return 1;
    }
    int opt = 1;
    setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(PORT);

    if(bind(serv_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1){
        fprintf(stderr, "bind failed\n");
        return 1;
    }
    if(listen(serv_sock, 8192) == -1){
        fprintf(stderr, "listen failed\n");
        return 1;
    }

    for(int i = 0; i < WORKER_NUM; i++){
        g_workers[i].id = i;
        pthread_mutex_init(&g_workers[i].clnt_mutex, nullptr);
        queue_init(&g_workers[i].broadcast_q);
    }

    log_queue_init();
    pthread_t log_tid;
    pthread_create(&log_tid, nullptr, log_worker_thread, nullptr);
    pthread_detach(log_tid);

    for(int i = 0; i < WORKER_NUM; i++){
        pthread_create(&g_workers[i].tid, nullptr, worker_thread, &g_workers[i]);
        pthread_detach(g_workers[i].tid);
    }

    pthread_t accept_tid;
    pthread_create(&accept_tid, nullptr, accept_thread, &serv_sock);
    pthread_detach(accept_tid);

    LOG_INFO("Reactor 聊天室服务启动，%d 个 Worker 端口：%d", WORKER_NUM, PORT);

    while(true){
        pause();
    }
    return 0;
}