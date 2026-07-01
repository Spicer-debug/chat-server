#include "protocol.h"
#include<sys/socket.h>
#include<arpa/inet.h>
#include<string.h>
#include<errno.h>

int send_protocol(int sock,const char* msg,int len){
    int net_len = htonl(len);
    int sent_head = 0;
    while(sent_head < 4){
        int ret = send(sock,(char*)&net_len + sent_head , 4 - sent_head ,MSG_NOSIGNAL);
        if(ret <= 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return -1;
            }
            return -2;
        }
        sent_head += ret;
    }
    int total = 0;
    while(total < len){
        int ret = send(sock,msg + total,len - total,MSG_NOSIGNAL);
        if(ret <= 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return -1;
            }
            return -2;
        }
        total += ret;
    }
    return 0;
}

int recv_protocol(int sock,char* out_buf,int max_len){
    int net_len = 0;
    int total = 0;
    while(total < 4){
        int ret = recv(sock,(char*)&net_len + total,4 - total,0);
        if(ret < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return -1;  // 非阻塞模式：暂无数据
            }
            return -2;  // 错误
        }
        if(ret == 0){
            return 0;  // EOF
        }
        total += ret;
    }
    int len = ntohl(net_len);
    if(len <= 0 || len >= max_len){
        return -2;
    }

    total = 0;
    while(total < len){
        int ret = recv(sock,out_buf + total,len - total, 0);
        if(ret < 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return -1;
            }
            return -2;
        }
        if(ret == 0){
            return 0;
        }
        total += ret;
    }
    out_buf[total] = '\0';
    return total;
}