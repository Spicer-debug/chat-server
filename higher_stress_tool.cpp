#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "protocol.h"
#define THREAD_COUNT 300
#define MSG_PER_CLIENT 10
#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8888

void* client_thread(void* arg){
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock == -1){
        perror("socket");
        return NULL;
    }
    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    if(connect(sock,(struct sockaddr*)&addr,sizeof(addr)) == -1){
        perror("connect");
        close(sock);
        return NULL;
    }
    const char* msg = "Hello from stress test!";
    for(int i = 0;i < MSG_PER_CLIENT; i++){
        if(send_protocol(sock,msg,strlen(msg)) == -1){
            perror("send");
            break;
        }
        usleep(50000);
    }
    close(sock);
    return NULL;
}

int main(){
    pthread_t threads[THREAD_COUNT];
    time_t start = time(NULL);

    for(int i = 0;i < THREAD_COUNT; i++){
        if(pthread_create(&threads[i],NULL,client_thread,NULL) != 0){
            perror("pthread_create");
            threads[i] = 0;
        }
    }

    for(int i = 0;i < THREAD_COUNT; i++){
        if(threads[i] != 0){
            pthread_join(threads[i],NULL);
        }
        
    }

    time_t end = time(NULL);
    std::cout << "Total time:" << (end - start) << "seconds" << std::endl;
    std::cout << "Total connect count: "<<THREAD_COUNT<< ",Total msg count: "<<THREAD_COUNT * MSG_PER_CLIENT << std::endl; 
    return 0;
}