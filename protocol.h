#ifndef PROTOCOL_H
#define PROTOCOL_H

#ifdef __cplusplus
extern "C"{
#endif

    int send_protocol(int sock,const char* msg,int len);
    int recv_protocol(int sock,char* out_buf,int max_len);

#ifdef __cplusplus
}
#endif

#endif