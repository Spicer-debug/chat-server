# Reactor 聊天服务器

C++ 写的多线程 TCP 聊天服务器，基于 epoll Reactor 模式，压测 1000 并发 0% 错误。

## 怎么跑

编译（需要 Linux + pthread）：

```
g++ epoll_reactor_server.cpp protocol.cpp -o reactor_server -pthread
g++ stress_test_real.cpp protocol.cpp -o stress_test_real -pthread
```

先启动服务端，再开压测：

```
./reactor_server
./stress_test_real
```

服务端日志写到 `server.log`。

## 架构

一个 accept 线程 + 4 个 Worker 线程，每个 Worker 有自己的 epoll 实例。新连接轮询分到不同 Worker，Worker 内部负责该连接的全部 I/O（EPOLLIN 收、EPOLLOUT 发）。

广播消息通过每个 Worker 内的 broadcast_q 传递——收到消息的 Worker 把消息推到所有 Worker（包括自己）的队列里，每个 Worker 消费自己的队列并分发给该 Worker 下的客户端。

## 协议

`[4 字节网络序长度 + 消息体]`，跟常见的 length-prefixed 协议一样。

## 压测结果

1000 并发长连接，每条消息间隔 200ms，稳定运行 30 秒：

- 错误率：0%
- QPS：~3700
- 无消息丢失，无死锁

## 已知问题

- out_queue 用的 vector，队头 pop 是 O(n)，高并发下可以换成 deque
- recv_buf 只增不减，跑久了内存会涨（实际压测 30 秒从 7GB 漏到稳定，长时间跑需要加个 compact 逻辑）
- Worker 数量写死成 4 个，没做动态适配
- accept 线程忙轮询，空闲时浪费 CPU

## 文件说明

| 文件 | 用途 |
|------|------|
| epoll_reactor_server.cpp | 多 epoll Reactor 服务器（主版本） |
| epoll_thread_chat_server.cpp | 单 epoll 多线程抢锁版（废弃，留着对比） |
| protocol.cpp / protocol.h | 协议编解码 |
| stress_test_real.cpp | 压测工具 |
| epoll_thread_logger.h | 日志宏 |