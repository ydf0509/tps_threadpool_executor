## 1. pip install tps_threadpool_executor

这个线程池和一般线程池不同，是自动控频的，能够将任意耗时大小的函数控制成指定的运行频率。

此线程池入参不是设置并发大小，而是设置tps大小。


## 4种控频
```
TpsThreadpoolExecutor 基于单进程的当前线程池控频。

DistributedTpsThreadpoolExecutor 基于多台机器的分布式控频，需要安装redis，统计出活跃线程池，从而平分任务。

TpsThreadpoolExecutorWithMultiProcess  基于单机 多进程 + 智能线程池 的控频率，自动开启多进程。

DistributedTpsThreadpoolExecutorWithMultiProcess 基于多机的，每台机器自动开多进程 + 多线程 的控频率。

例如你有1台 128核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，
则选择 TpsThreadpoolExecutorWithMultiProcess 合适（不需要安装redis）。

例如你有6台 16核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，
则选择 DistributedTpsThreadpoolExecutorWithMultiProcess 合适。

```


## 实现代码
```python
import json
import time
from queue import Queue
import threading
from threadpool_executor_shrink_able.sharp_threadpoolexecutor import ThreadPoolExecutorShrinkAble
import nb_log
import redis
import decorator_libs
import socket

import os
import multiprocessing
import atexit

# 4种控频


"""
TpsThreadpoolExecutor 基于单进程的当前线程池控频。

DistributedTpsThreadpoolExecutor 基于多台机器的分布式控频，需要安装redis，统计出活跃线程池，从而平分任务。

TpsThreadpoolExecutorWithMultiProcess  基于单机 多进程 + 智能线程池 的控频率，自动开启多进程。

DistributedTpsThreadpoolExecutorWithMultiProcess 基于多机的，每台机器自动开多进程的控频率。

例如你有1台 128核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，
则选择 TpsThreadpoolExecutorWithMultiProcess 合适（不需要安装redis）。

例如你有6台 16核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，
则选择 DistributedTpsThreadpoolExecutorWithMultiProcess 合适。



"""

