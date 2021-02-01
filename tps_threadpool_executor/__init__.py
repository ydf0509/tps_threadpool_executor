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

例如你有1台 128核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，则选择 TpsThreadpoolExecutorWithMultiProcess 合适（不需要安装redis）。
例如你有6台 16核的电脑作为压测客户机， 需要对web服务产生每秒1万次请求，则选择 DistributedTpsThreadpoolExecutorWithMultiProcess 合适。

"""


class ThreadPoolExecutorShrinkAbleWithSpecifyQueue(ThreadPoolExecutorShrinkAble):
    def __init__(self, *args, specify_work_queue=None, **kwargs):
        super(ThreadPoolExecutorShrinkAbleWithSpecifyQueue, self).__init__(*args, **kwargs)
        self.work_queue = specify_work_queue


class TpsThreadpoolExecutor(nb_log.LoggerMixin):

    def __init__(self, tps=0, max_workers=500, specify_work_queue=None):
        """
        :param tps:   指定线程池每秒运行多少次函数，为0这不限制运行次数
        """
        self.tps = tps
        self.time_interval = 1 / tps if tps != 0 else 0
        self.pool = ThreadPoolExecutorShrinkAbleWithSpecifyQueue(max_workers=max_workers,
                                                                 specify_work_queue=specify_work_queue or Queue(
                                                                     max_workers))  # 这是使用的智能线程池，所以可以写很大的数字，具体见另一个包的解释。
        self._last_submit_task_time = time.time()
        self._lock_for_count__last_submit_task_time = threading.Lock()

    def submit(self, func, *args, **kwargs):
        with self._lock_for_count__last_submit_task_time:
            if self.time_interval:
                time.sleep(self.time_interval)
            return self.pool.submit(func, *args, **kwargs)

    def shutdown(self, wait=True):
        self.pool.shutdown(wait=wait)


def get_host_ip():
    ip = ''
    host_name = ''
    # noinspection PyBroadException
    try:
        sc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sc.connect(('8.8.8.8', 80))
        ip = sc.getsockname()[0]
        host_name = socket.gethostname()
        sc.close()
    except Exception:
        pass
    return ip, host_name


class DistributedTpsThreadpoolExecutor(TpsThreadpoolExecutor, ):
    """
    这个是redis分布式控频线程池，不是基于incr计数的，是基于统计活跃消费者，然后每个线程池平分频率的。
    """

    def __init__(self, tps=0, max_workers=500, specify_work_queue=None, pool_identify: str = None,
                 redis_url: str = 'redis://:@127.0.0.1/0'):
        """
        :param tps: 指定线程池每秒运行多少次函数，为0这不限制运行次数
        :param pool_identify: 对相同标识的pool，进行分布式控频,例如多台机器都有标识为 123 的线程池，则所有机器加起来的运行次数控制成指定频率。
        :param redis_url:   'redis://:secret@100.22.233.110/7'
        """
        if pool_identify is None:
            raise ValueError('设置的参数错误')
        self._pool_identify = pool_identify
        super(DistributedTpsThreadpoolExecutor, self).__init__(tps=tps, max_workers=max_workers, specify_work_queue=specify_work_queue)
        # self.queue = multiprocessing.Queue(500)
        self.redis_db = redis.from_url(redis_url)
        self.redis_key_pool_identify = f'DistributedTpsThreadpoolExecutor:{pool_identify}'
        ip, host_name = get_host_ip()
        self.current_process_flag = f'{ip}-{host_name}-{os.getpid()}-{id(self)}'
        self._heartbeat_interval = 10
        decorator_libs.keep_circulating(self._heartbeat_interval, block=False, daemon=True)(
            self._send_heartbeat_to_redis)
        threading.Thread(target=self._run__send_heartbeat_to_redis_2_times).start()
        self._last_show_pool_instance_num = time.time()

    def _run__send_heartbeat_to_redis_2_times(self):
        """ 使开始时候快速检测两次"""
        self._send_heartbeat_to_redis()
        time.sleep(2)
        self._send_heartbeat_to_redis()

    def _send_heartbeat_to_redis(self):
        all_identify = self.redis_db.smembers(self.redis_key_pool_identify)
        for identify in all_identify:
            identify_dict = json.loads(identify)
            if identify_dict['current_process_flag'] == self.current_process_flag:
                self.redis_db.srem(self.redis_key_pool_identify, identify)
            if time.time() - identify_dict['last_heartbeat_ts'] > self._heartbeat_interval + 1:
                self.redis_db.srem(self.redis_key_pool_identify, identify)
        self.redis_db.sadd(self.redis_key_pool_identify, json.dumps(
            {'current_process_flag': self.current_process_flag, 'last_heartbeat_ts': time.time(),
             'last_heartbeat_time_str': time.strftime('%Y-%m-%d %H:%M:%S')}))
        pool_instance_num = self.redis_db.scard(self.redis_key_pool_identify)
        if time.time() - self._last_show_pool_instance_num > 60:
            self.logger.debug(f'分布式环境中一共有 {pool_instance_num} 个  {self._pool_identify} 标识的线程池')
        self.time_interval = (1.0 / self.tps) * pool_instance_num if self.tps != 0 else 0


class TpsThreadpoolExecutorWithMultiProcess(nb_log.LoggerMixin):
    """ 自动开多进程 + 线程池的方式。 例如你有一台128核的压测机器 对 web服务端进行压测，要求每秒压测1万 tps，单进程远远无法做到，可以方便设置 process_num 为 100"""

    def _start_a_threadpool(self, ):
        ttp = TpsThreadpoolExecutor(tps=self.tps / self.process_num, max_workers=self._max_works)  # noqa
        while True:
            func, args, kwargs = self.queue.get()  # 结束可以放None，然后这里判断，终止。或者joinable queue
            future = ttp.submit(func, *args, **kwargs)
            future.add_done_callback(self._queue_call_back)

    # noinspection PyUnusedLocal
    def _queue_call_back(self, result):
        self.queue.task_done()

    def __init__(self, tps=0, max_workers=500, process_num=1):
        # if os.name == 'nt':
        #     raise EnvironmentError('不支持win')
        # self.queue = multiprocessing.Queue(1)
        self._max_works = max_workers
        self.queue = multiprocessing.JoinableQueue(1)  # mu
        self.tps = tps
        self.process_num = process_num
        self.time_interval = 1 / tps if tps != 0 else 0
        self._lock_for_submit = multiprocessing.Lock()
        for _ in range(process_num):
            multiprocessing.Process(target=self._start_a_threadpool, daemon=True).start()
        atexit.register(self._at_exit)

    def submit(self, func, *args, **kwargs):
        self.queue.put((func, args, kwargs))

    def shutdown(self, wait=True):
        pass

    def _at_exit(self):
        self.logger.warning('触发atexit')
        self.queue.join()


# noinspection PyMethodOverriding
class DistributedTpsThreadpoolExecutorWithMultiProcess(TpsThreadpoolExecutorWithMultiProcess):
    """ 自动开多进程 + 线程池的方式。 例如你有6台16核的压测机器 对 web服务端进行压测，要求每秒压测1万 tps，单进程远远无法做到，可以方便设置 process_num 为 100"""

    def _start_a_threadpool(self):
        ttp = DistributedTpsThreadpoolExecutor(tps=self.tps, max_workers=self._max_works, pool_identify=self.pool_identify, redis_url=self.redis_url)  # noqa
        while True:
            func, args, kwargs = self.queue.get()
            future = ttp.submit(func, *args, **kwargs)
            future.add_done_callback(self._queue_call_back)

    # noinspection PyMissingConstructor
    def __init__(self, tps=0, max_workers=500, process_num=1, pool_identify: str = None, redis_url: str = 'redis://:@127.0.0.1/0'):
        self.pool_identify = pool_identify
        self.redis_url = redis_url
        self.queue = multiprocessing.JoinableQueue(1)
        self.tps = tps
        self.process_num = process_num
        self.time_interval = 1 / tps if tps != 0 else 0
        self._max_workers = max_workers
        # self.ttp = DistributedTpsThreadpoolExecutor(tps=self.tps, pool_identify=self.pool_identify, redis_url=self.redis_url)
        for _ in range(process_num):
            multiprocessing.Process(target=self._start_a_threadpool, daemon=True).start()
        atexit.register(self._at_exit)


def f1(x):
    time.sleep(0.5)
    print(os.getpid(), x)


def f2(x):
    time.sleep(7)
    print(os.getpid(), x)


def request_baidu():
    import requests
    resp = requests.get('http://www.baidu.com/content-search.xml')
    print(os.getpid(), resp.status_code, resp.text[:10])


if __name__ == '__main__':
    # tps_pool = TpsThreadpoolExecutor(tps=7)  # 这个是单机控频
    # tps_pool = DistributedTpsThreadpoolExecutor(tps=7, pool_identify='pool_for_use_print')  # 这个是redis分布式控频，不是基于频繁incr计数的，是基消费者数量统计的。
    tps_pool = TpsThreadpoolExecutorWithMultiProcess(tps=8, process_num=3)  # 这个是redis分布式控频，不是基于incr计数的，是基于
    # tps_pool = DistributedTpsThreadpoolExecutorWithMultiProcess(tps=4, pool_identify='pool_for_use_print', redis_url='redis://:372148@127.0.0.1/0', process_num=5)  # 这个是redis分布式控频，不是基于incr计数的，是基于
    for i in range(100):
        tps_pool.submit(f1, i)
        tps_pool.submit(f2, i * 10)
        # tps_pool.submit(request_baidu)
