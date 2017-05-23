import requests
import json
import redis
import threading
import time
from optparse import OptionParser
import random

def get_proxy_url_list(count=None):
    if not count:
        count = 5
    token_id = '938176699822329'
    request_proxy_info_url = 'http://ent.kuaidaili.com/api/getproxy/?orderid={1}&num={0}&b_pcchrome=1&b_pcie=1&b_pcff=1&protocol=1&method=1&an_ha=1&sp1=1&sp2=1&quality=1&sort=1&format=json&sep=1'
    request_proxy_info_url = request_proxy_info_url.format(count, token_id)
    r = requests.get(request_proxy_info_url)
    proxy_info_dic = eval(r.text.encode('utf-8'))
    return proxy_info_dic['data']


def feed_proxy_url_to_redis_queue(redis_client, proxy_redis_key, proxy_count):
    proxy_server_url = 'http://10.44.163.19:8080/?num={0}&type=3'.format(proxy_count)
    r = requests.get(proxy_server_url)
    proxy_list = eval(r.text)

    if proxy_list:
        for proxy_url in proxy_list:
            proxies = {'http': 'http://' + proxy_url, 'https': 'https://' + proxy_url}
            proxy_redis_count = redis_client.lpush(proxy_redis_key, proxies)
            print('feed_proxy_url_to_redis_queue length = {0}'.format(proxy_redis_count))
    else:
        proxy_info = get_proxy_url_list(proxy_count)
        for proxy_url in proxy_info['proxy_list']:
            proxies = {'http': 'http://' + proxy_url, 'https': 'https://' + proxy_url}
            if check_proxy_correct(proxies):
                proxy_redis_count = redis_client.lpush(proxy_redis_key, proxies)
                print('feed_proxy_url_to_redis_queue length = {0}'.format(proxy_redis_count))


def check_proxy_correct(proxies):
    check_url = 'http://www.toutiao.com/'
    try:
        r = requests.get(check_url, proxies=proxies, timeout=10)
        if r.status_code == 200:
            return True
    except Exception as e:
        return False

    return False


def get_redis_client(client=None):
    out_redis_host = '101.200.174.92'
    # in_redis_host = '10.44.163.19'
    in_redis_host = '10.160.15.209'

    real_redis_host = in_redis_host

    if not client:
        client = redis.StrictRedis(host=real_redis_host, port=6379, password='donews_1234')
    return client


class ThreadCheckProxy(threading.Thread):    
    def __init__(self, proxy_redis_key, queue_type, redis_client, sleep_time):
        threading.Thread.__init__(self)
        self.key_prefix = 'queue:crawl_source:'
        self.proxy_redis_key = proxy_redis_key
        self.queue_type = queue_type
        self.redis_client = redis_client
        self.sleep_time = sleep_time

    def run(self):
        while redis_client.llen(self.queue_type) > 0:
            feed_proxy_url_to_redis_queue(self.redis_client, self.key_prefix + self.proxy_redis_key, 100)
            time.sleep(self.sleep_time)


def get_option():
    parser = OptionParser()
    parser.add_option('-k', '--key', dest='redis_queue_key')
    parser.add_option('-n', '--num', dest='thread_num')
    parser.add_option('-t', '--time', dest='sleep_time')

    (options, args) = parser.parse_args()
    return (options, args)


def get_ip_from_redis(redis_client):
    ip_count = redis_client.zcount("kdl_proxy", 0, 1000000000000000000)
    if ip_count > 0:
        ip_addr = random.randint(0, ip_count - 1)
        ip = redis_client.zrange('kdl_proxy', ip_addr, ip_addr)
        if ip:
            ip_info = eval(ip[0])
            real_ip = 'http://{0}:{1}'.format(ip_info['ip'], ip_info['port'])
            return real_ip
    return None


if __name__ == '__main__':
    options, args = get_option()
    redis_queue_key = options.redis_queue_key
    thread_num = options.thread_num
    sleep_time = options.sleep_time

    if redis_queue_key:        
        redis_client = get_redis_client()

        key_prefix = 'queue:crawl_source:'
        proxy_redis_key = 'proxies'

        thread_list = []

        for index in range(int(thread_num)):
            thread_check_proxy = ThreadCheckProxy(proxy_redis_key, redis_queue_key, redis_client, int(sleep_time))
            thread_check_proxy.setDaemon(True)
            thread_check_proxy.start()
            thread_list.append(thread_check_proxy)
            time.sleep(10)

        for item in thread_list:
            item.join()
    