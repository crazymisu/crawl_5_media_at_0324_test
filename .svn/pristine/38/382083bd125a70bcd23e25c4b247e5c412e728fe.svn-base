import pickle

from crawl_5_media.utils.proxy_handler import feed_proxy_url_to_redis_queue, get_ip_from_redis


class ProxyMiddleware(object):

    def process_request(self, request, spider):
        sent_kafka_message = request.meta['queue_value']
        data_source_id = sent_kafka_message['data_source_id']

        if 'CZZ-JH' in data_source_id or 'CZZ-TTH' in data_source_id:
            proxy_ip = get_ip_from_redis(spider.db_3_redis_client)
            request.meta['proxy'] = proxy_ip
            request.meta['download_timeout'] = 10
            # else:                
            #     if spider.redis_client.llen(spider.redis_proxy_key) < 50:
            #         feed_proxy_url_to_redis_queue(spider.redis_client, spider.redis_proxy_key, 100)
            #     proxies = eval(spider.redis_client.lpop(spider.redis_proxy_key))
            #     request.meta['proxy'] = proxies['http']
            #     request.meta['download_timeout'] = 10