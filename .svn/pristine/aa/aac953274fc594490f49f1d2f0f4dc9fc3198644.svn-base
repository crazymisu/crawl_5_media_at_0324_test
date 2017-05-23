import pickle

from crawl_5_media.items import SentKafkaMessage


class CheckResponseStatusMiddleware(object):
    REQUESTS_HADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                       'Accept-Encoding': 'gzip, deflate, sdch, br',
                       'Accept-Language': 'en-US,en;q=0.8',
                       'Cache-Control': 'max-age=0',
                       'Connection': 'keep-alive',
                       'Upgrade-Insecure-Requests': '1',
                       'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36'}


    def process_request(self, request, spider):
        for key, value in self.REQUESTS_HADERS.iteritems():
            request.headers[key] = value

    def process_response(self, request, response, spider):
        response_status_code = str(response.status)
        if response_status_code[0] == '2' or response_status_code[0] == '3':
            return response

        sent_kafka_message = request.meta['queue_value']

        if response_status_code == '503':
            if sent_kafka_message and 'CZZ-CTT' in sent_kafka_message['data_source_id']:
                spider.new_logger.info('discard fail url data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3} ; status_code = {4} ; crawlid = {5} ; response_url = {6}'.format(sent_kafka_message['data_source_id'], 
                                                                                                                                                                                              sent_kafka_message['url'], 
                                                                                                                                                                                              sent_kafka_message['id'], 
                                                                                                                                                                                              sent_kafka_message['parse_function'] if 'parse_function' in sent_kafka_message.keys() else 'parse_detail_page', 
                                                                                                                                                                                              response.status, 
                                                                                                                                                                                              sent_kafka_message['crawlid'], 
                                                                                                                                                                                              response.url))
                return response
        
        if sent_kafka_message and isinstance(sent_kafka_message, SentKafkaMessage):
            spider.redis_client.lpush(spider.fail_url_redis_key, pickle.dumps(request.meta['queue_value']))
        
        spider.new_logger.info('crawl fail url data : data_source_id = {0} ; url = {1} ; id = {2} ; parse_function = {3} ; status_code = {4} ; crawlid = {5} ; response_url = {6}'.format(sent_kafka_message['data_source_id'], 
                                                                                                                                                                                          sent_kafka_message['url'], 
                                                                                                                                                                                          sent_kafka_message['id'], 
                                                                                                                                                                                          sent_kafka_message['parse_function'] if 'parse_function' in sent_kafka_message.keys() else 'parse_detail_page', 
                                                                                                                                                                                          response.status, 
                                                                                                                                                                                          sent_kafka_message['crawlid'], 
                                                                                                                                                                                          response.url))
        return request