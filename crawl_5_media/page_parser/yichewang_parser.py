# -*- coding: utf-8 -*-
import re
import urlparse
import scrapy
import bs4
from bs4 import BeautifulSoup
import sys
from scrapy.selector import Selector
import json
import copy
import codecs
import re
reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
from datetime import datetime
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class YiCheWangParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']
        #翻页
        url_page = None
        if u"?page" in response.url:
            for i in range(1, 11):
                url_page = response.url.split("=1")[0] + '='+str(i)
                if IS_CRAWL_NEXT_PAGE and url_page:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    print(url_page)
                    sent_kafka_message['url'] = url_page
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    yield sent_kafka_message
        elif u"&pageIndex=1&pageSize=" in response.url:
            for i in range(1, 11):
                next_url = response.url.split("=")
                if len(next_url) == 4:
                    url_page = next_url[0] + '=' + next_url[1] + \
                        '=' + next_url[2] + '=' + str(i)
                    if IS_CRAWL_NEXT_PAGE and url_page:
                        sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                        print(url_page)
                        sent_kafka_message['url'] = url_page
                        sent_kafka_message['parse_function'] = 'parse_list_page'
                        yield sent_kafka_message
        elif u"getTagNewsData&pageIndex" in response.url:
            for i in range(1, 11):
                url_page = response.url.split("getTagNewsData&pageIndex=")[0] + 'getTagNewsData&pageIndex='+str(i)
                if IS_CRAWL_NEXT_PAGE and url_page:
                    sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                    print(url_page)
                    sent_kafka_message['url'] = url_page
                    sent_kafka_message['parse_function'] = 'parse_list_page'
                    yield sent_kafka_message

        if u"?pageindex" in response.url:
            url_list = response.xpath("//div[@class='cartest-card'] | //div[@class='article-card horizon'] | //body/li").extract()
            # print(url_list)
            n = -1
            for each in url_list:
                small_img_location = []
                item = copy.deepcopy(original_sent_kafka_message)
                mess = Selector(text=each)
                # 缩略图
                n += 1
                time_ = mess.xpath("//div[@class='details']//span[@class='time'] | //div[@class='box2']/dl/dd[1]/text()").extract()
                time_ = time_[0].strip() if time_ else None
                publish_time = self.parse_toutiao_publish_time(time_)
                # print(publish_time)
                small_img = {}
                img_src = mess.xpath(
                    "//div[@class='img']/a/img/@src | //div[@class='inner-box']/a/img/@src | //div[@class='box1']/div/a/img/@src").extract()
                img_src = img_src[0] if img_src else None
                small_img['img_src'] = img_src if img_src else None
                if small_img['img_src']:
                    check_flag, img_file_info = save_img_file_to_server(
                        small_img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                    if not check_flag:
                        small_img_location.append({'img_src': small_img['img_src'], 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                        item['small_img_location']=small_img_location[0]
                        item['small_img_location_count'] = len(small_img_location[0])
                    else:
                        small_img['img_path'] = img_file_info['img_file_name']
                        small_img['img_index'] = 1
                        small_img['img_desc'] = None
                        small_img['img_width'] = img_file_info['img_width']
                        small_img['img_height'] = img_file_info['img_height']
                        small_img_location.append(small_img)
                        item['small_img_location'] = small_img_location[0]
                        item['small_img_location_count'] = len(small_img_location[0])
                else:
                    item['small_img_location']=None
                    item['small_img_location_count'] = 0

                each_url = mess.xpath("//div[@class='txt']/h3/a[1]/@href | //div[@class='inner-box']//div[@class='details']/h2/a/@href | //div[@class='box2']/h3/a/@href").extract()
                if u"guandian" in each_url[0]:
                    each_url='http://yiqishuo.yiche.com'+ each_url[0] if each_url else None
                else:
                    each_url = each_url[0].split('.html')[0]+u"_all.html#p1" if each_url else None
                # print(each_url)
                # print(item['small_img_location'])
                item['url'] = each_url
                item['parse_function'] = 'parse_detail_page'
                yield item
            # yield scrapy.Request(url_page, callback=self.parse_list_page)
        else:
            if u"getTagNewsData" in response.url:
                item = copy.deepcopy(original_sent_kafka_message)
                n=-1
                small_img_location = []
                try:
                    content_str=response.body_as_unicode()
                    print(content_str)
                    print("3535"*30)
                    html_content=content_str[15::][:-1]
                    print(html_content)
                    jsonresponse = json.dumps(html_content)
                    print(type(jsonresponse))
                    jsonresponse = json.loads(html_content)
                    print(type(jsonresponse))
                except Exception as e:
                    print e
                for i in jsonresponse:
                    n+=1
                    small_img_location = []
                    small_img = {}
                    try:
                        print i
                    except Exception as e:
                        print e
                    try:
                        time_=i.get('publishTime',None)
                        print(time_)
                    except Exception as e:
                        print e
                    publish_time = self.parse_toutiao_publish_time(time_)
                    img_src=i.get('imageCoverUrl',None)
                    small_img['img_src'] = img_src
                    if small_img['img_src']:
                        check_flag, img_file_info = save_img_file_to_server(
                            small_img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                        if not check_flag:
                            small_img_location.append({'img_src': small_img['img_src'], 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                            item['small_img_location']=small_img_location[0]
                            item['small_img_location_count'] = len(small_img_location[0])
                        else:
                            small_img['img_path'] = img_file_info['img_file_name']
                            small_img['img_index'] = 1
                            small_img['img_desc'] = None
                            small_img['img_width'] = img_file_info['img_width']
                            small_img['img_height'] = img_file_info['img_height']
                            small_img_location.append(small_img)
                            item['small_img_location'] = small_img_location[0]
                            item['small_img_location_count'] = len(small_img_location[0])
                    else:
                        item['small_img_location']=None
                        item['small_img_location_count'] = 0

                    each_url = i.get('url',None)
                    each_url = each_url.split('.html')[0] + u"_all.html#p1" if each_url else None
                    # print(each_url)
                    # print(item['small_img_location'])
                    yield item
            elif u"getPieceNewsList" in response.url:
                item = SentKafkaMessage()
                n=-1
                small_img_location = []
                try:
                    content_str=response.body_as_unicode()
                    # print(content_str)
                    html_content=content_str[25::][:-21]
                    print(html_content)
                    jsonresponse = json.dumps(html_content)
                    # print(type(jsonresponse))
                    jsonresponse = json.loads(html_content)
                    # print(type(jsonresponse))
                except Exception as e:
                    print e
                for i in jsonresponse:
                    n+=1
                    small_img = {}
                    try:
                        print i
                    except Exception as e:
                        print e
                    try:
                        time_=i.get('PublishTime',None)
                        print(time_)
                    except Exception as e:
                        print e
                    publish_time = self.parse_toutiao_publish_time(time_)
                    img_src=i.get('PicCover',None)
                    small_img['img_src'] = img_src if img_src else None
                    if small_img['img_src']:
                        check_flag, img_file_info = save_img_file_to_server(
                            small_img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                        if not check_flag:
                            small_img_location.append({'img_src': small_img['img_src'], 'img_path': None, 'img_index': 1, 'img_desc': None, 'img_width': None, 'img_height': None})
                            item['small_img_location']=small_img_location[0]
                            item['small_img_location_count'] = len(small_img_location[0])
                        else:
                            small_img['img_path'] = img_file_info['img_file_name']
                            small_img['img_index'] = 1
                            small_img['img_desc'] = None
                            small_img['img_width'] = img_file_info['img_width']
                            small_img['img_height'] = img_file_info['img_height']
                            small_img_location.append(small_img)
                            item['small_img_location'] = small_img_location[0]
                            item['small_img_location_count'] = len(small_img_location[0])
                    else:
                        item['small_img_location']=None
                        item['small_img_location_count'] = 0
                    each_url =u"http://www.autoreport.cn" + i.get('Url',None)
                    each_url = each_url.split('.html')[0] + u"_all.html#p1" if each_url else None
                    # print(each_url)
                    # print(item['small_img_location'])
                    yield item

    def parse_detail_page(self, response):
        title = response.xpath("//article//h1[@class='tit-h1']/text() | //h3/a/text() | //div[@class='title_box']/h1/text()").extract()
        if title:
            sent_kafka_message = response.meta['queue_value']
            try:
                sent_kafka_message['small_img_location']=sent_kafka_message['small_img_location']
                # print(sent_kafka_message['small_img_location'])
            except Exception as e:
                print e
            # crawl url domain
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            print(sent_kafka_message['url_domain'])
            # 跳转后的真实url
            sent_kafka_message['response_url'] = response.url
            print(sent_kafka_message['response_url'])
            # HTTP status code
            sent_kafka_message['status_code'] = response.status
            print(sent_kafka_message['status_code'])
            # title
            title = response.xpath("//article//h1[@class='tit-h1']/text() | //h3/a/text() | //div[@class='title_box']/h1/text()").extract()
            sent_kafka_message['title'] = title[0].strip() if title else None
            print(sent_kafka_message['title'])
            # 文章授权说明
            if u"guandian" in response.url:
                authorized=response.xpath("//div[@class='shengming_box']/text()").extract()
                sent_kafka_message['authorized'] = authorized[0].split("：")[1] if authorized else None
            else:
                sent_kafka_message['authorized']=None
            print(sent_kafka_message['authorized'])
            # 网页源代码 不需要base64加密
            sent_kafka_message['body'] = response.body_as_unicode()
            # print(type(sent_kafka_message['body']))
            # print(sent_kafka_message['body'])
            # 发布时间
            publish_time = response.xpath("//div[@class='article-information']/text() | //div[@class='title_box']/p/span/text()").extract()
            if u"guandian" in response.url:
                publish_time = publish_time[1].strip() if publish_time else None
            else:
                publish_time = publish_time[0].strip()if publish_time else None
            sent_kafka_message[
                'publish_time'] = self.parse_toutiao_publish_time(publish_time)
            # sent_kafka_message['publish_time'] = publish_time
            print(sent_kafka_message['publish_time'])
            # 作者
            author = response.xpath(
                "//div[@class='article-information']//span/text() | //div[@class='title_box']/p/span/a/text()").extract()
            # print(author)
            if u"guandian" in response.url:
                sent_kafka_message['author'] = author[0] if author else None
            else:
                sent_kafka_message['author'] = author[0].split("：")[1] if not len(author)==1 else None
            print(sent_kafka_message['author'])
            # 文章的信息来源
            info_source = response.xpath("//div[@class='article-information']//span/a/text()").extract()
            if u"guandian" in response.url:
                sent_kafka_message['info_source']=None
            else:
                sent_kafka_message['info_source'] = info_source[0] if info_source else None
            print(sent_kafka_message['info_source'])
            # 文章大图相关信息
            img_list = response.xpath("//div[@class='article-content']//p/a/img/@src | //div[@class='article-content']//p/img/@src | //div[@class='text_box']/p/img/@src").extract()
            print(img_list)
            img_location = []
            if img_list:
                for each in img_list:
                    img = {'img_path': None, 'img_desc': None, 'img_width': None, 'img_height': None}
                    img['img_src'] = each
                    check_flag, img_file_info = save_img_file_to_server(
                        img['img_src'], self.mongo_client, self.redis_client, self.redis_key, publish_time if publish_time else self.now_date)
                    img['img_index'] = img_list.index(each) + 1
                    if check_flag:
                        img['img_path'] = img_file_info['img_file_name']
                        img['img_desc'] = None
                        img['img_width'] = img_file_info['img_width']
                        img['img_height'] = img_file_info['img_height']

                    img_location.append(img)
                    sent_kafka_message['img_location_count'] = len(img_list)
                    if sent_kafka_message['img_location_count'] == 0:
                        sent_kafka_message['img_location'] = None
                    else:
                        # 文章大图个数
                        sent_kafka_message['img_location'] = img_location

                print(sent_kafka_message['img_location'])
                print(sent_kafka_message['img_location_count'])
            else:
                pass
            # 按照规定格式解析出的文章正文 <p>一段落</p>
            # content_and_img=response.xpath("//div[@class='clearfix contdiv']//p").extract()[:-1]
            # content_str =''.join([i for i in content_and_img])
            # #去掉class="newtext"
            # a = content_str.replace(' class="newtext"', '')
            # #去掉a标签的样式
            # b = re.subn(u' class="ebkw" title="[\u4e00-\u9fa5]+"', "", a)[0]
            # #去掉img标签
            # c= re.subn(u' style="text-align:center"',"",b)[0].encode("utf-8")
            # c=b.replace(' style="text-align:center"','')

            # 按照规定格式解析出的文章纯文本
            content = response.xpath(
                "//div[@class='article-content']//text() | //div[@class='text_box']//text()").extract()
            content = content if content else None
            # print(content)
            sent_kafka_message['parsed_content_main_body'] = ''.join(
                [i for i in content])
            # print(sent_kafka_message['parsed_content_main_body'])
            # 按照规定格式解析出的文章纯文本个数
            # sent_kafka_message['parsed_content_char_count'] = len(response.xpath(
            #     "//div[@class='clearfix contdiv']//p[@class='newtext']/text()").extract()[:-1])
            sent_kafka_message['parsed_content_char_count'] = len(
                sent_kafka_message['parsed_content_main_body'])
            # print(sent_kafka_message['parsed_content_char_count'])
            # 文章关键词标签
            if u"guandian" in response.url:
                sent_kafka_message['tags'] = None
            else:
                tags=response.xpath("//div[@class='keyword-btn-box']/div/a/text()").extract()
                sent_kafka_message['tags'] = tags if tags else None
            # print(sent_kafka_message['tags'])
            # 点赞数
            num = response.xpath(
                "//div[@class='tp-box zz-sty zan']//p/text() | //div[@class='zan_box']//em/text()").extract()
            if u"guandian" in response.url:
                sent_kafka_message['like_count'] = num[0][0] if num else 0
            else:
                sent_kafka_message['like_count'] = num[0][-1] if num else 0
            print(sent_kafka_message['like_count'])
            # 回复数
            sent_kafka_message['comment_count'] = None
            print(sent_kafka_message['comment_count'])
            # 按照规定格式解析出的文章正文 <p>一段落</p>
            try:
                sent_kafka_message['parsed_content'] = self.get_parsed_content(
                    response, publish_time)
                # response.xpath("//div[@class='clearfix contdiv']//p").extract()[:-1]
                print(sent_kafka_message['parsed_content'])
            except Exception as e:
                print e

            yield sent_kafka_message
        else:
            pass

    def get_parsed_content(self, response, publish_time):
        # 文章段落原生版
        publish_time = publish_time
        base_body = response.xpath(
            "//div[@class='article-content'] | //div[@class='text_box']")[0].extract()
        parsed_content = self.filter_tags(
            base_body, publish_time, response)  # a标签替换还没有写
        # print(parsed_content)
        return parsed_content

    def filter_tags(self, htmlstr, publish_time, response):
        publish_time = publish_time
        parsed_content = []
        if htmlstr:
            soup = BeautifulSoup(htmlstr,'lxml')
            # parsed_content_main_body = soup.body.div.text
            p_content_document_list = soup.body.div.children
            for p in p_content_document_list:
                if isinstance(p, bs4.Tag):
                    if p.findChild('img'):
                        try:
                            # img_src = re.findall(
                            #     r'src="(.*)"?', str(p.img), re.S)[0]
                            img_src=p.img['src']
                        except Exception as e:
                            print e
                        try:
                            check_flag, img_file_info = save_img_file_to_server(
                                img_src,
                                self.mongo_client,
                                self.redis_client,
                                self.redis_key,
                                save_date=publish_time)
                            if check_flag:
                                img_path = img_file_info['img_file_name']
                            else:
                                img_path = img_src
                            img_src = '<img src="%s"' % img_path
                        except Exception as e:
                            print e.message
                        try:
                            img_src = '<img src="%s"' % img_path
                        except Exception as e:
                            print e.message
                        p_img = '%s%s%s' % ('<p>', img_src, '</p>')
                        # print(type(p_img))
                        # print(p_img)
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content.append(p_content)
                        parsed_content.append(p_img)
                    else:
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content.append(p_content)
        # print(parsed_content)
        content_str = ''.join([i.decode() for i in parsed_content])
        # print(parsed_content)
        content_text = content_str.replace(
            '<p></p>',
            '').replace(
            '<p> </p>',
            '')
        content_text = content_text.split(' ')
        content_text = ''.join(content_text)
        print(content_text)
        # print("1212"*30)
        return content_text
