# -*- coding: utf-8 -*-
import re
import urlparse
import scrapy
import bs4
from bs4 import BeautifulSoup
import sys
from scrapy.selector import Selector
import json
import codecs
reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
from datetime import datetime
import copy
import traceback
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.redis_client import get_redis_client
from crawl_5_media.utils.mongo_client import get_mongo_client
from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE


class FengHuangParser(Parser):

    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        original_sent_kafka_message = response.meta['queue_value']
        #翻页
        url_page = None
        if u"800/0/1/rtlist" in response.url:
            next_page_url_document = response.xpath("//div[@class='next']//table//a/@href").extract()
            url_page = next_page_url_document[0] if next_page_url_document else None
        elif u"1/list.shtml" in response.url:
            next_page_url_document = response.xpath("//div[@class='next']//table//a/@href").extract()
            url_page = next_page_url_document[1] if next_page_url_document else None

        if IS_CRAWL_NEXT_PAGE and url_page:
            sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
            sent_kafka_message['url'] = url_page
            sent_kafka_message['parse_function'] = 'parse_list_page'
            yield sent_kafka_message

        url_list = response.xpath(
            "//div[@class='col01L']//div[@class='box01']//div[@class='zheng_list pl10 box']  | //div[@class='col_L']//div[@class='box650']//div[@class='box_list clearfix']").extract()
        n = -1
        for each in url_list:
            small_img_location = []
            item = copy.deepcopy(original_sent_kafka_message)
            mess = Selector(text=each)
            # 缩略图
            n += 1
            time_= mess.xpath(
                "//div[@class='Function']/span/text() | //div[@class='box_txt clearfix']/span/text()").extract()
            time_ = time_[0] if time_ else None
            publish_time = self.parse_toutiao_publish_time(time_)
            # print(publish_time)
            small_img = {}
            img_src=mess.xpath(
                 "//div[@class='zxbd clearfix']/div[@class='pic']/a/img/@src | //div[@class='box_pic']/a/img/@src").extract()
            img_src=img_src[0] if img_src else None
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

            each_url=mess.xpath("//h1/a/@href | //h2/a/@href").extract()
            each_url = each_url[0] if each_url else None
            # print(each_url)
            # print(item['small_img_location'])
            item['url'] = each_url
            item['parse_function'] = 'parse_detail_page'
            yield item

            # yield scrapy.Request(each_url, callback=self.parse_detail_page, meta={"queue_value": item})
        # yield scrapy.Request(url_page, callback=self.parse_list_page)

    def parse_detail_page(self, response):
        title = response.xpath("//div[@class='left']//h1/text()").extract()
        page_num=response.xpath("//div[@class='pageNum']").extract()

        if title and (not page_num):
            sent_kafka_message = response.meta['queue_value']
            small_img_list = []
            try:
                small_img_location = response.meta['queue_value']['small_img_location']
                small_img_list.append(small_img_location)
                sent_kafka_message[
                    'small_img_location'] = small_img_list
                # print(sent_kafka_message['small_img_location'])
            except Exception as e:
                print e
                print(traceback.format_exc())
            # crawl url domain
            sent_kafka_message['url_domain'] = response.url.split('/')[2]
            # print(sent_kafka_message['url_domain'])
            # 跳转后的真实url
            sent_kafka_message['response_url'] = response.url
            # print(sent_kafka_message['response_url'])
            # HTTP status code
            sent_kafka_message['status_code'] = response.status
            # print(sent_kafka_message['status_code'])
            # title
            title = response.xpath("//div[@class='left']//h1/text()").extract()
            sent_kafka_message['title'] = title[0] if title else None
            # print(sent_kafka_message['title'])
            # 文章授权说明
            sent_kafka_message['authorized'] = None
            # print(sent_kafka_message['authorized'])
            # 网页源代码 不需要base64加密
            sent_kafka_message['body'] = response.body_as_unicode()
            # print(type(sent_kafka_message['body']))
            # print(sent_kafka_message['body'])
            # 发布时间
            publish_time = response.xpath(
                "//p[@class='p_time']/span[@itemprop='datePublished']/text()").extract()
            publish_time = publish_time[0].strip() if publish_time else None
            sent_kafka_message['publish_time'] = self.parse_toutiao_publish_time(publish_time)
            # sent_kafka_message['publish_time'] = publish_time
            # print(sent_kafka_message['publish_time'])
            # 作者
            author = response.xpath(
                "//p[@class='p_time']/span[@itemprop='author']/span/text()").extract()
            sent_kafka_message['author'] = author[0] if author else None
            # print(sent_kafka_message['author'])
            # 文章的信息来源
            info_source = response.xpath(
                "//p[@class='p_time']/span[@itemprop='publisher']/span/a/text() | //p[@class='p_time']/span[@itemprop='publisher']/span/text()").extract()
            sent_kafka_message['info_source'] = info_source[0] if info_source else None
            # print(sent_kafka_message['info_source'])
                        # 文章大图相关信息
            img_list = response.xpath("//p[@class='detailPic']/img/@src | //p[@align='center']/img/@src | //p/img/@src").extract()
            # print(img_list)
            img_location = []
            if img_list:
                for each in img_list[:-1]:
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
                    sent_kafka_message['img_location_count'] = len(img_list[:-1])
                    if sent_kafka_message['img_location_count'] == 0:
                        sent_kafka_message['img_location'] = None
                    else:
                        # 文章大图个数
                        sent_kafka_message['img_location'] = img_location
                    # else:
                    #     sent_kafka_message[
                    #         'parse_function'] = 'parse_detail_page'
                    #     sent_kafka_message['body'] = None
                    #     yield sent_kafka_message
                # print(sent_kafka_message['img_location'])
                # print(sent_kafka_message['img_location_count'])
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
                "//div[@id='main_content']/p/text() | //p[@class='picIntro']/span/text() | //div[@id='main_content']/p/text()").extract()
            content = content if content else None
            # print(content)
            if content:
                sent_kafka_message['parsed_content_main_body'] = ''.join(
                    [i for i in content])
                # print(sent_kafka_message['parsed_content_main_body'])
                # 按照规定格式解析出的文章纯文本个数
                # sent_kafka_message['parsed_content_char_count'] = len(response.xpath(
                #     "//div[@class='clearfix contdiv']//p[@class='newtext']/text()").extract()[:-1])
                sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])
                # print(sent_kafka_message['parsed_content_char_count'])
                # 文章关键词标签
                sent_kafka_message['tags'] = None
                # print(sent_kafka_message['tags'])
                # 点赞数
                num = response.xpath(
                    "//ul[@class='face_box']/li/span/text()").extract()
                try:
                    sent_kafka_message['like_count'] = num[0] if num else 0
                    # print("****" * 30)
                    # print(sent_kafka_message['like_count'])
                except Exception as e:
                    print e
                    print(traceback.format_exc())
                # 回复数
                # comment_count_list = response.xpath(
                #     "//div[@class='mod-commentTextareaTitle js_head']//span[@class='w-com']//span[@class='w-num']/text()").extract()
                comment_count_list = response.xpath(
                    "//h5[@class='cmt js_commentCount']//a//em/text()").extract()
                sent_kafka_message['comment_count'] = comment_count_list[0] if comment_count_list else None
                # print("3333" * 30)
                # print(sent_kafka_message['comment_count'])
                # 按照规定格式解析出的文章正文 <p>一段落</p>
                try:
                    sent_kafka_message[
                        'parsed_content'] = self.get_parsed_content(response, publish_time)
                    # response.xpath("//div[@class='clearfix contdiv']//p").extract()[:-1]
                    # print(sent_kafka_message['parsed_content'])
                except Exception as e:
                    print e
                    print(traceback.format_exc())

                yield sent_kafka_message
            else:
                pass
        else:
            pass


    def get_parsed_content(self, response,publish_time):
        # 文章段落原生版
        publish_time=publish_time
        base_body = response.xpath(
            "//div[@id='main_content']")[0].extract()
        parsed_content = self.filter_tags(base_body,publish_time,response) # a标签替换还没有写
        # print(parsed_content)
        return parsed_content

    def filter_tags(self, htmlstr,publish_time,response):
        publish_time=publish_time
        parsed_content = ''
        if htmlstr:
            soup = BeautifulSoup(htmlstr,'lxml')
            # parsed_content_main_body = soup.body.div.text
            p_content_document_list = soup.body.div.children
            for p in p_content_document_list:
                if isinstance(p, bs4.Tag):
                    if p.findChild('img'):
                        try:
                            img_src = p.img['src']
                        except Exception as e:
                            print e
                        if u'http://p2.ifengimg.com/a/2016/0810/204c433878d5cf9size1_w16_h16.png' in img_src:
                            p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                            parsed_content += p_content
                        else:
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
                            p_img = '%s%s%s' % ('<p>', img_src, '</p>')
                            # print(type(p_img))
                            # print(p_img)
                            p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                            parsed_content+=p_content
                            parsed_content+=p_img
                    else:
                        p_content = '%s%s%s' % ('<p>', p.text, '</p>')
                        parsed_content+=p_content
        # print(parsed_content)
        content_text = parsed_content.replace('<p></p>', '').replace('<p> </p>','')
        # print(content_text)
        return content_text
