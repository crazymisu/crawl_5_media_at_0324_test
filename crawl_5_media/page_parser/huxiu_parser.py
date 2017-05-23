# -*- coding: utf-8 -*-
import re
import requests
import urlparse
import random
import scrapy
import datetime
import bs4
from parsel import Selector
from bs4 import BeautifulSoup
from datetime import datetime
import copy
import traceback

from crawl_5_media.items import SentKafkaMessage
from crawl_5_media.page_parser.parser import Parser
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from crawl_5_media.utils.save_video_to_share_server import save_video_file_to_server
from crawl_5_media.localsettings import IS_CRAWL_NEXT_PAGE

class HuxiuParser(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def parse_list_page(self, response):
        '''
            parse article url
            example:
                sent_kafka_message = SentKafkaMessage()
                sent_kafka_message['url'] = url
                sent_kafka_message['title'] = title
                ...
                yield sent_kafka_message
        '''
        try:
            original_sent_kafka_message = response.meta['queue_value']        

            urls = response.xpath('//h2/a/@href').extract()
            for url in urls:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                sent_kafka_message['url'] = 'https://www.huxiu.com' + url
                sent_kafka_message['response_url'] = sent_kafka_message['url']
                sent_kafka_message['url_domain'] = 'huxiu.com'
                sent_kafka_message['parse_function'] = 'parse_detail_page'
                yield sent_kafka_message

                # yield scrapy.Resquest(sent_kafka_message['url'], callback=self.parse_detail_page, meta={'queue_value': "sent_kafka_message"})

        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())



    def parse_detail_page(self, response):
        try:
            sent_kafka_message = response.meta['queue_value']

            # sent_kafka_message = {}
            '''
                parse detail page
                example:
                    page_html = response.body
                    parse_result =  self.parse_article_content(page_html)
                    
            '''
            print sent_kafka_message['response_url']

            sent_kafka_message['status_code'] = response.status  # HTTP status code
            sent_kafka_message['title'] = response.xpath('//h1[@class="t-h1"]/text()').extract()[0].strip()
            sent_kafka_message['body'] = response.body
            sent_kafka_message['publish_time'] = self.get_publish_time(response)
            sent_kafka_message['info_source'] = response.xpath('//div[@class="author-name"]/a/text()').extract()[0].strip()

            sent_kafka_message['tags'] = self.get_tags(response)
            sent_kafka_message['like_count'] = int(response.xpath('//div[@class="Qr-code"]//span/text()').extract()[1].strip())  # 点赞数
            sent_kafka_message['comment_count'] = self.get_comment_count(response)

            sent_kafka_message['video_location'] = self.get_vedio_location(response)

            sent_kafka_message['small_img_location'],check_flag = self.get_small_img_location(response)
            if sent_kafka_message['small_img_location']:   # 如果下载成功，且有图片
                sent_kafka_message['small_img_location_count'] = len(sent_kafka_message['small_img_location'])

            # sent_kafka_message['img_location'],check_flag = self.get_img_location(response)
            # if sent_kafka_message['img_location'] is not None:  # 如果下载成功，且有图片
            #     sent_kafka_message['img_location_count'] = len(sent_kafka_message['img_location'])  # 图片数量
            #
            #
            # sent_kafka_message['parsed_content'] = self.get_parsed_content(response, sent_kafka_message['img_location'], sent_kafka_message['small_img_location'])
            # sent_kafka_message['parsed_content_main_body'] = self.get_parsed_content_main_body(sent_kafka_message['parsed_content'])
            # sent_kafka_message['parsed_content_char_count'] = len(sent_kafka_message['parsed_content_main_body'])

            content_html = self.get_content_html(response)
            parsed_content, parsed_content_main_body, parsed_content_char_count, img_location, img_location_count = self.parse_article(
                content_html, sent_kafka_message['publish_time'], sent_kafka_message['small_img_location']
            )

            sent_kafka_message['img_location'] = img_location
            sent_kafka_message['img_location_count'] = img_location_count

            sent_kafka_message['parsed_content'] = parsed_content  # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
            sent_kafka_message['parsed_content_main_body'] = parsed_content_main_body  # 按照规定格式解析出的文章纯文本 ; string type

            return sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())
            print(traceback.format_exc())


        
    # 发布时间
    def get_publish_time(self, response):
        publish_time = response.xpath(
            '//div[@class="article-author"]/div[@class="column-link-box"]/span[@class="article-time pull-left"]/text()').extract()[
            0].strip()

        return publish_time + ':00'


    # 文章大图相关信息 [{img_src: '', img_src: '', 'img_index':'', 'img_desc':''}]
    def get_img_location(self, response):
        try:
            img_urls = response.xpath('//span[@class="img-center-box"]/img/@src|//p[@class="img-center-box"]/img/@src').extract()
        except:
            img_urls = None
        try:
            head_img_url = response.xpath('//div[@class="article-img-box"]/img/@src').extract()[0].strip()
        except:
            head_img_url = None
        if img_urls is not None:  # 如果文章中有图片
            img_location = []
            if head_img_url is not None:
                img_urls.insert(0, head_img_url)
            for url in img_urls:  # check_flag为false的时候下载图片失败
                check_flag, img_file_info = save_img_file_to_server(url, self.mongo_client, self.redis_client, self.redis_key, self.get_publish_time(response))
                if check_flag:  # 如果图片下载成功
                    img = {
                        'img_src': url,
                        'img_path': img_file_info['img_file_name'],
                        'img_width': img_file_info['img_width'],
                        'img_height': img_file_info['img_height'],
                        'img_index': str(img_urls.index(url) + 1),
                        'img_desc': None
                    }
                else:
                    img = {
                        'img_src': url,
                        'img_path': None,
                        'img_width': None,
                        'img_height': None,
                        'img_index': str(img_urls.index(url) + 1),
                        'img_desc': None
                    }
                img_location.append(img)
                # else:  # 如果没有下载成功，返回check_flag为False
                #     return None, False
            return img_location, True  # 如果都下载成功，返回check_flag为True
        else:
            return None, True # 没有图片返回None，返回check_flag为True


    # 文章小图相关信息 [{img_src: '', img_src: '', 'img_index':'', 'img_desc':''}]
    def get_small_img_location(self, response):
        try:
            url = response.xpath('//div[@class="article-img-box"]/img/@src').extract()[0].strip()
        except:
            url = None
        if url is not None:  # 如果有小图
            check_flag, img_file_info = save_img_file_to_server(url, self.mongo_client, self.redis_client, self.redis_key, self.get_publish_time(response))
            if check_flag:  # 如果下载成功
                small_img_location = [{
                    'img_src': url,
                    'img_path': img_file_info['img_file_name'],
                    'img_width': img_file_info['img_width'],
                    'img_height': img_file_info['img_height'],
                    'img_index': '1',  # 每个文章最多一个小图
                    'img_desc': None
                }]
            else:
                small_img_location = [{
                    'img_src': url,
                    'img_path': None,
                    'img_width': None,
                    'img_height': None,
                    'img_index': '1',  # 每个文章最多一个小图
                    'img_desc': None
                }]
            return small_img_location, True
            # else:
            #     return None, False
        else:
            return None, True

    # 文章视频相关信息 [{video_src: '', video_src: '', video_index: '', video_desc: '', video_duration: ''}]
    def get_vedio_location(self, response):   # 还没写好
        try:
            videos = response.xpath('//div[@class="article-video-content"]').extract()
        except:
            videos = None
        if videos is not None:
            video_location = []
            for v in videos:
                video_url = re.search(r"file: '(.*?)',", v, re.S).group(1)
                check_flag, video_info = save_video_file_to_server(video_url, self.mongo_client, self.redis_client, self.redis_key, self.get_publish_time(response))
                video = {'video_src': video_url, 'video_path': video_info['video_path'], 'video_index': 1,
                     'video_desc': '', 'video_duration': video_info['video_duration'],
                     'video_width': video_info['video_width'], 'video_height': video_info['video_height']}
                video_location.append(video)
            return video_location
        else:
            return None

    # 按照规定格式解析出的文章正文 <p>一段落</p>
    def get_content_html(self, response):
        base_body = response.xpath('//div[@class="article-content-wrap"]').extract()[0].strip()  # 文章段落原生版
        try:
            shouquan = response.xpath('//div[@class="neirong-shouquan-public"]').extract()[0]  # 去除关注虎嗅公众号

            base_body = base_body.replace(shouquan, '')
        except:
            pass

        try:
            shouquan2 = response.xpath('//div[@class="neirong-shouquan"]').extract()[0] # 去除作者授权声明

            base_body = base_body.replace(shouquan2, '')
        except:
            pass

        return base_body

    # 按照规定格式解析出的文章纯文本
    # def get_parsed_content_main_body(self, parsed_content):
    #     base_body = parsed_content
    #     re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    #     re_br = re.compile('<br\s*?/?>')  # 处理换行
    #     re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    #     re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    #     s = re_style.sub('', base_body)  # 去掉style
    #     s = re_br.sub('\n', s)  # 将br转换为换行
    #     s = re_h.sub('', s)  # 去掉HTML 标签
    #     s = re_comment.sub('', s)  # 去掉HTML注释
    #     blank_line = re.compile('\n+')
    #     parsed_content_main_body = blank_line.sub('\n', s)
    #     return parsed_content_main_body.strip()

    # 文章关键词标签
    def get_tags(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        try:
            tag_box = soup.find_all('div', attrs={'class': 'tag-box'})[0]
            a = tag_box.find_all('a')
            tags = []
            for t in a:
                tag = t.get_text()
                tags.append(tag)
        except:
            tags = None

        return tags

    def get_comment_count(self, response):
        pattern = re.compile(r'(\d+)')
        c_count = response.xpath(
            '//div[@class="article-author"]/div[@class="column-link-box"]/span[@class="article-pl pull-left"]/text()').extract()[
            0].strip()
        comment_count = re.findall(pattern, c_count)[0]  # 评论
        return int(comment_count)

    def parse_article(self, content_html, pub_date, small_img_location):
        parsed_content = ''
        parsed_content_main_body = ''
        parsed_content_char_count = 0
        img_location = []
        img_location_count = 0
        img_index = 1

        if content_html:
            soup = BeautifulSoup(content_html,'lxml')
            parsed_content_main_body = soup.body.div.text.replace(u"\n", "").replace(u'\r',u'').replace(u' ', u'').strip()  # 纯文本
            parsed_content_char_count = len(parsed_content_main_body)  # 字符个数
            p_content_document_list = soup.body.div.children  # 段落
            for p in p_content_document_list:
                if isinstance(p,bs4.Tag):
                    if p.findChild('img'):
                        check_flag, img_file_info = save_img_file_to_server(p.img['src'],mongo_client=self.mongo_client,redis_client=self.redis_client , redis_key=self.redis_key,save_date=pub_date)
                        if check_flag:
                            img_location.append({'img_src':p.img['src'],'img_path':img_file_info['img_file_name'],'img_width': img_file_info['img_width'],'img_height': img_file_info['img_height'],'img_desc':None,'img_index':img_index})
                        else:
                            img_location.append({'img_src':p.img['src'],'img_path':None,'img_desc':None,'img_index':img_index})
                        img_location_count += 1
                        img_index +=1
                        parsed_content += u'%s"%s"%s'% (u'<p><img src=',img_file_info['img_file_name'],u'/></p>')
                        if p.text != u' ':
                            p_content = u'%s%s%s' % (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            # print p_content
                            parsed_content += p_content

                    else:
                        if p.text!= u' ':
                            p_content = u'%s%s%s'% (u'<p>',p.text.replace(u' ',u'').strip(),u'</p>')
                            parsed_content += p_content

            parsed_content = parsed_content.replace(u'\n',u'').replace(u'\r',u'').replace(u'<p></p>',u'')

            if small_img_location :
                for s in small_img_location:
                    parsed_content = '<p><img src="%s"></p>%s' % (s['img_path'], parsed_content)

        return parsed_content ,parsed_content_main_body,parsed_content_char_count,img_location,img_location_count
