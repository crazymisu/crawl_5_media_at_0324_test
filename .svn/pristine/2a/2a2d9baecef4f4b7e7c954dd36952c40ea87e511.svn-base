# -*- coding: utf-8 -*-
from datetime import datetime
import copy
import traceback
from crawl_5_media.page_parser.parser  import Parser
from crawl_5_media.utils.save_file_to_share_server import save_img_file_to_server
from urlparse import urlparse, urlunparse
from bs4 import BeautifulSoup
import time
class CcidentParse(Parser):
    def __init__(self):
        super(Parser, self).__init__()
        self.init()

    def get_class_by_name(self,one_article,class_name=None):
        return one_article.select(".%s" % str(class_name))

    def get_by_class_of_attrs(self,select_class,attrs_index=None,attrs_name=None):
        return  select_class[attrs_index].get(str(attrs_name))

    def get_by_class_of_element(self,select_class,element_index=None,element_name=None):
        return  select_class[element_index].select(element_name)

    def one_article_set_attrs(self,sent_kafka_message,one_article):
        select_title_partition = self.get_class_by_name( one_article, class_name="t_zi1")
        title_v2 = self.get_by_class_of_attrs(select_title_partition,attrs_index=0,attrs_name='title')
        response_url_v2=self.get_by_class_of_attrs(select_title_partition,attrs_index=0,attrs_name='href')
        select_dec_partition=self.get_class_by_name(one_article, class_name="plist1_p") #.select('p').getText()
        desc_v2=self.get_by_class_of_element(select_dec_partition,element_index=0,element_name='p')[0].getText()

        img_src=one_article.select('.plist1_img')[0].select('img')[0].get('src')
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        small_img_location_v2 = self.get_small_img_location(img_src, now_time)
        small_img_location_count_v2=1
        select_tags_class=self.get_class_by_name( one_article, class_name="tags")
        tags_v2=self.get_by_class_of_attrs(select_tags_class,attrs_index=0,attrs_name="tags").split(" ")
        like_count_v2=self.get_class_by_name( one_article, class_name="cy_cmt_count")[0].getText()
        comment_count_v2 = self.get_class_by_name(one_article, class_name="t_icon3")[0].getText()
        put_var_in_diz(sent_kafka_message, __file__,locals(),'_v2')

    def set_article_info(self,article_info,sent_kafka_message):
        publish_time_source_author = article_info.split(u"发布时间：")[1].split(u"来源：")
        publish_time = time.strptime(publish_time_source_author[0].strip(), "%Y-%m-%d %H:%M")
        publish_time_v4 = time.strftime('%Y-%m-%d %H:%M:%S', publish_time)
        source_author = publish_time_source_author[1].split(u"作者：")
        info_source_v4 = source_author[0].strip()
        author_v4 = source_author[1].strip()
        put_var_in_diz(sent_kafka_message,__file__,locals(),'_v4')

    def get_small_img_location(self, img_src, publish_time):
        check_flag, img_file_info = save_img_file_to_server(img_src, self.mongo_client, self.redis_client,
                                                            self.redis_key, publish_time)
        if check_flag:
            img = {
                'img_src': img_src,
                'img_path': img_file_info['img_file_name'],
                'img_width': img_file_info['img_width'],
                'img_height': img_file_info['img_height'],
                'img_index': 1,
                'img_desc': None
            }
            return img, True
        else:
            img = {
                'img_src': img_src,
                'img_path': None,
                'img_width': None,
                'img_height': None,
                'img_index': 1,
                'img_desc': None
            }
            return img, False

    def parse_list_page(self, response):
        try:
            original_sent_kafka_message = response.meta['queue_value']
            html=response.body
            url_v1=response.url
            url_domain_v1=urlparse(url_v1).netloc
            put_var_in_diz(original_sent_kafka_message,__file__,locals(),"_v1")
            soup=BeautifulSoup(html,"lxml")
            documnt_list=soup.select(".plist1 ")

            for one_article in documnt_list:
                sent_kafka_message = copy.deepcopy(original_sent_kafka_message)
                self.one_article_set_attrs(sent_kafka_message,one_article)
                yield sent_kafka_message



        except Exception as e:
            # print(traceback.format_exc())
            self.error_logger.error(traceback.format_exc())



    def parse_detail_page(self, response):
        print "-------"*4
        try:
            sent_kafka_message = response.meta['queue_value']
            # sent_kafka_message={}
            status_code_v3 = response.status
            body_v3 = response.body
            soup = BeautifulSoup(body_v3, "lxml")
            article_info=self.get_class_by_name(soup,"tittle_j")[0].getText()
            self.set_article_info(article_info,sent_kafka_message)
            parsed_content_div_by_class=self.get_class_by_name(soup,"main_content")[0]
            parsed_content_main_body_v3=parsed_content_div_by_class.getText()
            parsed_content_char_count_v3=len(parsed_content_main_body_v3)
            parsed_content_v3="".join([str(i) for i in parsed_content_div_by_class.select('p')])
            img_src=parsed_content_div_by_class.select('img')[0].get('src')
            publish_time=sent_kafka_message.get('publish_time',datetime.now())
            img_location_v3=self.get_small_img_location(img_src,publish_time )
            put_var_in_diz(sent_kafka_message, __file__, locals(), '_v3')
            # print_dict(sent_kafka_message)
            yield  sent_kafka_message
        except Exception as e:
            self.error_logger.error(traceback.format_exc())


def put_var_in_diz(message,path,local,suffix_string):
    if '.pyc' in path:
        path=path[0:len(path)-1]
    names=get_var_name(path,suffix_string)

    for name in names:
        length=len(name)-3
        if local.has_key(name):
            message[name[0:length]]=local[name]

def get_var_name(path,suffix_string):
    f = open(path)  # 返回一个文件对象
    line = f.readline()  # 调用文件的 readline()方法
    names=[]
    while line:
        line=line.strip()
        line_len=line.find(suffix_string)
        if line_len > 0:
            names.append(line[0:line_len+3])
        line = f.readline()
    f.close()
    return names

def print_dict(dicts):
    for key in dicts:
        print "key:%s-->value:%s"%(key,dicts[key])