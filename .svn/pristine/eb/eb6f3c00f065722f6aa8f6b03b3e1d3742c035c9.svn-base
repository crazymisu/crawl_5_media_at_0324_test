# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SentKafkaMessage(scrapy.Item):
    '''
        此部分是从MySql读取的信息，不需要赋值
    '''
    data_source_type = scrapy.Field()
    data_source_category = scrapy.Field()
    data_source_class = scrapy.Field()
    data_source_class_id = scrapy.Field()
    data_source_id = scrapy.Field()
    media = scrapy.Field()

    '''
        此部分是爬虫相关的信息，不需要赋值
    '''
    crawlid = scrapy.Field()
    appid = scrapy.Field()
    id = scrapy.Field()
    timestamp = scrapy.Field()
    
    '''
        此部分是爬虫调度相关的信息，可为空
    '''
    parse_function = scrapy.Field()
        
    '''
        此部分是需要解析的信息，需要赋值
    '''
    url = scrapy.Field() # crawl url
    url_domain = scrapy.Field() # crawl url domain
    response_url = scrapy.Field() # 跳转后的真实url
    status_code = scrapy.Field() # HTTP status code ; int type
    title = scrapy.Field() # title
    desc = scrapy.Field() # 文章摘要 ; string type
    body = scrapy.Field() # 网页源代码 不需要base64加密 ; string type
    publish_time = scrapy.Field() # 发布时间 ; string type
    author = scrapy.Field() # 文章作者
    info_source = scrapy.Field() # 文章的信息来源 ; string type
    video_location = scrapy.Field() # 文章视频相关信息 ; list type ; [{video_src: None, video_path: None, video_index: None, video_desc: None, video_duration: None}]
    img_location = scrapy.Field() # 文章大图相关信息 ; list type ; [{img_src: None, img_path: None, 'img_index': None, 'img_desc': None, 'img_height': None, 'img_width': None}]
    img_location_count = scrapy.Field() # 文章大图个数 ; int type
    small_img_location = scrapy.Field() # 文章小图相关信息 ; list type ; [{img_src: None, img_path: None, 'img_index': None, 'img_desc': None, 'img_height': None, 'img_width': None}]
    small_img_location_count = scrapy.Field() # 文章小图个数 ; int type
    parsed_content = scrapy.Field() # 按照规定格式解析出的文章正文 <p>一段落</p> ; string type
    parsed_content_main_body = scrapy.Field() # 按照规定格式解析出的文章纯文本 ; string type
    parsed_content_char_count = scrapy.Field()# 按照规定格式解析出的文章纯文本个数 ; int type
    tags = scrapy.Field() # 文章关键词标签 ; list type
    like_count = scrapy.Field() # 点赞数 ; int type
    click_count = scrapy.Field() # 点击数 ; int type
    comment_count = scrapy.Field() # 回复数 ; int type
    repost_count = scrapy.Field() # 转发数 ; int type
    authorized = scrapy.Field() # 文章授权信息 ; string type
    article_genre = scrapy.Field() # toutiao分类 ; string type
    info_source_url = scrapy.Field() # toutiao作者，实际是头条号的链接 ; string type
    toutiao_out_url = scrapy.Field() # toutiaohao的新闻url在waibude的显示url ; string type
    toutiao_refer_url = scrapy.Field() # toutiaohao，的新闻url在列表页的显示url ; string type
    toutiao_category_class_id = scrapy.Field() # toutiaohao，的新闻url在列表页的显示url ; string type
    toutiao_category_class = scrapy.Field() # toutiaohao，的新闻url在列表页的显示url ; string type