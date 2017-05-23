# -*- coding: utf-8 -*-
# @Time    : 2017/3/31 下午4:17
# @Author  : zhanyanjun
# @File    : save_video_to_share_server.py
# @Software: PyCharm
import hashlib
import uuid
import re
from datetime import datetime
import os
from crawl_5_media.localsettings import SERVER_VIDEO_PATH
from crawl_5_media.utils.video_downloader import download_video_file_to_cloud
import time
import logging
import socket
socket_hostname = socket.gethostname()

logging.basicConfig(filename='download_video_{0}_{1}.log'.format(socket_hostname, datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
video_logger = logging.getLogger('download_video')
video_logger.setLevel(logging.INFO)

def save_video_file_to_server(video_url, mongo_client, redis_client, redis_key, save_date):
    check_flag, video_file_name = check_hash_md5_url_at_mongo(video_url, mongo_client, redis_client, redis_key, save_date)
    return check_flag, video_file_name

def check_hash_md5_url_at_mongo(video_url, mongo_client, redis_client, redis_key, save_date):
    now_date = datetime.now().date()
    now_date_str = str(now_date)
    hash_val = hashlib.md5(video_url).hexdigest()
    db = mongo_client['JoinMongoCrawledHistory']   # 视频是不是存在这里
    collection = db['article_video_hash']   # 视频是不是存在这里

    find_result = collection.find({'_id': hash_val})
    hash_flag = find_result.count() > 0
    if hash_flag:
        try:
            download_video_result_info = find_result.next()
            video_path = download_video_result_info['video_path']
            video_duration = download_video_result_info['video_duration']
            date = download_video_result_info['date']
            video_url = download_video_result_info['video_url']
            video_width = download_video_result_info['video_width']
            video_height = download_video_result_info['video_height']
            if '/data/shareimg' not in video_path:
                hash_flag = False
            else:
                return True, {'video_url': video_url, 'date': date, 'video_path': video_path, 'video_duration': video_duration, 'video_width': video_width, 'video_height': video_height}
        except Exception as e:
            hash_flag = False
    else:
        real_save_video_path = check_save_video_dir(now_date, save_date)
        video_file_name = os.path.join(real_save_video_path, str(uuid.uuid1()))
        video_logger.info('start' + '+' * 20 + str(datetime.now()) + video_url)
        download_video_result_info = download_video_file_to_cloud(video_url, video_file_name, video_logger)
        video_logger.info('end' + '-' * 20 + str(datetime.now()) + video_url)
        if download_video_result_info['flag']:
            video_path = download_video_result_info['video_path']
            video_duration = download_video_result_info['video_duration']
            video_width = download_video_result_info['video_frame_width']
            video_height = download_video_result_info['video_frame_height']

            insert_value_dic = {'video_url': video_url, 'date': now_date_str, 'video_path': video_path,'video_duration': video_duration, 'video_width': video_width, 'video_height': video_height}

            try:
                insert_value_dic['_id'] = hash_val
                collection.insert(insert_value_dic)
            except Exception as e:
                insert_value_dic.pop('_id')
                collection.update({'_id': hash_val}, {'$set': insert_value_dic}, upsert=False, multi=True)
            video_logger.info(insert_value_dic)
            return True, insert_value_dic
        else:
            video_logger.error({'message': download_video_result_info['message'], 'video_src': video_url})
            return False, download_video_result_info['message']


def check_save_video_dir(now_date, save_date):
    try:
        time_value_list = re.findall(r'\d+', save_date)
        save_date = datetime(int(time_value_list[0]), int(time_value_list[1]), int(time_value_list[2]))
        time_value_list = time_value_list[:3]
    except Exception as e:
        time_value_list = [now_date.year, now_date.month, now_date.day]

    real_save_video_path = os.path.join(SERVER_VIDEO_PATH, time_value_list[0], time_value_list[1], time_value_list[2])
    
    if '/data/shareimg_oss' not in real_save_video_path:
        if not os.path.exists(real_save_video_path):
            os.makedirs(real_save_video_path)  

    return real_save_video_path


def feed_file_url_to_redis_queue(file_info_dic, redis_client, key):
    redis_client.lpush(key, file_info_dic)
