import hashlib
import uuid
import re
from datetime import datetime
import os
from crawl_5_media.localsettings import SERVER_IMG_PATH
from crawl_5_media.utils.image_downloader import download_img_file_to_cloud
import time
import logging
import socket

socket_hostname = socket.gethostname()

logging.basicConfig(
    filename='download_img_{0}_{1}.log'.format(socket_hostname, datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')),
    level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
img_logger = logging.getLogger('download_img')
img_logger.setLevel(logging.INFO)


def save_img_file_to_server(img_url, mongo_client, redis_client, redis_key, save_date, headers=None):
    if not img_url:
        return False, 'img url is None!'
    check_flag, img_file_info = check_hash_md5_url_at_mongo(img_url, mongo_client, redis_client, redis_key, save_date,
                                                            headers=headers)

    # if check_flag:
    #     feed_file_url_to_redis_queue({'img_url': img_url, 'saved_img_url': img_file_name}, redis_client, redis_key)

    return check_flag, img_file_info


def check_hash_md5_url_at_mongo(img_url, mongo_client, redis_client, redis_key, save_date, headers=None):
    now_date = datetime.now().date()
    now_date_str = str(now_date)
    hash_val = hashlib.md5(img_url).hexdigest()
    db = mongo_client['JoinMongoCrawledHistory']
    collection = db['article_photo_hash']

    find_result = collection.find({'_id': hash_val})
    hash_flag = find_result.count() > 0
    if hash_flag:
        try:
            download_img_result_info = find_result.next()
            img_height = download_img_result_info['img_height']
            img_width = download_img_result_info['img_width']
            img_file_name = download_img_result_info['saved_img_url']

            if '/data/shareimg' not in img_file_name:
                hash_flag = False
            else:
                return True, {'img_file_name': img_file_name, 'img_height': img_height, 'img_width': img_width}
        except Exception as e:
            hash_flag = False

    if not hash_flag:
        real_save_img_path = check_save_img_dir(now_date, save_date)
        img_file_name = os.path.join(real_save_img_path, str(uuid.uuid1()))
        img_logger.info('start' + '+' * 20 + str(datetime.now()) + img_url)
        download_img_result_info = download_img_file_to_cloud(img_url, img_file_name, img_logger, headers=headers)
        img_logger.info('end' + '-' * 20 + str(datetime.now()) + img_url)
        if download_img_result_info['flag']:
            img_file_name = download_img_result_info['img_path']
            img_height = download_img_result_info['height']
            img_width = download_img_result_info['width']
            try:
                collection.insert(
                    {'_id': hash_val, 'img_url': img_url, 'date': now_date_str, 'saved_img_url': img_file_name,
                     'img_height': img_height, 'img_width': img_width})
            except Exception as e:
                collection.update({'_id': hash_val}, {
                    '$set': {'img_url': img_url, 'date': now_date_str, 'saved_img_url': img_file_name,
                             'img_height': img_height, 'img_width': img_width}}, upsert=False, multi=True)
            img_logger.info(
                {'img_file_name': img_file_name, 'img_height': img_height, 'img_width': img_width, 'img_src': img_url})
            return True, {'img_file_name': img_file_name, 'img_height': img_height, 'img_width': img_width}
        else:
            img_logger.error({'message': download_img_result_info['message'], 'img_src': img_url})
            return False, download_img_result_info['message']


def check_save_img_dir(now_date, save_date, save_file_path=None):
    try:
        time_value_list = re.findall(r'\d+', save_date)
        save_date = datetime(int(time_value_list[0]), int(time_value_list[1]), int(time_value_list[2]))
        time_value_list = time_value_list[:3]
    except Exception as e:
        time_value_list = [now_date.year, now_date.month, now_date.day]

    save_file_path = save_file_path if save_file_path else SERVER_IMG_PATH
    real_save_img_path = os.path.join(save_file_path, str(time_value_list[0]), str(time_value_list[1]),
                                      str(time_value_list[2]))

    if '/data/shareimg_oss' not in real_save_img_path:
        if not os.path.exists(real_save_img_path):
            os.makedirs(real_save_img_path)

    return real_save_img_path


def feed_file_url_to_redis_queue(file_info_dic, redis_client, key):
    redis_client.lpush(key, file_info_dic)
