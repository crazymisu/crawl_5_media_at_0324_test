# -*- coding: utf-8 -*-
__author__ = 'liangchu'
__date__ = '2017/04/11'

import requests
from PIL import Image
import hashlib
import os
import uuid
import traceback

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

import oss2
from crawl_5_media.localsettings import oss_info, environment

REQUESTS_HADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                   'Accept-Encoding': 'gzip, deflate, sdch, br',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Cache-Control': 'max-age=0',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36'
                   }


def download_image_file_to_server(imgurl, img_save_path, img_logger,headers=None):
    img_info = {}
    download_flag = False
    message = ''
    save_file_name = ''
    if not headers:
        headers = REQUESTS_HADERS
    else:
        headers = headers
    try:
        res = requests.get(imgurl, timeout=3, headers=headers)
        if res.status_code == 200:
            img_content_length = res.headers.get('content-length')            
            orig_image = Image.open(BytesIO(res.content))
            if res.content:
                img_info = {'flag': True,
                            'format': orig_image.format,
                            'size': orig_image.size,
                            'width': orig_image.size[0],
                            'height': orig_image.size[1],
                            'content_length': img_content_length,
                            'message': None}

                dir_path = img_save_path.replace(img_save_path.split('/')[-1], '')
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)

                save_file_name = '%s.%s' % (img_save_path, orig_image.format)
                with open(save_file_name, "wb") as img:
                    img.write(res.content)
                    img.close()

                download_flag = True
                img_info['img_path'] = save_file_name
                return img_info
            else:
                message = 'response content is None!'
        else:
            message = 'res.status_code != 200'
            return {'flag': download_flag, 'message': message}
    except Exception, e:
        message = traceback.format_exc()
        print(message)
        if save_file_name and os.path.exists(save_file_name):
            os.remove(save_file_name)
        return {'flag': download_flag, 'message': message}

    return img_info if img_info else {'flag': False, 'message': message}


def download_img_file_to_cloud(imgurl, img_save_path, img_logger,headers=None):
    img_info = {}
    download_flag = False
    message = ''
    save_file_name = ''
    if not headers:
        headers = REQUESTS_HADERS
    else:
        headers = headers
    put_oss_log_template = 'put file to oss log: "put_flag" = "{0}" ; "url" = "{1}" ; "saved_path" = "{2}" ; "status" = "{3}" ; "put_time" = "{4}" ; "message" = "{5}"'

    try:
        r = requests.get(imgurl, timeout=3, headers=headers, stream=True, allow_redirects=True)
        if r.status_code == 200:
            img_content_length = r.headers.get('content-length')
            file_raw_data = r.content
            orig_image = Image.open(BytesIO(file_raw_data))            
            if file_raw_data:
                img_info = {'flag': True,
                            'format': orig_image.format,
                            'size': orig_image.size,
                            'width': orig_image.size[0],
                            'height': orig_image.size[1],
                            'content_length': img_content_length,
                            'message': None}

                save_file_name = '%s.%s' % (img_save_path, orig_image.format)
                img_info['img_path'] = save_file_name

                while_index = 0
                put_flag = False
                while not put_flag and while_index <= oss_info['RETRY_TIME']:
                    while_index += 1
                    put_flag, put_object_result_status, put_object_result_message = put_img_file_to_cloud(save_file_name, file_raw_data, img_logger)                
                    if put_flag:
                        img_logger.info(put_oss_log_template.format(put_flag, imgurl, save_file_name, put_object_result_status, while_index, None))
                    else:
                        img_logger.info(put_oss_log_template.format(put_flag, imgurl, save_file_name, put_object_result_status, while_index, put_object_result_message))

                if put_flag:                    
                    return img_info
                else:                    
                    return {'flag': download_flag, 'message': 'put_flag is False!'}
        else:
            message = 'res.status_code != 200'
            return {'flag': download_flag, 'message': message}
    except Exception, e:
        message = traceback.format_exc()
        return {'flag': download_flag, 'message': message}


def put_img_file_to_cloud(img_path, file_content, img_logger):
    APPKEY_ID = oss_info['APPKEY_ID']
    APPKEY_SECRET = oss_info['APPKEY_SECRET']
    ENDPOINT = oss_info[environment]['ENDPOINT']
    BUCKET_NAME = oss_info['BUCKET_NAME']

    auth = oss2.Auth(APPKEY_ID, APPKEY_SECRET)
    bucket = oss2.Bucket(auth, ENDPOINT, BUCKET_NAME)

    put_flag = False
    put_object_result_status = None
    put_object_result_message = None
    
    try:
        put_object_result = bucket.put_object(img_path[1:], file_content)
        if put_object_result.status == 200:
            put_flag = True
        else:
            img_logger.error('oss2 put object result fail status code = {0}'.format(put_object_result.status))
        put_object_result_status = put_object_result.status
    except Exception as e:            
        img_logger.error(traceback.format_exc())
        if hasattr(e, 'status'):
            put_object_result_status = e.status
            put_object_result_message = str(e)
        else:
            img_logger.error(e)
        put_object_result_status = e.status

    return put_flag, put_object_result_status, put_object_result_message