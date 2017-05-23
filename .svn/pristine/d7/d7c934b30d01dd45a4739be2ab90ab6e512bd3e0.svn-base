# -*- coding: utf-8 -*-
__author__ = 'liangchu'
__date__ = '2017/04/27'

import os
import time
try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO
import requests
from datetime import datetime
import cv2
import skvideo.io
import uuid
import logging
import traceback

from crawl_5_media.utils.image_downloader import put_img_file_to_cloud
from crawl_5_media.localsettings import TEMP_SERVER_VIDEO_PATH
from crawl_5_media.localsettings import oss_info


header ={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
         'Accept-Encoding':'gzip, deflate',
         'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3,zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
         'Connection':'keep-alive',
         'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'}


def download_video_file_to_cloud(url, save_path, img_logger):
    download_info = {}
    video_info = {}
    message = ''
    save_file_name = ''
    real_save_local_file_name = ''
    put_oss_log_template = 'put file to oss log: "put_flag" = "{0}" ; "url" = "{1}" ; "saved_path" = "{2}" ; "status" = "{3}" ; "put_time" = "{4}" ; "message" = "{5}"'

    try:
        res = requests.get(url, timeout=20, headers=header, stream=True, allow_redirects=True)
        if res.status_code == 200:
            content_length = res.headers['Content-Length']
            content_type = res.headers['Content-Type'].split(u'/')[1]

            save_file_name = '%s.%s' % (save_path, content_type)
            real_save_local_file_name = os.path.join(TEMP_SERVER_VIDEO_PATH, os.path.basename(save_file_name))

            if not os.path.exists(TEMP_SERVER_VIDEO_PATH):
                os.makedirs(TEMP_SERVER_VIDEO_PATH)

            file_content = ''

            with open(real_save_local_file_name, 'wb') as temp_video:
                for chunk in res.iter_content(chunk_size=128):
                    temp_video.write(chunk)
                    file_content += chunk

            video_meta_data = skvideo.io.ffprobe(real_save_local_file_name)
            FOURCC = video_meta_data['video']['@codec_name']  # 视频编码方式 四字码
            fps = video_meta_data['video']['@avg_frame_rate']  # 帧速率
            width = int(video_meta_data['video']['@coded_width'])  # 帧宽度
            height = int(video_meta_data['video']['@coded_height'])  # 帧高度
            frame_count = int(video_meta_data['video']['@duration_ts'])  # 视频帧数
            video_duration = video_meta_data['video']['@duration']  # 视频时长 /秒

            video_info = {'video_path': save_file_name,
                          'video_code': FOURCC,
                          'video_frame_width': width,
                          'video_frame_height': height,
                          'video_fps': fps,
                          'video_frame_count': frame_count,
                          'video_duration': video_duration,
                          'flag': True,
                          'message':''
                          }

            while_index = 0
            put_flag = False
            while not put_flag and while_index <= oss_info['RETRY_TIME']:
                while_index += 1
                put_flag, put_object_result_status, put_object_result_message = put_img_file_to_cloud(save_file_name, file_content, img_logger)
                if put_flag:
                    img_logger.info(put_oss_log_template.format(put_flag, url, save_file_name, put_object_result_status, while_index, None))
                else:
                    img_logger.info(put_oss_log_template.format(put_flag, url, save_file_name, put_object_result_status, while_index, put_object_result_message))
            os.remove(real_save_local_file_name)

            if put_flag:
                return video_info
            else:                
                return {'flag': download_flag, 'message': 'put_lag is False!'}        
        else:
            message = 'res.status_code != 200'
            return {'flag': False, 'message': message}

    except Exception, e:
        message = traceback.format_exc()
        if real_save_local_file_name and os.path.exists(real_save_local_file_name):
            os.remove(real_save_local_file_name)
        return {'flag': False, 'message': message}


def download_video_file_to_server(url, save_path, img_logger):
    download_info = {}
    video_info = {}
    message = ''
    save_file_name = ''

    try:
        res = requests.get(url, headers=header)
        if res.status_code == 200:
            content_length = res.headers['Content-Length']
            content_type = res.headers['Content-Type'].split(u'/')[1]
            save_path_no_file = save_path.replace(save_path.split('/')[-1], '')
            save_file_name = '%s.%s' % (save_path, content_type)            

            capture = cv2.VideoCapture(res.content)
            video_isopened = capture.isOpened()
            if video_isopened == True:

                put_img_file_to_cloud(save_file_name, res.content, img_logger)

                FOURCC = int(capture.get(cv2.cv.CV_CAP_PROP_FOURCC))  # 视频编码方式 四字码
                fps = capture.get(cv2.cv.CV_CAP_PROP_FPS)  # 帧速率
                width = int(capture.get(cv2.cv.CV_CAP_PROP_FRAME_WIDTH))  # 帧宽度
                height = int(capture.get(cv2.cv.CV_CAP_PROP_FRAME_HEIGHT))  # 帧高度
                frame_count = capture.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)  # 视频帧数
                video_time_length = int(frame_count / fps)  # 视频时长 /秒
                video_info = {'video_save_path': save_file_name,
                              'video_code': FOURCC,
                              'video_frame_width': width,
                              'video_frame_height': height,
                              'video_fps': fps,
                              'video_frame_count': frame_count,
                              'video_time_length': video_time_length,
                              'flag': True,
                              'message':'',
                              }
                return video_info
            else:
                # os.remove(save_file_name)
                logging.info('Failed when opening video %s' % save_path)
                message = 'Failed when opening video'
                capture.release()
                return {'flag': False, 'message': message}
        else:
            message = 'res.status_code != 200'
            return {'flag': False, 'message': message}

    except Exception, e:
        message = traceback.format_exc()
        print(message)
        img_logger.error(message)
        # if save_file_name and os.path.exists(save_file_name):
        #     os.remove(save_file_name)
        return {'flag': False, 'message': message}


def test_put_file_to_cloud_result(file_path):
    url = 'http://donews-test1.oss-cn-beijing-internal.aliyuncs.com' + file_path    
    r = requests.get(url)
    print('+' * 30 + '-' * 30)
    print(url)
    print( r.status_code)
    print('-' * 30 + '+' * 30)