# -*- coding: utf-8 -*-

environment = 'intranet'
# environment = 'external'


mongo_client_info = {'intranet': {'MONGODB_HOST': '10.160.15.209', 'MONGODB_PORT': 27017, 'USER_NAME': 'access_mongo', 'PWD': 'donewsusemongodbby20170222'},
                     'external': {'MONGODB_HOST': '112.124.17.26', 'MONGODB_PORT': 27017, 'USER_NAME': 'access_mongo', 'PWD': 'donewsusemongodbby20170222'}}

test_mongo_client_info = {'intranet': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@10.160.15.209:27017/admin?readPreference=secondaryPreferred'},
                          'external': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@112.124.17.26:27017/?readPreference=secondaryPreferred'}}

redis_client_info = {'intranet': {'REDIS_HOST': '10.44.163.19', 'REDIS_PORT': 6379, 'REDIS_PWD': 'donews_1234'},
                     'external': {'REDIS_HOST': '101.200.174.92', 'REDIS_PORT': 6379, 'REDIS_PWD': 'donews_1234'}}

kafka_client_info = {'intranet': {'KAFKA_HOSTS': ['10.51.119.39:9092','10.44.136.182:9092', '10.44.159.186:9092']},
                     'external': {'KAFKA_HOSTS': None}}

phantomjs_server_info = {'intranet': {'PHANTOMJS_HOSTS': '10.44.163.19:9092'},
                     'external': {'PHANTOMJS_HOSTS': '101.200.174.92:9092'}}
mysql_client_info = {
                        'intranet': {
                                    'MYSQL_DBKWARGS': {
                                        'host': '10.28.49.54',
                                        'user': 'admin001',
                                        'passwd': 'donews1234',
                                        'db': 'monitors',
                                        'charset': 'utf8'
                                    }

                        },
                        'external': {
                                    'MYSQL_DBKWARGS': {
                                        'host': '59.110.52.178',
                                        'user': 'admin001',
                                        'passwd': 'donews1234',
                                        'db': 'monitors',
                                        'charset': 'utf8'
                                    }

                        }
                    }

save_file_path_info = {'intranet': {'SERVER_IMG_PATH': '/data/shareimg_oss2/test/big_media_img/', 'SERVER_VIDEO_PATH': '/data/shareimg_oss2/test/big_media_article_video/', 'TEMP_SERVER_VIDEO_PATH': '/data/shareimg5/test/big_media_article_video/temp/'},
                       'external': {'SERVER_IMG_PATH': '/data/shareimg_oss2/test/big_media_img/', 'SERVER_VIDEO_PATH': '/data/shareimg_oss2/test/big_media_article_video/', 'TEMP_SERVER_VIDEO_PATH': '/data/shareimg5/test/big_media_article_video/temp/'}}


oss_info = {'intranet': {'ENDPOINT': 'oss-cn-beijing-internal.aliyuncs.com'},
            'external': {'ENDPOINT': 'oss-cn-beijing.aliyuncs.com'},
            'APPKEY_ID': 'LTAIEdu2OfvhWD7f', # 用于标识用户
            'APPKEY_SECRET': 'dd2V8omztBSxqHHYvolmwS6HJqnpRn', # 用户用于加密签名字符串和 OSS 用来验证签名字符串的密钥
            'BUCKET_NAME': 'donews-test1' # 存储空间bucket_name
            }

environment_mongo_client_info = mongo_client_info[environment]
environment_redis_client_info = redis_client_info[environment]
environment_test_mongo_client_info = test_mongo_client_info[environment]
environment_kafka_client_info = kafka_client_info[environment]
environment_phantomjs_server_info = phantomjs_server_info[environment]
environment_mysql_client_info = mysql_client_info[environment]
environment_save_file_path_info = save_file_path_info[environment]

MONGODB_HOST = environment_mongo_client_info['MONGODB_HOST']
MONGODB_PORT = environment_mongo_client_info['MONGODB_PORT']
MONGO_USER_NAME = environment_mongo_client_info['USER_NAME']
MONGO_PWD = environment_mongo_client_info['PWD']

REDIS_HOST = environment_redis_client_info['REDIS_HOST']
REDIS_PORT = environment_redis_client_info['REDIS_PORT']
REDIS_PWD = environment_redis_client_info['REDIS_PWD']

KAFKA_HOSTS = environment_kafka_client_info['KAFKA_HOSTS']
PHANTOMJS_HOSTS = environment_phantomjs_server_info['PHANTOMJS_HOSTS']

TEST_MONGO_CONN_STR = environment_test_mongo_client_info['MONGO_CONN_STR']

MYSQL_DBKWARGS = environment_mysql_client_info['MYSQL_DBKWARGS']

SERVER_VIDEO_PATH = environment_save_file_path_info['SERVER_VIDEO_PATH']
SERVER_IMG_PATH = environment_save_file_path_info['SERVER_IMG_PATH']

IS_CRAWL_NEXT_PAGE = True

# 晨光配置文件
hash_file_mongo_client_info = {'intranet': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@10.160.15.209:27017/admin?readPreference=secondaryPreferred'},
                          'external': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@112.124.17.26:27017/?readPreference=secondaryPreferred'}}

save_data_mongo_client_info = {'intranet': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@10.160.15.209:27017/admin?readPreference=secondaryPreferred'},
                          'external': {'MONGO_CONN_STR' :'mongodb://access_mongo:donewsusemongodbby20170222@112.124.17.26:27017/?readPreference=secondaryPreferred'}}

proxy_redis_client_info = {'intranet': {'REDIS_HOST': '10.160.15.209', 'REDIS_PORT': 6379, 'REDIS_PWD': 'donews_1234', 'DB': 3},
                            'external': {'REDIS_HOST': '112.124.17.26', 'REDIS_PORT': 6379, 'REDIS_PWD': 'donews_1234', 'DB': 3}}

oss_info = {'intranet': {'ENDPOINT': 'oss-cn-beijing-internal.aliyuncs.com'},
            'external': {'ENDPOINT': 'oss-cn-beijing.aliyuncs.com'},
            'APPKEY_ID': 'LTAIEdu2OfvhWD7f', # 用于标识用户
            'APPKEY_SECRET': 'dd2V8omztBSxqHHYvolmwS6HJqnpRn', # 用户用于加密签名字符串和 OSS 用来验证签名字符串的密钥
            'BUCKET_NAME': 'donews-test1', # 存储空间bucket_name
            'RETRY_TIME': 10
            }

environment_hash_file_mongo_client_info = hash_file_mongo_client_info[environment]
environment_save_data_mongo_client_info = save_data_mongo_client_info[environment]
environment_redis_client_info = redis_client_info[environment]
environment_proxy_redis_client_info = proxy_redis_client_info[environment]
environment_kafka_client_info = kafka_client_info[environment]
environment_phantomjs_server_info = phantomjs_server_info[environment]
environment_mysql_client_info = mysql_client_info[environment]
environment_save_file_path_info = save_file_path_info[environment]

REDIS_HOST = environment_redis_client_info['REDIS_HOST']
REDIS_PORT = environment_redis_client_info['REDIS_PORT']
REDIS_PWD = environment_redis_client_info['REDIS_PWD']

KAFKA_HOSTS = environment_kafka_client_info['KAFKA_HOSTS']
PHANTOMJS_HOSTS = environment_phantomjs_server_info['PHANTOMJS_HOSTS']

MYSQL_DBKWARGS = environment_mysql_client_info['MYSQL_DBKWARGS']

SERVER_VIDEO_PATH = environment_save_file_path_info['SERVER_VIDEO_PATH']
SERVER_IMG_PATH = environment_save_file_path_info['SERVER_IMG_PATH']
TEMP_SERVER_VIDEO_PATH = environment_save_file_path_info['TEMP_SERVER_VIDEO_PATH']

IS_CRAWL_NEXT_PAGE = True