from datetime import datetime, timedelta
import time
import os
from collections import OrderedDict
from optparse import OptionParser
import re
import json
import MySQLdb

from crawl_5_media.localsettings import MYSQL_DBKWARGS, environment
from crawl_5_media.utils.mongo_client import get_test_mongo_client


command_dic = OrderedDict({
    'feed_url': 'grep feed {0} | grep data_source_id | grep -c {1}',
    'lpush_detail': 'grep lpush {0} | grep data_source_id | grep {1} | grep -c parse_detail_page',
    'lpush_list': 'grep lpush {0} | grep data_source_id | grep {1} | grep -c parse_list_page',
    'duplicate_url': 'grep check_hash_md5_url_at_redis {0} | grep data_source_id | grep -c {1}',
    'success_url': 'grep insert {0} | grep data_source_id | grep -c {1}',
    'fail_url': 'grep fail {0} | grep data_source_id | grep -c {1}'
})

calculate_one_type_command_dic = OrderedDict({
    'feed_url': 'grep feed {0} | grep -c data_source_id',
    'lpush_detail': 'grep lpush {0} | grep data_source_id | grep -c parse_detail_page',
    'lpush_list': 'grep lpush {0} | grep data_source_id | grep -c parse_list_page',
    'duplicate_url': 'grep check_hash_md5_url_at_redis {0} | grep -c data_source_id ',
    'success_url': 'grep insert {0} | grep -c data_source_id ',
    'fail_url': 'grep fail {0} | grep -c data_source_id ',
    'error_data': 'grep save_error_data {0} | grep -c data_source_id'
})

data_source_id_list = [
                        'CZZ-QLW',
                        'CZZ-HWCJ',
                        'CZZ-GZW',
                        'CZZ-KFW',
                        'CZZ-YO',
                        'CZZ-FHKJ',
                        'CZZ-CTT',
                        'CZZ-HX',
                        'CZZ-YBDL',
                        'CZZ-XFSB',
                        'CZZ-TTH',
                        'CZZ-JH'
]


def get_option():
    parser = OptionParser()    
    parser.add_option('-t', '--type', dest='execute_type')
    parser.add_option('-c', '--content', dest='find_content')

    (options, args) = parser.parse_args()
    return (options, args)


def get_one_day_log_file_list(start_time, end_time):
    one_day_file_list = []
    pwd_path = os.getcwd()
    file_list = os.listdir(pwd_path)
    for file in file_list:
        if '.log' in file:            
            log_time = file.split('_')[-1]
            try:
                log_time = log_time.split('.')[0]
                if '-' in log_time:
                    log_time = datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S')
                else:
                    log_time = datetime.strptime(log_time, '%Y%m%dT%H%M%S')
                if log_time > start_time and log_time < end_time:
                    one_day_file_list.append(file)
                    print(file)
            except Exception as e:
                continue

    return one_day_file_list


def calculate_log_file(one_day_log_list, mongo_client):
    execute_file = ' '.join(one_day_log_list)

    _collection = mongo_client['spider_crawld_result']['big_media_day_log']
    log_date = str(datetime.now().date())

    for data_source_id in data_source_id_list:
        print('*' * 20)
        print(data_source_id)

        insert_value_dic = {'timestamp': log_date, 'data_source_id': data_source_id}

        for command_key, command_template in command_dic.iteritems():
            command = command_template.format(execute_file, data_source_id)
            result = os.popen(command)
            insert_value_dic[command_key] = int(re.search(r'\d+', result.readline()).group())
            print('{0} = {1}'.format(command_key, insert_value_dic[command_key]))

        _collection.insert(insert_value_dic)

        print('*' * 20)


def find_text_from_log_file(content):
    log_start_time = datetime.now().date() + timedelta(days=-1)
    log_end_time = log_start_time + timedelta(days=1)

    one_day_log_list = get_one_day_log_file_list(log_start_time, log_end_time)

    execute_file = ' '.join(one_day_log_list)
    pwd_path = os.getcwd()
    file_list = os.listdir(pwd_path)
    command = 'grep {0} {1}'.format(content, execute_file)
    result = os.popen(command)

    url_index = 0

    result_dic = {'data': []}

    for result_line in result.readlines():
        print(result_line)
        split_one = result_line.split(' url = ')
        if len(split_one) > 1:
            insert_url = split_one[1].split(' ; id')[0]
            result_dic['data'].append(insert_url)
            url_index += 1
            # print(url_index)

    with open('crawl_success_url.json', 'w') as fp:
        json.dump(result_dic, fp, indent=4)


def calculate_list_page():
    log_start_time = datetime.now() + timedelta(days=-1) #05-02
    log_start_time = datetime(log_start_time.year, log_start_time.month, log_start_time.day, 0, 0, 0)
    log_end_time = log_start_time + timedelta(days=1)
    log_end_time = datetime(log_end_time.year, log_end_time.month, log_end_time.day, 0, 0, 0)
    
    # log_start_time = '2017-05-03T14:50:00'
    # log_start_time = datetime.strptime(log_start_time, '%Y-%m-%dT%H:%M:%S')
    # log_end_time = log_start_time + timedelta(days=1)
    
    one_day_file_list = []
    pwd_path = os.getcwd()
    # pwd_path = os.getcwd() + '/DONE/'
    file_list = os.listdir(pwd_path)
    for file in file_list:
        if '.log' in file:            
            log_time = file.split('_')[-1]
            try:
                log_time = log_time.split('.')[0]
                if '-' in log_time:
                    log_time = datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S')
                else:
                    log_time = datetime.strptime(log_time, '%Y%m%dT%H%M%S')

                if log_time > log_start_time and log_time < log_end_time:
                    one_day_file_list.append(file)
                    print(file)
            except Exception as e:
                continue
    
    execute_file = ' '.join(one_day_file_list)

    log_date = str(datetime.now().date())
    data_source_id = os.getcwd()
    insert_value_dic = {'timestamp': str(log_start_time), 'project_path': data_source_id, 'data_source_id': None}

    for key, val in calculate_one_type_command_dic.iteritems():        
        command = 'cd {0} ; '.format(pwd_path) + val.format(execute_file)        
        result = os.popen(command)
        for item in result:
            # print('{0} value = {1}'.format(key, item))
            insert_value_dic[key] = int(re.search(r'\d+', item).group())

    insert_calculate_result_to_mysql(insert_value_dic)


def insert_calculate_result_to_mysql(result_dic):
    sql = '''
        INSERT INTO big_media_day_log (feed_url_count, error_data_count, 
                                        duplicate_url_count, fail_url_count, 
                                        success_url_count, lpush_detail_count,
                                        lpush_list_count, timestamp,
                                        data_source_id, project_path)
        VALUES(%s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s)
    '''

    DBKWARGS = MYSQL_DBKWARGS
    DBKWARGS['db'] = 'supervision'
    conn = MySQLdb.connect(**DBKWARGS)
    cur = conn.cursor()
    cur.execute(sql, [result_dic['feed_url'], result_dic['error_data'],
                        result_dic['duplicate_url'], result_dic['fail_url'], 
                        result_dic['success_url'], result_dic['lpush_detail'],
                        result_dic['lpush_list'], result_dic['timestamp'],
                        result_dic['data_source_id'], result_dic['project_path']])
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__'    :
    options, args = get_option()
    execute_type = options.execute_type   

    if execute_type and execute_type == 'find':
        find_content = options.find_content
        find_text_from_log_file(find_content)
    elif execute_type and execute_type == 'test':
        # mongo_client = get_test_mongo_client()
        calculate_list_page()