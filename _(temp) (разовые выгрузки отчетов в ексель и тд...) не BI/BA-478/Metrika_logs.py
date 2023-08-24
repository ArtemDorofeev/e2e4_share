# Импорт библиотек

import requests
import sys
import json
import datetime
import pandas as pd
import numpy as np
import time

############################## Параметры метрики #######################

# Токен метрики

#api_token = 'OAuth y0_AgAAAAAKbPwPAAoqfgAAAADnhGWcDf-pg2flQQmJupIgB4GqDCOjWUc'  # artmdor@yandex.ru (доступ)
api_token = 'OAuth y0_AgAAAABbXJbyAApKewAAAADpaIwj30VWaJcoSCSnK85-1dooLYMxOBc'  # e2e4marina@yandex.ru

# Номер счетчика
count = 1104419

##################################### Start logs ########################

# Формирование параметров запроса на логи

def param_logs(date_start, date_end):

    # HEADER запроса
    metrika_headers = {
    'GET': '/management/v1/counters HTTP/1.1',
    'Host': 'api-metrika.yandex.net',
    'Authorization': api_token,
    'Content-Type': 'application/x-yametrika+json'
    }

    # URL запроса
    metrika_url = 'https://api-metrika.yandex.net/management/v1/counter'

    # Список полей отчета
    fields_list = ['ym:s:visitID',
                   'ym:s:dateTimeUTC',
                   'ym:s:regionCity','ym:s:clientID',
                   'ym:s:goalsID',
                   'ym:s:goalsDateTime',
                   'ym:s:goalsOrder',
                   'ym:s:lastTrafficSource','ym:s:lastAdvEngine',                   
                   'ym:s:last_yandex_direct_clickDirectClickOrder',
                   'ym:s:last_yandex_direct_clickDirectClickOrderName',
                   'ym:s:openstatAd',
                   'ym:s:from',
                   'ym:s:UTMCampaign',
                   'ym:s:UTMContent',
                   'ym:s:UTMMedium',
                   'ym:s:UTMSource',
                   'ym:s:UTMTerm'
                  ]

    field_rus = ['ID визита',                 
                 'Дата и время визита',
                 'Город','ID пользователя',
                 'ID целей',
                 'Время достижения цели',
                 'ID заказов','Источник трафика',
                 'Рекламная система',
                 'Кампания ЯД',
                 'Название кампании ЯД',
                 'Openstat Ad',
                 'Метка from',
                 'UTM Campaign',
                 'UTM Content',
                 'UTM Medium',
                 'UTM Source',
                 'UTM Term'
                ]

    # Параметры для выгрузки
    payload = {
               'fields': fields_list,
               'date1': date_start,  # начальная дата
               'date2': date_end,  # конечная дата
               'source': 'visits'           
    }
    
    return metrika_headers, metrika_url, payload, count, field_rus

# Оценка возможности выгрузки данных

def possibles(metrika_headers, metrika_url, payload, count):
    
    possible_download = f'{metrika_url}/{count}/logrequests/evaluate'
    try:
        poss_resp = requests.get(possible_download, params=payload, headers=metrika_headers)
        possible_status = poss_resp.status_code
        possible = poss_resp.json()["log_request_evaluation"]["possible"]   
        return possible_status, possible
    except Exception as e:
        print(e)
        possible_status = 'Ошибка проверки выгрузки с сервера Яндекс'
        possible = False
        return possible_status, possible
    
# Запрос на создание логов (POST)

def create_log(metrika_headers, metrika_url, payload, count):
    
    create_logs = f'{metrika_url}/{count}/logrequests'
    try:
        create_resp = requests.post(create_logs, params=payload, headers=metrika_headers)
        create = json.loads(create_resp.text)
        create_status = create_resp.status_code
        request_id = create["log_request"]["request_id"]
        return create_status, request_id
    except Exception as e:
        print(e)
        create_status = 'Ошибка создания запроса на сервер Яндекс'
        request_id = 0
        return create_status, request_id

# Проверка статуса обработки лога

def check_status(metrika_headers, metrika_url, payload, count, request_id):

    status_logs = f'{metrika_url}/{count}/logrequest/{request_id}'
    try:
        for i in range(100):
            time.sleep(30)    
            check_resp = requests.get(status_logs, headers=metrika_headers)
            check = json.loads(check_resp.text)
            status_log = check["log_request"]["status"]
            if status_log == 'processed':
                part_num = len(check["log_request"]["parts"])            
                return status_log, part_num
                break
            elif status_log == 'created':
                continue
            else:
                part_num = 'Логи не готовы к выгрузке'
                return status_log, part_num
        part_num = 'Логи не готовы к выгрузке'
        return status_log, part_num
    except Exception as e:
        print(e)
        status_log = 'Ошибка определения статуса запроса на сервере Яндекс'
        part_num = ''
        return status_log, part_num

# Выгрузка логов с сервера

def log_download(metrika_headers, metrika_url, payload, count, request_id, part_num, field_rus):
    df = pd.DataFrame()
    try:
        for i in range(part_num):
            part = i
            download_logs = f'{metrika_url}/{count}/logrequest/{request_id}/part/{part}/download'
            download_resp = requests.get(download_logs, headers=metrika_headers)
            download_status = download_resp.status_code

            data = [x.split('\t') for x in download_resp.content.decode('utf-8').split('\n')[:-1]]
            df_ym = pd.DataFrame(data[1:], columns = field_rus)
            df = pd.concat([df, df_ym], ignore_index=True)
        return download_status, df
    except Exception as e:
        print(e)
        download_status = 'Ошибка загрузки логов с сервера Яндекс'
        df = ''
        return download_status, df
    
# Удаление логов с сервера

def log_clear(metrika_headers, metrika_url, payload, count, request_id):

    clear_logs = f'{metrika_url}/{count}/logrequest/{request_id}/clean'
    try:
        clear_resp = requests.post(clear_logs, headers=metrika_headers)
        clear = json.loads(clear_resp.text)
        clear_status = clear_resp.status_code
        clear_id = clear["log_request"]["request_id"]
        clear_action = clear["log_request"]["status"]    
        return clear_status, clear_action
    except Exception as e:
        print(e)
        clear_status = 'Ошибка очистки логов на сервере Яндекс'
        clear_action = ''
        return clear_status, clear_action
    


def make_logs(metrika_headers, metrika_url, payload, count, field_rus):
    print('1. Проверяем возможность создания логов на сервере Яндекса')
    possible_status, possible = possibles(metrika_headers, metrika_url, payload, count)
    
    if possible:
        print('2. Отправляем запрос на создание логов на сервер Яндекса')
        create_status, request_id = create_log(metrika_headers, metrika_url, payload, count)
        
        if create_status == 200:
            print(f'3. Проверяем статус запроса {request_id} на сервере Яндекса')
            status_log, part_num = check_status(metrika_headers, metrika_url, payload, count, request_id)
            
            if status_log == 'processed':
                print(f'4. Логи готовы ({part_num} партии), скачиваем их с сервера Яндекса')
                download_status, df = log_download(metrika_headers, metrika_url, payload, count, request_id, part_num, field_rus)
                
                if download_status == 200:
                    print('5. Логи скачены, удаляем их с сервера Яндекса')
                    clear_status, clear_action = log_clear(metrika_headers, metrika_url, payload, count, request_id)
                    
                    if clear_status == 200 and clear_action == 'cleaned_by_user':
                        print('6. Логи удалены с сервера Яндекса')
                        return df
                    else:
                        print(clear_status)
                
                else:
                    print(download_status)
            
            else:
                print(status_log)
        
        else:
            print(create_status)
    
    elif possible_status == 200:
        print('Логи с такими параметрами недоступны')
    else:
        print(possible_status)
        
####################################### End logs ######################################

##################################### Start costs ##############################

# Формирование параметров запроса на затраты

def param_costs(date_start, date_end, client_login):

    # HEADER запроса
    metrika_headers = {
    'GET': '/management/v1/counters HTTP/1.1',
    'Host': 'api-metrika.yandex.net',
    'Authorization': api_token,
    'Content-Type': 'application/x-yametrika+json'
    }

    # URL запроса
    metrika_url = 'https://api-metrika.yandex.net/stat/v1/data'
    
    # Параметры для выгрузки
    payload = {
            "ids":count,
            "metrics":"ym:ev:expenses<currency>",
            "currency":"RUB",
            "dimensions":"ym:ev:<attribution>ExpenseSource, ym:ev:<attribution>ExpenseCampaign",
            "direct_client_logins":client_login,
            "date1":date_start,
            "date2":date_end,            
            "accuracy":"full"         
    }
    
    return metrika_headers, metrika_url, payload

# Выгружаем отчеты для каждого клиента Я.Директ и объединяем данные в датафрейм

def report_costs(date_start, date_end, client_logins):
    df_costs = pd.DataFrame()
    dic = {'source': [], 'campaign_id': [], 'campaign_name': [], 'costs': []}
    for i in client_logins:
        metrika_headers, metrika_url, payload = param_costs(date_start, date_end, i)
        try:
            report_resp = requests.get(metrika_url, params=payload, headers=metrika_headers)
            download_status = report_resp.status_code
            report_content = json.loads(report_resp.text)
            data = report_content['data']
            if data:
                for j in range(len(data)):
                    source = data[j]['dimensions'][0]['name']
                    if source == 'Yandex.Direct':                        
                        campaign_id = data[j]['dimensions'][1]['direct_id'].split('-')[1]
                    else:
                        campaign_id = data[j]['dimensions'][1]['direct_id']
                    campaign_name = data[j]['dimensions'][1]['name']
                    costs = data[j]['metrics'][0]
                    dic['source'].append(source)
                    dic['campaign_id'].append(campaign_id)
                    dic['campaign_name'].append(campaign_name)
                    dic['costs'].append(costs)                       
            else:
                continue
        except Exception as e:
            print(e)
            download_status = f'Ошибка загрузки отчета с сервера Яндекс. Клиент {i}'
            df_costs = ''
            return download_status, df_costs
    df_costs = pd.DataFrame(dic)
    
    return download_status, df_costs

####################################### End costs ##################################