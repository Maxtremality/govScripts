import asyncio
import requests
import json
import time
from datetime import datetime
from datetime import timedelta

from db2 import data_post
from db2 import data_truncate
from ets_data import roads_get
from ets_data import token_get
import settings


async def auth_cafap():
    url = 'https://cafap.mos.ru/api/login'
    xsrf_get = requests.post(url)
    payload = {'user': 'egorov2',
               'password': 'Logitech001!',
               'rememberMe': 'false'}
    headers = {'x-xsrf-token': (xsrf_get.headers['Set-Cookie'].split(';')[0]).split('=')[1],
               'cookie': xsrf_get.headers['Set-Cookie'].split(';')[0]}
    req_post = requests.post(url, headers=headers, json=payload)

    return {'cookie': xsrf_get.headers['Set-Cookie'].split(';')[0] + '; ' + req_post.headers['Set-Cookie'].split(';')[0], 'x-xsrf-token': (xsrf_get.headers['Set-Cookie'].split(';')[0]).split('=')[1]}


async def get_entry_er():
    date = (datetime.now() - timedelta(days=365)).strftime('%d.%m.%Y')
    table_name = 'api_ermos'
    await data_truncate(table_name, settings.DB_API, 'public', settings.DB_ACCESS)
    roads = await roads_get(await token_get(settings.TRAVEL_TIME_ACCESS['avdVAO']['login'], settings.TRAVEL_TIME_ACCESS['avdVAO']['password']))

    url = 'https://gorod.mos.ru/api/service/report/direct/report-page'
    headers = {
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI4MzljYTQ3MjY5MWE5YzQxNzJkMGEyY2M3YjI5ODY5NCIsImp0aSI6IjMzNGQ2YzQ4MWViNTgzOWEyNTMxYjFmZWE2OWUwNTMwOWRmYjEwYTQ5MzMxYzFlYjIzN2IxYzVkZWFlMTcwMjkyYTMxYjJiZDA5OGMyYzRiIiwiaWF0IjoxNjc5Mjk4MjkzLjQzMTg0NiwibmJmIjoxNjc5Mjk4MjkzLjQzMTg1LCJleHAiOjE2NzkzMjcwOTMuNDI2NDUsInN1YiI6IjIwMDAwMzkyOjoxdjNCcnRCNS8vUHdneVFKUkpWdWgwK0VSK1FJekpUU3Q3TVlaZjNMT2dtZU43bkJQV2pBTlRkUCIsInNjb3BlcyI6WyJvcGVuaWQiXX0.RTCjlEhGQiOWd1S0pGn9uJXVZyuUEuqNh22ZD7YtDAMLZp5eWLnGh_P_Du6pa8KxG6aQ8-6tkWymj-m2oWJbUe-9Mqgwmz5ij14diRWGgemvWSenT-aF0i1pnDfcTosvcL229g00XOA9IZWh72rZ5mg34THmxdelXusdgFBn8oxyhMXaPCAFnbd2O8wbeC-tde6bvwAH9WlWntW-vpoRAdNv8wPub7TcAQrdxKIt9USZscujc7ckinqxZVwim_pKEKqDZoiiqafyy5gQUJtC6n6qisnloK6lE1TEz-7CweXX3pkcDRfFk86STrhcy1Yg8_IZWOHfFRvl6-WNHqPvRw',
        'x-jwt-token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJnb3JvZDIiLCJpc3MiOiJhdXRoLXNlcnZpY2UiLCJqdGkiOiI0NGQ1MGNjNS0xODI1LTQxZWQtODAwZi05MzJmMmM2ZGJkOGQiLCJpYXQiOjE2NzkyOTgyOTMuNjMyNzMsImV4cCI6MTY3OTMwMTg5My42MzI3MywidXNlcl9pZCI6MjAwMDAzOTIsImF1dGgiOnRydWV9.sGsU0F_C80otGzTupkhwYWI9eaxY4VFffaNQ5iL1H7dpmeIw__g3_P6we_tWcxrRIpJrkN_mQFPP-qf4YiIxdeQqnDGzw6oSbv08pIoPqPAD4NsOLHuFjq4SYehHyhr3I2k9FnROXDkHhJSITWcZJDR98EyxIiS-dWwuQ-L7aBWNYRdTGzzINpm-25XFnQq5iKlYwVZOCm147kX7MGQX915pVx59_pt49JTR1mto6TdrAq32HroY-hSOPYFgwO5Dk2fhRFRofSEJbIHLJ08mdNLnDnzDI0Sf9a4tR1H7XJ-OBWbCiZBiVA4NHxMQS--mXboubx5RcBVNLXtI5NeH6A'}
    payload = {
        "filter": {'c_comment_publish_date_beg': date},
        "pages": {"page": 1, "perPage": 9999},
        "sort": {"by": "id", "order": "desc"},
        "report_id": 61
    }
    req_post = requests.post(url, json=payload, headers=headers)
    req_data = json.loads(req_post.text)
    for comment in req_data['data']['list']:
        data = []
        photo = comment['c_comment_photo'].split(';')

        table_column = 'source, comment_id, ref_issue_id, comment_type, comment_publish_date, deadline_at_comment, ' \
                       'answer_status, problem_status, status, theme, last_organization_name, region, ' \
                       'company_structures, object_name, object_name_ets, address, latitude, longitude, ' \
                       'object_category, monitor_category, monitor_critical, monitor_violation_type, ' \
                       'ord1, ord2, ord3, c_user_alias'
        for p in range(1, len(photo) + 1):
            table_column += ', ph' + str(p)

        status = next((x['c_answer_status'] for x in req_data['data']['list']
                       if comment['c_ref_issue_id'] == x['c_ref_issue_id']
                       and x['c_problem_status'] is None
                       and (x['c_answer_status'] != 'На модерации'
                            or x['c_answer_status'] != 'На утверждении')),
                      comment['c_problem_status'])
        company_structures = next((x['company_structures_text'] for x in roads['result']['rows'] if x['name'] == comment['i_object_name']), '')
        data.append(('ER',
                     '' if comment['c_comment_id'] is None else comment['c_comment_id'],
                     '' if comment['c_ref_issue_id'] is None else comment['c_ref_issue_id'],
                     '' if comment['c_comment_type'] is None else comment['c_comment_type'],
                     '' if datetime.strptime(comment['c_comment_publish_date'], '%d.%m.%Y %H:%M:%S').isoformat() is None else datetime.strptime(comment['c_comment_publish_date'], '%d.%m.%Y %H:%M:%S').isoformat(),
                     '' if datetime.strptime(comment['c_deadline_at_comment'], '%d.%m.%Y %H:%M:%S').isoformat() is None else datetime.strptime(comment['c_deadline_at_comment'], '%d.%m.%Y %H:%M:%S').isoformat(),
                     '' if comment['c_answer_status'] is None else comment['c_answer_status'],
                     '' if comment['c_problem_status'] is None else comment['c_problem_status'],
                     status,
                     '' if comment['i_theme'] is None else comment['i_theme'],
                     'Не определено' if comment['a_last_organization_name'] is None else comment['a_last_organization_name'],
                     'Не определено' if comment['i_region'] is None else comment['i_region'],
                     company_structures,
                     comment['i_address'] if comment['i_object_name'] is None else comment['i_object_name'],
                     'Не найдено по полигонам',
                     '' if comment['i_address'] is None else comment['i_address'],
                     '' if comment['i_latitude'] is None else comment['i_latitude'],
                     '' if comment['i_longitude'] is None else comment['i_longitude'],
                     '' if comment['i_object_category'] is None else comment['i_object_category'],
                     'Не определено' if comment['m_monitor_category'] is None else comment['m_monitor_category'],
                     '' if comment['m_monitor_critical'] is None else comment['m_monitor_critical'],
                     '' if comment['m_monitor_violation_type'] is None else comment['m_monitor_violation_type'],
                     '' if comment['ord1'] is None else comment['ord1'],
                     '' if comment['ord2'] is None else comment['ord2'],
                     '' if comment['ord3'] is None else comment['ord3'],
                     '' if comment['c_user_alias'] is None else comment['c_user_alias'],
                     *photo))
        if data:
            await data_post(data, table_name, table_column, settings.DB_API, 'public', settings.DB_ACCESS)


async def get_entry_cafap(auth_data, processing_status, status):
    data = []
    url = 'https://cafap.mos.ru/api/issue/getByFilter?page=1&processingStatus=' + processing_status + '&sort=deadline,asc'
    headers = {
        'cookie': auth_data['cookie'],
        'x-xsrf-token': auth_data['x-xsrf-token'],
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_get_page = requests.get(url, headers=headers)
    if json.loads(req_get_page.text)['paging']['totalPages'] > 1:
        for page in range(json.loads(req_get_page.text)['paging']['totalPages']):
            url = 'https://cafap.mos.ru/api/issue/getByFilter?page=' + str(page) + '&processingStatus=' + processing_status + '&sort=deadline,asc'
            req_get = requests.get(url, headers=headers)
            for entry in json.loads(req_get.text)['data']:
                data.append(('CAFAP',
                             entry['number'],
                             entry['number'],
                             'ЦАФАП',
                             'ЦАФАП',
                             entry['object']['address'],
                             entry['type']['description']))
    else:
        url = 'https://cafap.mos.ru/api/issue/getByFilter?page=0&processingStatus=' + processing_status + '&sort=deadline,asc'
        req_get = requests.get(url, headers=headers)
        for entry in json.loads(req_get.text)['data']:
            data.append(('CAFAP',
                         entry['number'],
                         entry['number'],
                         'ЦАФАП',
                         'ЦАФАП',
                         entry['object']['address'],
                         entry['type']['description']))
    if data:
        await data_post(data, 'api_ermos', 'source, comment_id, ref_issue_id, answer_status, status, object_name, theme', settings.DB_API, 'public', settings.DB_ACCESS)


async def main():
    start = time.time()

    auth_data = await auth_cafap()
    await get_entry_er()
    await get_entry_cafap(auth_data, 'CONTRACTOR_WORK', 'В работе')
    await get_entry_cafap(auth_data, 'CONTRACTOR_ACTION', 'Принятие мер')

    end = time.time()
    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == '__main__':
    asyncio.run(main())
