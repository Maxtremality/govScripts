import asyncio
import requests
import json
import time

from db import data_post
from db import data_truncate
import settings


async def auth_cafap():
    url = 'https://cafap.mos.ru/api/login'
    xsrf_get = requests.post(url)
    payload = {'user': 'egorov2',
               'password': 'P@rol1012',
               'rememberMe': 'false'}
    headers = {'x-xsrf-token': (xsrf_get.headers['Set-Cookie'].split(';')[0]).split('=')[1],
               'cookie': xsrf_get.headers['Set-Cookie'].split(';')[0]}
    req_post = requests.post(url, headers=headers, json=payload)

    return {'cookie': xsrf_get.headers['Set-Cookie'].split(';')[0] + '; ' + req_post.headers['Set-Cookie'].split(';')[0], 'x-xsrf-token': (xsrf_get.headers['Set-Cookie'].split(';')[0]).split('=')[1]}


async def get_entry_cafap(auth_data, processing_status):
    data = []
    url = 'https://cafap.mos.ru/api/issue/getByFilter?page=0&processingStatus=' + processing_status + '&sort=deadline,asc'
    headers = {'cookie': auth_data['cookie'],
               'x-xsrf-token': auth_data['x-xsrf-token']}
    req_get = requests.get(url, headers=headers)
    for entry in json.loads(req_get.text)['data']:
        data.append((entry['number'],
                     entry['object']['address'],
                     entry['type']['description']))

    await data_post(data, 'api_cafapmos', 'number, address, description', settings.DB_API, settings.DB_ACCESS)


async def main():
    start = time.time()

    await data_truncate('api_cafapmos', settings.DB_API, settings.DB_ACCESS)
    auth_data = await auth()
    await get_entry(auth_data, 'CONTRACTOR_WORK')
    await get_entry(auth_data, 'CONTRACTOR_ACTION')

    end = time.time()
    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == '__main__':
    asyncio.run(main())
