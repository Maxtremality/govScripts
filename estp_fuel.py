import json
import requests
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta

url = 'https://estp.msk.ru/index.php?r=fuel/fuel-static-new'
data = {'search': '',
           'org': '255',
           'filter[user]': '',
           'filter[max_count]': '9999',
           'filter[date_from]': datetime.strftime(datetime.now() - relativedelta(days=1), '%d.%m.%Y %H:%M:%S'),
           'filter[date_to]': datetime.strftime(datetime.now(), '%d.%m.%Y %H:%M:%S')}
headers = {'x-csrf-token': '3ttykbEbCPGBwaNSOtPpbg_G_u0dh-D28Azw3ofA3Xnp7CWmiUQlp_eV1WFPmYNde4uMlFm01KOJRp6Vy5WkLA==',
           'Cookie':
               '_identity=7d0263df2d2cab7cce31b68a3e5d8c061cc564df65eefb9fcb8a3d16f87e8229a%3A2%3A%7Bi%3A0%3Bs%3A9%3A%22_identity%22%3Bi%3A1%3Bs%3A48%3A%22%5B338%2C%22Sqo2iASHeThaFXLZmel5VNMXUT27QHKq%22%2C2592000%5D%22%3B%7D; \
               _csrf=b8f801ea40520247bb2226b22499a4c6cef4a3ce3a57f1c107ba98e544f4c0fea%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%2277W78_-VvTv3uJj3tMryD34UyJnKLUyU%22%3B%7D',
           'Content-type': 'application/x-www-form-urlencoded'}
req_post = requests.post(url, data=data, headers=headers)
f = 1
for x in req_post.json()['result']:
    print(f, x)
    f += 1
