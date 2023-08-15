import asyncio
from datetime import datetime
from dateutil.relativedelta import *
import requests
import json
import aiohttp

async def token_get_user():
    url = 'https://ets.mos.ru/services/auth'
    payload = {'login': "Dustkulova_AN", 'password': "Dustkulova93!"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            data = await response.json()
            return 'Token ' + data['token']



async def waybill_list(token):
    data = []
    date = datetime.now() - relativedelta(days=7)
    url = f'https://ets.mos.ru/services/waybill?filter={{"date_create__gt":"{str(date.date())}","status__in":["closed"]}}'
    headers = {'Authorization': token}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data_full = await response.json()
            data1 = data_full['result']
            for x in data1:
                data.append((x['id'], x['fact_departure_date'], x['fact_arrival_date'], x['gov_number']))
            return data


async def estp_auto(waybill_info):
    headers = {
        'Cookie': 'PHPSESSID=6q6tfnpn3e30r4h7517p39e5it; _identity=399fd0229ea093a8612b572bfa4018f84215c5947855b801200f629725f02a64a%3A2%3A%7Bi%3A0%3Bs%3A9%3A%22_identity%22%3Bi%3A1%3Bs%3A48%3A%22%5B815%2C%22PfYWziVdX5aondZIFligsbNd_8m5Ju3H%22%2C2592000%5D%22%3B%7D; _csrf=7c7969fe7c1c1c60207440ee1d8b507a69012b9c85aab5ac7e4ea77e3ff9cbbfa%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22cBwpr5w1lahQXtbWdlLqn8xL7NYUjdh2%22%3B%7D',
        # 'X-Csrf-Token': '8fu_KUPo7PTdXRllON_0PaLuO3uEgF6cJXII9DycorW1jPpNJN6gmOgqLBZSpr4OxYdWCsnlKPRvMT2hRf3W2A=='
    }

    async with aiohttp.ClientSession() as session:
        for x in waybill_info:
            url = 'https://estp.dgkh.msk.ru/index.php?r=fuel/fuel-static-new&search=' + str(x[3]) + '&org=0&filter%5Buser%5D=&filter%5Bmax_count%5D=100&filter%5Bdate_to%5D='+ str(x[2]) + '&filter%5Bdate_from%5D=' + str(x[1])

            async with session.get(url, headers=headers) as response:
                data_full = await response.text()
                data_full = json.loads(data_full)
                data_result = data_full['result']
                for y in data_result:
                    fuel_up = 0 if y['fuel_up_l_val'] == '' else y['fuel_up_l_val']
                    parts = str(fuel_up).split(";")
                    if len(parts) >= 2:
                        num1 = int(0 if parts[0] == '' else parts[0])
                        num2 = int(0 if parts[1] == '' else parts[1])
                        result = num1 + num2
                    else:
                        result = parts[0]
                    print(x[3], x[1], x[2], result)





async def main():
    token = await token_get_user()
    waybill_info = await waybill_list(token)
    await estp_auto(waybill_info)

asyncio.run(main())
