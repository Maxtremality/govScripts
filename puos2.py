import aiohttp
import asyncio
import time

import settings
from db2 import data_post
from db2 import data_truncate


async def token_get_user():
    url = 'https://puos.mos.ru/auth/v1/tokens/access'
    payload = {'user': "IsaevON", 'password': "eTx987654321ZQ!"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            data = await response.json()
            return 'Bearer ' + data['accessToken']


async def puos(token):
    result = []
    tickets_type = {
        "manual": "Электронный талон сформирован ручным разбором",
        "auto": "Автоматически сформированный электронный талон",
        "auto2": "Автоматически сформированная квитанция",
        "auto3": "Автоматически сформированный электронный талон с корректировкой объёма",
        "auto5": "Автоматический электронный талон, сформированный по результатам проведения корректировки",
        "correction": "Корректировочный электронный талон",
    }
    url = f'https://puos.mos.ru/main/v1/tickets?page=0&size=99999999&sortBy=piip_create_date&sortDir=DESC&season=3&start_date=1667250000&end_date=1685566799&cargoSendCode=120250'
    headers = {'Authorization': token,
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data_full = await response.json()
            for item in data_full['items']:
                result.append((
                    item['piip_create_date'],
                    tickets_type[item['ticket_type']] if item['ticket_type'] in tickets_type else 'Новый статус',
                    item['ticket_number'],
                    item['card_number'].rsplit(',')[-1],
                    item['grz'],
                    0 if item['carrier'] is None else item['carrier'],
                    round(item['total_volume'], 2),
                    item['ssp_name_in']
                ))

    return result


async def main():
    start = time.time()

    token = await token_get_user()
    data1 = await puos(token)
    await data_truncate('puos', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    await data_post(
        data1,
        'puos',
        'piip_create_date, ticket_type, ticket_number, card_number, gov_number, carrier, cargo_diff, ssp_name',
        settings.DB_SCRIPTS,
        'scripts',
        settings.DB_ACCESS
    )

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == '__main__':
    asyncio.run(main())

