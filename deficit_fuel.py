import json
import aiohttp
import requests
import asyncio
import time
from datetime import datetime

from ets_data import token_get
from db2 import data_get
from db2 import data_post
from db2 import data_delete
import settings


data = []
data_cars = {}
tokens = {}


async def car_action(username, token):
    data_car = []
    url = 'https://ets.mos.ru/services/car_actual'
    headers = {'Authorization': token}
    car = requests.get(url, headers=headers)
    data_full = json.loads(car.text)
    data_result = data_full['result']['rows']
    for x in data_result:
        if x['waybill_closing_date'] is None:
            continue
        else:
            date1 = datetime.strptime(str(x['waybill_closing_date']), "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
            data_car.append((x['asuods_id'], x['gov_number'], date1, x['okrug_name'], x['company_name'], x['level_sensors_num'], x['gps_code']))

    data_cars[username] = data_car


async def get_waybill_data(username, token, session, asuods_id, level_sensors_num, gps_code, closing_date):
    url = 'https://ets.mos.ru/services/waybill?limit=1&sort_by=number:desc&filter={"car_id__in":["' + str(asuods_id) + '"],"status__in":["closed"],"closing_date__eq":"' + str(closing_date) + '"}'
    headers = {'Authorization': token}
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            wb = await resp.json()
            if wb['result'] and isinstance(wb['result'], list):
                waybill = wb['result'][0]
                waybill['level_sensors_num'] = level_sensors_num
                waybill['gps_code'] = gps_code
                return waybill
            else:
                print(f'Нет данных: {username, wb, url}')
    except TimeoutError:
        print(f'Таймаут {username, url}')


async def main():

    start = time.time()

    table_column = 'date_upload, okrug_name, company_name, gov_number, fact_fuel_end, sensor_finish_value, difference, status, date, level_sensors_num, gps_code, status_diff'
    users = await data_get('*', 'ets_users', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    for user in users:
        username = user[1]
        password = user[2]
        tokens[username] = await token_get(username, password)
        await car_action(username, tokens[username])

    async with aiohttp.ClientSession() as session:
        tasks = []
        for username in data_cars:
            for car in data_cars[username]:
                tasks.append(asyncio.ensure_future(get_waybill_data(username, tokens[username], session, car[0], car[5], car[6], car[2])))
        waybills_data = await asyncio.gather(*tasks)
        for x in waybills_data:
            if x:
                fuel_end = (0 if x['equipment_fact_fuel_end'] is None else x['equipment_fact_fuel_end']) + x['fact_fuel_end']
                sensor_value = 0 if x['sensor_finish_value'] is None else x['sensor_finish_value']
                result_fuel = round(fuel_end - sensor_value, 2)
                data.append((datetime.now().date().strftime('%Y-%m-%d'),
                             x['okrug_name'],
                             x['company_name'],
                             x['gov_number'],
                             fuel_end,
                             0 if x['sensor_finish_value'] is None else x['sensor_finish_value'],
                             result_fuel,
                             "ДУТ не работает" if x['sensor_finish_value'] == '0' else 'ДУТ работает',
                             x['closing_date'],
                             x['level_sensors_num'],
                             'Нет БНСО' if x['gps_code'] is None else x['gps_code'],
                             "Остаток" if result_fuel > 0 else "Норма"))

    params = 'where extract(day from (current_timestamp::timestamp without time zone - date_upload::timestamp without time zone)) > 7 OR extract(day from (current_timestamp::timestamp without time zone - date_upload::timestamp without time zone)) = 0'
    await data_delete('deficit_fuel', params, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    await data_post(data, 'deficit_fuel', table_column, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)

    print(time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

if __name__ == "__main__":
    asyncio.run(main())
