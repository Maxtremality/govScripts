import asyncio
import websockets
import json
from datetime import datetime
import time

from ets_data import wss_token_get
from ets_data import token_get
from ets_data import car_actual_get
from db import data_get
from db import data_truncate
from db import data_post
import settings

checkout_table = 'checkout4'
wss_data = {}


async def wss(token):
    async with websockets.connect(settings.CHECKOUT_WSS_URL + token) as websocket:
        async for message in websocket:
            wss_data.update(json.loads(message))


async def checkout(cars):
    """
            0 - статус для первой загрузки
            1 - в движении
            2 - остановка
            3 - стоянка
            4 - не на связи
            10 - тс на базе
            11 - в стоянке более 15 минут
    """
    final_dict = {}
    data = []
    time_status_change = {}

    last_status = await data_get('status', checkout_table, '', settings.DB_USERS, settings.DB_ACCESS)
    last_time_status_change = await data_get('time_status_change', checkout_table, '', settings.DB_USERS, settings.DB_ACCESS)
    for key in wss_data:
        final_dict.update(wss_data[key])

        if len(last_status) == 0:
            last_status = ((final_dict['status'],),)
        if (last_status[0][0] == 3 or last_status[0][0] == 11) and (final_dict['status'] == 3 or final_dict['status'] == 11):
            if last_time_status_change[0] is None:
                time_status_change[final_dict['id']] = final_dict['timestamp']
            elif last_time_status_change[0][0] == 0:
                time_status_change[final_dict['id']] = final_dict['timestamp']
            else:
                time_status_change[final_dict['id']] = last_time_status_change[0][0]
        else:
            time_status_change[final_dict['id']] = final_dict['timestamp']

    await data_truncate(checkout_table, settings.DB_USERS, settings.DB_ACCESS)
    for key in wss_data:

        final_dict.update(wss_data[key])

        for base in settings.BASE_COORDS[99 if next(car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key) is None else next(car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key)][next(car['company_id'] for car in cars['result']['rows'] if car['gps_code'] == key)]:
            if base[0] - 80 < final_dict['coords_msk'][0] < base[0] + 80 and base[1] - 80 < final_dict['coords_msk'][1] < base[1] + 80:
                final_dict['status'] = 10
        if final_dict['timestamp'] - time_status_change[final_dict['id']] > 900 and final_dict['timestamp'] - time_status_change[final_dict['id']] != final_dict['timestamp']:  # 900 сек - 15 минут
            final_dict['status'] = 11
        if final_dict['status'] == 1:
            final_dict['status_text'] = 'В движении'
        elif final_dict['status'] == 2:
            final_dict['status_text'] = 'Остановка'
        elif final_dict['status'] == 3:
            final_dict['status_text'] = 'Стоянка'
        elif final_dict['status'] == 4:
            final_dict['status_text'] = 'Не на связи'
        elif final_dict['status'] == 10:
            final_dict['status_text'] = 'ТС на базе'
        elif final_dict['status'] == 11:
            final_dict['status_text'] = 'В стоянке более 15 минут'
        if next(car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key) is None:
            okrug_id = next(car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key)
        else:
            okrug_id = next(car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key)
        company_id = next(car['company_id'] for car in cars['result']['rows'] if car['gps_code'] == key)
        gov_number = next(car['gov_number'] for car in cars['result']['rows'] if car['gps_code'] == key)
        condition_text = next(car['condition_text'] for car in cars['result']['rows'] if car['gps_code'] == key)
        type_name = next(car['type_name'] for car in cars['result']['rows'] if car['gps_code'] == key)
        data.append((okrug_id, company_id, gov_number, condition_text, type_name,
                     final_dict['id'], final_dict['speed'], final_dict['speed_max'], final_dict['distance'],
                     final_dict['status'], final_dict['status_text'], time_status_change[final_dict['id']],
                     final_dict['coords'][0], final_dict['coords'][1]))
    await data_post(data, checkout_table, '', settings.DB_USERS, settings.DB_ACCESS)
    print("Data uploaded", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))


async def main():
    start = time.time()

    login = settings.CHECKOUT_ACCESS['SAO']['login']
    password = settings.CHECKOUT_ACCESS['SAO']['password']
    token = await token_get(login, password)
    cars = await car_actual_get(token, '')
    wss_task = asyncio.create_task(wss(await wss_token_get(login, password)))
    await asyncio.sleep(3)
    wss_task.cancel()
    await checkout(cars)

    end = time.time()
    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
