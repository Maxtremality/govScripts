import asyncio
import websockets
import json
from datetime import datetime
import time

from ets_data import wss_token_get
from ets_data import token_get
from ets_data import car_actual_get
from db2 import data_get
from db2 import data_delete
from db2 import data_post
import settings

wss_data = {}
checkout_table = 'checkout4'


async def wss(token):
    async with websockets.connect(settings.CHECKOUT_WSS_URL + token) as websocket:
        async for message in websocket:
            wss_data.update(json.loads(message))


async def data_remove(table_name: str):
    params = 'where extract(day from (current_timestamp::timestamp without time zone - date_upload::timestamp without time zone)) > 7 OR extract(day from (current_timestamp::timestamp without time zone - date_upload::timestamp without time zone)) = 0'
    await data_delete(table_name, params, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', 'Лишние данные из', table_name, 'удалены')


async def checkout(cars, okrug, last_status_data, last_time_status_change_data):
    """
            0 - статус для первой загрузки
            1 - в движении
            2 - остановка
            3 - стоянка
            4 - не на связи
            10 - тс на базе
            11 - в стоянке более 15 минут
            12 - в стоянке на гидранте
    """

    def last(key):
        last_status = next((x[1] for x in last_status_data if x[0] == int(key)), 0)
        last_time_status_change = next((x[1] for x in last_time_status_change_data if x[0] == int(key)), 0)

        return [last_status, last_time_status_change]

    final_dict = {}
    data = []
    base_ev = []
    time_status_change = {}

    for key in wss_data:
        final_dict.update(wss_data[key])

        last_data = last(key)

        last_status = last_data[0]
        last_time_status_change = last_data[1]
        if (last_status == 3 or last_status == 11) and (final_dict['status'] == 3 or final_dict['status'] == 11):
            if last_time_status_change is None:
                time_status_change[final_dict['id']] = final_dict['timestamp']
            elif last_time_status_change == 0:
                time_status_change[final_dict['id']] = final_dict['timestamp']
            else:
                time_status_change[final_dict['id']] = last_time_status_change
        else:
            time_status_change[final_dict['id']] = final_dict['timestamp']

        base_event = None

        for base in settings.BASE_COORDS[next((car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)][next((car['company_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)]:
            if base[0] - 80 < final_dict['coords_msk'][0] < base[0] + 80 and base[1] - 80 < final_dict['coords_msk'][1] < base[1] + 80 and final_dict['status'] != 4:
                final_dict['status'] = 10
        for hydrant in settings.HYDRANT_COORDS[next((car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)][next((car['company_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)]:
            if hydrant[0] - 80 < final_dict['coords_msk'][0] < hydrant[0] + 80 and hydrant[1] - 80 < final_dict['coords_msk'][1] < hydrant[1] + 80 and wss_data[key]['status'] == 3:
                final_dict['status'] = 12
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
        elif final_dict['status'] == 12:
            final_dict['status_text'] = 'В стоянке на гидранте'
        if last_status == 10 and final_dict['status'] != 10 and final_dict['status'] != 4:
            base_event = 'Выезд с базы'
        if last_status != 10 and final_dict['status'] == 10 and final_dict['status'] != 4:
            base_event = 'Возвращение на базу'
        if next((car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0) is None:
            okrug_id = next((car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)
        else:
            okrug_id = next((car['okrug_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)
        company_id = next((car['company_id'] for car in cars['result']['rows'] if car['gps_code'] == key), 0)
        gov_number = next(car['gov_number'] for car in cars['result']['rows'] if car['gps_code'] == key)
        condition_text = next(car['condition_text'] for car in cars['result']['rows'] if car['gps_code'] == key)
        type_name = next(car['type_name'] for car in cars['result']['rows'] if car['gps_code'] == key)
        data.append((okrug, okrug_id, company_id, gov_number, condition_text, type_name,
                     final_dict['id'], final_dict['speed'], final_dict['speed_max'], final_dict['distance'],
                     final_dict['status'], wss_data[key]['status'], final_dict['status_text'], time_status_change[final_dict['id']],
                     final_dict['coords'][0], final_dict['coords'][1]))
        if base_event is not None:
            base_ev.append((gov_number, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), base_event))
    if len(base_ev) > 0 and okrug == 'VAO':
        # await data_remove('base_event')
        await data_post(base_ev, 'base_event', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    await data_post(data, checkout_table, '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    print("Data uploaded", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))


async def main():
    start = time.time()

    for okrug in settings.CHECKOUT_ACCESS:
        last_status_data = await data_get('gps_code, status', checkout_table, f"WHERE okrug = '{okrug}'", settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
        last_time_status_change_data = await data_get('gps_code, time_status_change', checkout_table, f"WHERE okrug = '{okrug}'", settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
        await data_delete(checkout_table, f"WHERE okrug = '{okrug}'", settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
        wss_data.clear()
        login = settings.CHECKOUT_ACCESS[okrug]['login']
        password = settings.CHECKOUT_ACCESS[okrug]['password']
        token = await token_get(login, password)
        cars = await car_actual_get(token, '')
        wss_task = asyncio.create_task(wss(await wss_token_get(login, password)))
        await asyncio.sleep(5)
        wss_task.cancel()
        await checkout(cars, okrug, last_status_data, last_time_status_change_data)

    end = time.time()
    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
