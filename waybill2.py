import asyncio
import aiohttp
import time
import random
from datetime import datetime
from dateutil.relativedelta import *
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from ets_data import token_get
from db2 import data_get
from db2 import data_post
from db2 import data_truncate
import settings


data = {}


def data_processing(username, waybills, cars, average_consumption_data, track_data):
    result = []
    for x in waybills['result']:
        for i in cars['result']['rows']:
            if str(i['gov_number']) == str(x['gov_number']):
                for k in average_consumption_data:
                    if i['special_model_name'] == k[1] and i['full_model_name'] == k[2] and i['type_name'] == k[3]:
                        i['normal_average_consumption'] = float(k[4])
                        break
                    else:
                        i['normal_average_consumption'] = 0

                if x['track_length_km'] is not None and x['track_length_km'] != 0 and x['sensor_consumption'] is not None and x['sensor_consumption'] != 0:
                    i['sensor_average_consumption'] = round(x['sensor_consumption'] / x['track_length_km'] * 100, 2)
                else:
                    i['sensor_average_consumption'] = 0

                if i['sensor_average_consumption'] > i['normal_average_consumption'] and i['normal_average_consumption'] != 0:
                    i['status_average_consumption'] = 'Перерасход'
                    i['difference_consumption'] = round((i['normal_average_consumption'] / 100 * x['track_length_km']) - x['sensor_consumption'], 2)
                else:
                    i['status_average_consumption'] = 'Норма'
                    i['difference_consumption'] = 0
                car = i
                sensors = car['level_sensors_num']
                gps_code = 'Нет БНСО' if car['gps_code'] is None else 'Есть БНСО'

                odometr_diff = (0 if x['odometr_end'] is None else x['odometr_end']) - (0 if x['odometr_start'] is None else x['odometr_start'])  #Пробег по ПЛ
                motohours_diff = (0 if x['motohours_end'] is None else x['motohours_end']) - (0 if x['motohours_start'] is None else x['motohours_start'])  #Моточасы по ПЛ
                motohours_equip_diff = (0 if x['motohours_equip_end'] is None else x['motohours_equip_end']) - (0 if x['motohours_equip_start'] is None else x['motohours_equip_start']) #Моточасы по ПЛ оборудование
                fuel_givens = (0 if x['equipment_fuel_given'] is None else x['equipment_fuel_given']) + (0 if x['fuel_given'] is None else x['fuel_given']) #Выдано литры общее
                mileage_diff = (odometr_diff - (0 if x['track_length_km'] is None else x['track_length_km'])) if odometr_diff > 0 and x['track_length_km'] == 0 else round((odometr_diff - ((0 if x['track_length_km'] is None else x['track_length_km']) / 100 * 110)), 3) #Разница пробегов ПЛ-ГЛОНАСС( с 10% погрешности)

                try:
                    mileage_diff_percent = round((100 - (((0 if x['track_length_km'] is None else x['track_length_km']) * 0.1 + (0 if x['track_length_km'] is None else x['track_length_km'])) / odometr_diff) * 100), 2)
                except ZeroDivisionError:
                    mileage_diff_percent = "0"      # Разница пробега ПЛ-ГЛОНАСС проценты

                mileage_diff_status = "Перепробег" if float(mileage_diff_percent) > 0 else "Норма"      # Статус разницы пробегов ПЛ-ГЛОНАСС
                motohours_or_km_status = "Моточасы" if (0 if x['motohours_start'] is None else x['motohours_start']) > 0 else "Километры"   # Статус моточасы или километры
                refill_diff = (fuel_givens - (0 if x['sensor_refill'] is None else x['sensor_refill'])) if (0 if x['sensor_refill'] is None else x['sensor_refill']) == 0 else round(((fuel_givens / 100) * 93 - (0 if x['sensor_refill'] is None else x['sensor_refill'])), 2) # Разница заправки с учетом 7% литры

                try:
                    refill_diff_percent = round(((fuel_givens - (0 if x['sensor_refill'] is None else x['sensor_refill'])) / fuel_givens * 100),)
                except ZeroDivisionError:
                    refill_diff_percent = "0"


                refill_diff_status = "Недолив" if float(refill_diff_percent) > 7 else "Норма"
                odometr_motohours_status = "Требует внимание" if motohours_diff > 0 and (0 if x['track_length_km'] is None else x['track_length_km']) == 0 else "Норма"
                sum_consumption = (0 if x['fact_consumption'] is None else x['fact_consumption']) + (0 if x['equipment_fact_consumption'] is None else x['equipment_fact_consumption'])
                diff_sum_consumption = round((sum_consumption / 100 * 93 - (0 if x['sensor_consumption'] is None else x['sensor_consumption'])), 2)
                diff_sum_consumption_status = "Требует внимания" if float(diff_sum_consumption) > 0 else "Норма"
                diff_fact_tax = round((((0 if x['fact_consumption'] is None else x['fact_consumption']) + (0 if x['equipment_fact_consumption'] is None else x['equipment_fact_consumption'])) - ((0 if x['tax_consumption'] is None else x['tax_consumption']) + (0 if x['equipment_tax_consumption'] is None else x['equipment_tax_consumption']))), 2)
                diff_fact_tax_status = "Требует внимания" if diff_fact_tax > 0 else "Норма"

                result.append((0 if x['okrug_name'] is None else x['okrug_name'],
                             0 if x['company_name'] is None else x['company_name'],
                             0 if x['status_text'] is None else x['status_text'],
                             0 if x['number'] is None else x['number'],
                             0 if x['activating_date'] is None else x['activating_date'],
                             datetime(1970, 1, 2, 1, 0, 0).isoformat() if x['closing_date'] is None else x['closing_date'],
                             0 if x['driver_name'] is None else x['driver_name'],
                             0 if x['gov_number'] is None else x['gov_number'],
                             0 if x['fact_departure_date'] is None else x['fact_departure_date'],
                             0 if x['fact_arrival_date'] is None else x['fact_arrival_date'],
                             0 if x['closed_by_employee_name'] is None else x['closed_by_employee_name'],
                             0 if x['odometr_start'] is None else x['odometr_start'],
                             0 if x['odometr_end'] is None else x['odometr_end'],
                             odometr_diff,
                             mileage_diff,
                             mileage_diff_percent,
                             mileage_diff_status,
                             motohours_or_km_status,
                             odometr_motohours_status,
                             0 if x['motohours_start'] is None else x['motohours_start'],
                             0 if x['motohours_end'] is None else x['motohours_end'],
                             motohours_diff,
                             0 if x['motohours_equip_start'] is None else x['motohours_equip_start'],
                             0 if x['motohours_equip_end'] is None else x['motohours_equip_end'],
                             motohours_equip_diff,
                             0 if x['fuel_type'] is None else x['fuel_type'],
                             0 if x['fuel_given'] is None else x['fuel_given'],
                             0 if x['fuel_start'] is None else x['fuel_start'],
                             0 if x['fuel_end'] is None else x['fuel_end'],
                             0 if x['equipment_fuel_type'] is None else x['equipment_fuel_type'],
                             0 if x['equipment_fuel_given'] is None else x['equipment_fuel_given'],
                             0 if x['equipment_fuel_start'] is None else x['equipment_fuel_start'],
                             0 if x['equipment_fuel_end'] is None else x['equipment_fuel_end'],
                             0 if x['tax_consumption'] is None else x['tax_consumption'],
                             0 if x['fact_consumption'] is None else x['fact_consumption'],
                             0 if x['equipment_tax_consumption'] is None else x['equipment_tax_consumption'],
                             0 if x['equipment_fact_consumption'] is None else x['equipment_fact_consumption'],
                             0 if x['equipment_diff_consumption'] is None else x['equipment_diff_consumption'],
                             diff_sum_consumption,
                             diff_sum_consumption_status,
                             fuel_givens,
                             0 if x['fuel_card_id'] is None else x['fuel_card_id'],
                             0 if x['equipment_fuel_card_id'] is None else x['equipment_fuel_card_id'],
                             0 if x['track_length_km'] is None else x['track_length_km'],
                             0 if x['sensor_start_value'] is None else x['sensor_start_value'],
                             0 if x['sensor_finish_value'] is None else x['sensor_finish_value'],
                             0 if x['sensor_consumption'] is None else x['sensor_consumption'],
                             0 if x['sensor_refill'] is None else x['sensor_refill'], # Заправка по ДУТ
                             refill_diff,           # Разница заправки с учетом 7% литры
                             refill_diff_status,    # Статус заправки недолив или норма.
                             refill_diff_percent,   # Процент заправки.
                             diff_fact_tax,
                             diff_fact_tax_status,
                             0 if x['sensor_leak'] is None else x['sensor_leak'],
                             0 if x['structure_name'] is None else x['structure_name'],
                             0 if x['comment'] is None else x['comment'],
                             sensors,
                             gps_code,
                             car['normal_average_consumption'],
                             car['sensor_average_consumption'],
                             car['difference_consumption'],
                             car['status_average_consumption'],
                             car['type_name'],
                             car['special_model_name'],
                             car['full_model_name'],
                             "ГЛОНАСС исправен" if x['is_bnso_broken'] is False else ("ГЛОНАСС неисправен" if x['is_bnso_broken'] is True else "ГЛОНАСС не установлен"),
                             track_data[x['id']]['travel_time'] if x['id'] in track_data else 0,
                             track_data[x['id']]['time_of_parking'] if x['id'] in track_data else 0))
                break

    print(f'{username} обработка данных завершена')
    data[username] = result


async def get_cars_data(session, username, token):
    url = 'https://ets.mos.ru/services/car_actual'
    headers = {'Authorization': token}
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            req = await resp.json()
            if resp.status == 500:
                req = await get_cars_data(session, username, token)
                return req
            print(f'{username} реестр техники загружен')
            return {username: req}
    except TimeoutError:
        print(f'Таймаут {username}')


async def get_waybills_data(session, username, token, date):
    url = 'https://ets.mos.ru/services/waybill?filter={"date_create__gt":"' + str(date.date()) + '","status__in":["closed"]}'
    headers = {'Authorization': token}
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            req = await resp.json()
            if resp.status == 500:
                req = await get_waybills_data(session, username, token, date)
                return req
            print(f'{username} путевые загружены')
            return {username: req}
    except TimeoutError:
        print(f'Таймаут {username}')


async def get_track_data(session, token, car, period):
    url = f'https://psd.mos.ru/tracks-caching/tracks?version=4&car_id={car["car_id"]}&from_dt={period["from_dt"]}&to_dt={period["to_dt"]}&sensors=1'
    headers = {'Authorization': token}
    time.sleep(random.randint(1, 60))
    try:
        async with session.get(url, headers=headers, timeout=3000) as resp:
            track = await resp.json()
            if len(track['track']) < 3:
                print(f'Нет трека {car["okrug_name"]} {car["company_name"]} {car["gov_number"]} {car["number"]}')
                return {car['id']: {'travel_time': 0, 'time_of_parking': 0}}
            else:
                return {car['id']: {'travel_time': track['travel_time'], 'time_of_parking': track['time_of_parking']}}
    except TimeoutError:
        print(f'Отказ сервера {car["okrug_name"]} {car["company_name"]} {car["gov_number"]} {car["number"]}')
        return await get_track_data(session, token, car, period)


async def main():
    start = time.time()

    waybills = {}
    cars = {}
    tokens = {}
    track_data = {}
    
    waybill_columns = 'okrug_name, company_name, status_text, number, activating_date, closing_date, driver_name, \
                      gov_number, fact_departure_date, fact_arrival_date, closed_by_employee_name, odometr_start, \
                      odometr_end, odometr_diff, mileage_diff, mileage_diff_percent, mileage_diff_status, \
                      motohours_or_km_status, odometr_motohours_status, motohours_start, motohours_end, motohours_diff, motohours_equip_start, \
                      motohours_equip_end, motohours_equip_diff, fuel_type, fuel_given, fuel_start, fuel_end, \
                      equipment_fuel_type, equipment_fuel_given, equipment_fuel_start, equipment_fuel_end, \
                      tax_consumption, fact_consumption, equipment_tax_consumption, \
                      equipment_fact_consumption, equipment_diff_consumption, diff_sum_consumption, diff_sum_consumption_status, fuel_givens, fuel_card_id, \
                      equipment_fuel_card_id, track_length_km, sensor_start_value, sensor_finish_value, \
                      sensor_consumption, sensor_refill, refill_diff, refill_diff_status, refill_diff_percent, \
                      diff_fact_tax, diff_fact_tax_status, sensor_leak, \
                      structure_name, comment, sensors, gps_code, \
                      normal_average_consumption, sensor_average_consumption, difference_consumption, status_average_consumption, ' \
                      'type_name, special_model_name, full_model_name, glonass_status, travel_time, time_of_parking'

    date = datetime.now() - relativedelta(days=3)
    users = await data_get('*', 'ets_users', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    average_consumption_data = await data_get('*', 'average_consumption', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)

    for user in users:
        username = user[1]
        password = user[2]
        tokens[username] = await token_get(username, password)

    async with aiohttp.ClientSession() as session:
        tasks_waybills = []
        tasks_cars = []
        for username in tokens:
            tasks_cars.append(asyncio.ensure_future(get_cars_data(session, username, tokens[username])))
            tasks_waybills.append(asyncio.ensure_future(get_waybills_data(session, username, tokens[username], date)))
        waybills_data = await asyncio.gather(*tasks_waybills)
        cars_data = await asyncio.gather(*tasks_cars)

    for dictionary in waybills_data:
        waybills.update(dictionary)
    for dictionary in cars_data:
        cars.update(dictionary)

    async with aiohttp.ClientSession(trust_env=True) as session:
        tasks_track = []
        for username in waybills:
            for waybill in waybills[username]['result']:
                period = {
                    'from_dt': int(datetime.fromisoformat(waybill['activating_date']).timestamp()),
                    'to_dt': int(datetime.fromisoformat(waybill['closing_date']).timestamp()),
                }
                tasks_track.append(asyncio.ensure_future(get_track_data(session, tokens[username], waybill, period)))
        td = await asyncio.gather(*tasks_track)

    for dictionary in td:
        track_data.update(dictionary)

    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        tasks_dp = [loop.run_in_executor(executor, partial(data_processing, username, waybills[username], cars[username], average_consumption_data, track_data)) for username in waybills]
        await asyncio.gather(*tasks_dp)

    tasks_data = []
    await data_truncate('waybill_all', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    for username in data:
        await data_truncate('waybill_' + username, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
        tasks_data.append(asyncio.ensure_future(data_post(data[username], 'waybill_' + username, waybill_columns, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)))
        tasks_data.append(asyncio.ensure_future(data_post(data[username], 'waybill_all', waybill_columns, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)))

    await asyncio.gather(*tasks_data)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == '__main__':
    asyncio.run(main())
