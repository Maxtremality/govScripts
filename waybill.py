import asyncio
import aiohttp
import time
from datetime import datetime
from dateutil.relativedelta import *
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json

from ets_data import token_get
from db2 import data_get
from db2 import data_post
from db2 import data_truncate
import settings


data = {}


def data_processing(username, waybills, cars, average_consumption_data, estp):
    result = []
    for waybill in waybills['result']:
        if waybill['car_id'] in cars:
            fuel_estp = estp[waybill['id'] if waybill['id'] in estp else 0]

            car = cars[waybill['car_id']]
            car['normal_average_consumption'] = float(average_consumption_data[f"{car['special_model_name']} {car['full_model_name']}"]) if f"{car['special_model_name']} {car['full_model_name']}" in average_consumption_data else 0

            if waybill['track_length_km'] is not None and waybill['track_length_km'] != 0 and waybill['sensor_consumption'] is not None and waybill['sensor_consumption'] != 0:
                car['sensor_average_consumption'] = round(waybill['sensor_consumption'] / waybill['track_length_km'] * 100, 2)
            else:
                car['sensor_average_consumption'] = 0

            if car['sensor_average_consumption'] > car['normal_average_consumption'] and car['normal_average_consumption'] != 0:
                car['status_average_consumption'] = 'Перерасход'
                car['difference_consumption'] = round((car['normal_average_consumption'] / 100 * waybill['track_length_km']) - waybill['sensor_consumption'], 2)
            else:
                car['status_average_consumption'] = 'Норма'
                car['difference_consumption'] = 0
            sensors = car['level_sensors_num']
            gps_code = 'Нет БНСО' if car['gps_code'] is None else 'Есть БНСО'

            odometr_diff = (0 if waybill['odometr_end'] is None else waybill['odometr_end']) - (0 if waybill['odometr_start'] is None else waybill['odometr_start'])  #Пробег по ПЛ
            motohours_diff = (0 if waybill['motohours_end'] is None else waybill['motohours_end']) - (0 if waybill['motohours_start'] is None else waybill['motohours_start'])  #Моточасы по ПЛ
            motohours_equip_diff = (0 if waybill['motohours_equip_end'] is None else waybill['motohours_equip_end']) - (0 if waybill['motohours_equip_start'] is None else waybill['motohours_equip_start']) #Моточасы по ПЛ оборудование
            fuel_givens = (0 if waybill['equipment_fuel_given'] is None else waybill['equipment_fuel_given']) + (0 if waybill['fuel_given'] is None else waybill['fuel_given']) #Выдано литры общее
            mileage_diff = (odometr_diff - (0 if waybill['track_length_km'] is None else waybill['track_length_km'])) if odometr_diff > 0 and waybill['track_length_km'] == 0 else round((odometr_diff - ((0 if waybill['track_length_km'] is None else waybill['track_length_km']) / 100 * 110)), 3) #Разница пробегов ПЛ-ГЛОНАСС( с 10% погрешности)

            try:
                mileage_diff_percent = round((100 - (((0 if waybill['track_length_km'] is None else waybill['track_length_km']) * 0.1 + (0 if waybill['track_length_km'] is None else waybill['track_length_km'])) / odometr_diff) * 100), 2)
            except ZeroDivisionError:
                mileage_diff_percent = "0"      # Разница пробега ПЛ-ГЛОНАСС проценты

            mileage_diff_status = "Перепробег" if float(mileage_diff_percent) > 0 else "Норма"      # Статус разницы пробегов ПЛ-ГЛОНАСС
            motohours_or_km_status = "Моточасы" if (0 if waybill['motohours_start'] is None else waybill['motohours_start']) > 0 else "Километры"   # Статус моточасы или километры
            refill_diff = (fuel_givens - (0 if waybill['sensor_refill'] is None else waybill['sensor_refill'])) if (0 if waybill['sensor_refill'] is None else waybill['sensor_refill']) == 0 else round(((fuel_givens / 100) * 93 - (0 if waybill['sensor_refill'] is None else waybill['sensor_refill'])), 2) # Разница заправки с учетом 7% литры

            try:
                refill_diff_percent = round(((fuel_givens - (0 if waybill['sensor_refill'] is None else waybill['sensor_refill'])) / fuel_givens * 100),)
            except ZeroDivisionError:
                refill_diff_percent = "0"


            refill_diff_status = "Недолив" if float(refill_diff_percent) > 7 else "Норма"
            odometr_motohours_status = "Требует внимание" if motohours_diff > 0 and (0 if waybill['track_length_km'] is None else waybill['track_length_km']) == 0 else "Норма"
            sum_consumption = (0 if waybill['fact_consumption'] is None else waybill['fact_consumption']) + (0 if waybill['equipment_fact_consumption'] is None else waybill['equipment_fact_consumption'])
            diff_sum_consumption = round((sum_consumption / 100 * 93 - (0 if waybill['sensor_consumption'] is None or waybill['sensor_consumption'] < 0 else waybill['sensor_consumption'])), 2)
            diff_sum_consumption_status = "Требует внимания" if float(diff_sum_consumption) > 0 else "Норма"
            diff_fact_tax = round((((0 if waybill['fact_consumption'] is None else waybill['fact_consumption']) + (0 if waybill['equipment_fact_consumption'] is None else waybill['equipment_fact_consumption'])) - ((0 if waybill['tax_consumption'] is None else waybill['tax_consumption']) + (0 if waybill['equipment_tax_consumption'] is None else waybill['equipment_tax_consumption']))), 2)
            diff_fact_tax_status = "Требует внимания" if diff_fact_tax > 0 else "Норма"

            result.append((0 if waybill['okrug_name'] is None else waybill['okrug_name'],
                           0 if waybill['company_name'] is None else waybill['company_name'],
                           0 if waybill['status_text'] is None else waybill['status_text'],
                           0 if waybill['number'] is None else waybill['number'],
                           0 if waybill['activating_date'] is None else waybill['activating_date'],
                           datetime(1970, 1, 2, 1, 0, 0).isoformat() if waybill['closing_date'] is None else waybill['closing_date'],
                           0 if waybill['driver_name'] is None else waybill['driver_name'],
                           0 if waybill['gov_number'] is None else waybill['gov_number'],
                           0 if waybill['fact_departure_date'] is None else waybill['fact_departure_date'],
                           0 if waybill['fact_arrival_date'] is None else waybill['fact_arrival_date'],
                           0 if waybill['closed_by_employee_name'] is None else waybill['closed_by_employee_name'],
                           0 if waybill['odometr_start'] is None else waybill['odometr_start'],
                           0 if waybill['odometr_end'] is None else waybill['odometr_end'],
                           odometr_diff,
                           mileage_diff,
                           mileage_diff_percent,
                           mileage_diff_status,
                           motohours_or_km_status,
                           odometr_motohours_status,
                           0 if waybill['motohours_start'] is None else waybill['motohours_start'],
                           0 if waybill['motohours_end'] is None else waybill['motohours_end'],
                           motohours_diff,
                           0 if waybill['motohours_equip_start'] is None else waybill['motohours_equip_start'],
                           0 if waybill['motohours_equip_end'] is None else waybill['motohours_equip_end'],
                           motohours_equip_diff,
                           0 if waybill['fuel_type'] is None else waybill['fuel_type'],
                           0 if waybill['fuel_given'] is None else waybill['fuel_given'],
                           0 if waybill['fuel_start'] is None else waybill['fuel_start'],
                           0 if waybill['fuel_end'] is None else waybill['fuel_end'],
                           0 if waybill['equipment_fuel_type'] is None else waybill['equipment_fuel_type'],
                           0 if waybill['equipment_fuel_given'] is None else waybill['equipment_fuel_given'],
                           0 if waybill['equipment_fuel_start'] is None else waybill['equipment_fuel_start'],
                           0 if waybill['equipment_fuel_end'] is None else waybill['equipment_fuel_end'],
                           0 if waybill['tax_consumption'] is None else waybill['tax_consumption'],
                           0 if waybill['fact_consumption'] is None else waybill['fact_consumption'],
                           0 if waybill['equipment_tax_consumption'] is None else waybill['equipment_tax_consumption'],
                           0 if waybill['equipment_fact_consumption'] is None else waybill['equipment_fact_consumption'],
                           0 if waybill['equipment_diff_consumption'] is None else waybill['equipment_diff_consumption'],
                           diff_sum_consumption,
                           diff_sum_consumption_status,
                           fuel_givens,
                           0 if waybill['fuel_card_id'] is None else waybill['fuel_card_id'],
                           0 if waybill['equipment_fuel_card_id'] is None else waybill['equipment_fuel_card_id'],
                           0 if waybill['track_length_km'] is None else waybill['track_length_km'],
                           0 if waybill['sensor_start_value'] is None else waybill['sensor_start_value'],
                           0 if waybill['sensor_finish_value'] is None else waybill['sensor_finish_value'],
                           0 if waybill['sensor_consumption'] is None else waybill['sensor_consumption'],
                           0 if waybill['sensor_refill'] is None else waybill['sensor_refill'], # Заправка по ДУТ
                           refill_diff,           # Разница заправки с учетом 7% литры
                           refill_diff_status,    # Статус заправки недолив или норма.
                           refill_diff_percent,   # Процент заправки.
                           diff_fact_tax,
                           diff_fact_tax_status,
                           0 if waybill['sensor_leak'] is None else waybill['sensor_leak'],
                           0 if waybill['structure_name'] is None else waybill['structure_name'],
                           0 if waybill['comment'] is None else waybill['comment'],
                           sensors,
                           gps_code,
                           car['normal_average_consumption'],
                           car['sensor_average_consumption'],
                           car['difference_consumption'],
                           car['status_average_consumption'],
                           car['type_name'],
                           car['special_model_name'],
                           car['full_model_name'],
                           fuel_estp,
                           "ГЛОНАСС исправен" if waybill['is_bnso_broken'] is False else ("ГЛОНАСС неисправен" if waybill['is_bnso_broken'] is True else "ГЛОНАСС не установлен")))

    print(f'{username} обработка данных завершена')
    data[username] = result


async def get_cars_data(session, username, token):
    url = 'https://ets.mos.ru/services/car_actual'
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            req = await resp.json()
            if resp.status == 500:
                req = await get_cars_data(session, username, token)
                return req
            print(f'{username} реестр техники загружен')
            result_dict = {}
            for item in req['result']['rows']:
                result_dict[item['asuods_id']] = item
            return {username: result_dict}
    except TimeoutError:
        print(f'Таймаут {username}')


async def get_waybills_data(session, username, token, date):
    url = 'https://ets.mos.ru/services/waybill?filter={"date_create__gt":"' + str(date.date()) + '","status__in":["closed"]}'
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            req = await resp.json()
            if resp.status == 500:
                req = await get_waybills_data(session, username, token, date)
                return req
            print(f'{username} путевые листы загружены')
            return {username: req}
    except TimeoutError:
        print(f'Таймаут {username}')
        
        
async def get_estp_data(session, waybill):
    if waybill['company_name'] == 'АвД ВАО':
        url = f'https://estp.dgkh.msk.ru/index.php?r=fuel/fuel-static-new&search={ waybill["gov_number"] }&org=0&filter%5Buser%5D=&filter%5Bmax_count%5D=100&filter%5Bdate_to%5D={ waybill["fact_arrival_date"] }&filter%5Bdate_from%5D={ waybill["fact_departure_date"] }'
        headers_estp = {
            'Cookie': '_identity=399fd0229ea093a8612b572bfa4018f84215c5947855b801200f629725f02a64a%3A2%3A%7Bi%3A0%3Bs%3A9%3A%22_identity%22%3Bi%3A1%3Bs%3A48%3A%22%5B815%2C%22PfYWziVdX5aondZIFligsbNd_8m5Ju3H%22%2C2592000%5D%22%3B%7D; PHPSESSID=8ve0sv3409sha9d39f1mflc635; _csrf=c39ce217332155de14d7a1343f5d6096cf21ed576c75c6489874df1ec3d717dca%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22aMFKDkyOq1jyGd_K_z4B0hEAAj6Sqqg6%22%3B%7D',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }
        async with session.get(url, headers=headers_estp, timeout=600) as response:
            resp = await response.text()
            data_full = json.loads(resp)
            if data_full['result']:
                data_result = data_full['result'][0]
                fuel_up = data_result['fuel_up_l_val']
                if fuel_up == '':
                    fuel_estp = 0
                else:
                    parts = [int(val) for val in fuel_up.split(";")]
                    fuel_estp = sum(parts)
            else:
                fuel_estp = 0

        return {waybill['id']: fuel_estp}
    else:
        return {waybill['id']: 0}


async def main():
    start = time.time()

    waybills = {}
    estp = {}
    cars = {}
    tokens = {}
    
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
                      normal_average_consumption, sensor_average_consumption, difference_consumption, status_average_consumption, type_name, special_model_name, full_model_name, fuel_estp,glonass_status'

    date = datetime.now() - relativedelta(months=3)
    users_tuple = await data_get('*', 'ets_users', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    keys = ['id', 'username', 'password', 'okrug_id', 'company_id', 'okrug_name']
    users = [dict(zip(keys, row)) for row in users_tuple]
    average_consumption_data = await data_get('*', 'average_consumption', '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    average_consumption_dict = {f"{item[1]} {item[2]}": item[4] for item in average_consumption_data}

    for user in users:
        if user['okrug_name'] not in tokens:
            tokens[user['okrug_name']] = {}
        tokens[user['okrug_name']][user['company_id']] = await token_get(user['username'], user['password'])

    async with aiohttp.ClientSession() as session:
        tasks_waybills = []
        tasks_cars = []
        for okrug_name in tokens:
            if 'all' in tokens[okrug_name]:
                tasks_cars.append(asyncio.ensure_future(get_cars_data(session, okrug_name, tokens[okrug_name]['all'])))
                tasks_waybills.append(asyncio.ensure_future(get_waybills_data(session, okrug_name, tokens[okrug_name]['all'], date)))
            else:
                for company_name in tokens[okrug_name]:
                    tasks_cars.append(asyncio.ensure_future(get_cars_data(session, okrug_name, tokens[okrug_name][company_name])))
                    tasks_waybills.append(asyncio.ensure_future(get_waybills_data(session, okrug_name, tokens[okrug_name][company_name], date)))
        waybills_data = await asyncio.gather(*tasks_waybills)
        cars_data = await asyncio.gather(*tasks_cars)

    for dictionary in waybills_data:
        waybills.update(dictionary)
    for dictionary in cars_data:
        cars.update(dictionary)

    async with aiohttp.ClientSession() as session:
        estp_data_tasks = []
        for okrug in waybills:
            for waybill in waybills[okrug]['result']:
                estp_data_tasks.append(asyncio.ensure_future(get_estp_data(session, waybill)))

        estp_data = await asyncio.gather(*estp_data_tasks)
        print(f'Данные ЕСТП загружены')

    for dictionary in estp_data:
        estp.update(dictionary)

    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        tasks_dp = [loop.run_in_executor(executor, partial(data_processing, username, waybills[username], cars[username], average_consumption_dict, estp)) for username in waybills]
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
