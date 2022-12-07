import asyncio
from datetime import datetime
from datetime import timedelta
import time

from ets_data import token_get
from ets_data import waybill_get
from ets_data import car_actual_get
from ets_data import fuel_report_get
from db import data_post
from db import data_get
from db import data_truncate
import settings


async def fuel_report(token, find_car, date):
    params = '?car=all&date_end=' + date + '&date_start=' + (datetime.fromisoformat(date) - timedelta(minutes=10)).isoformat() + '&level=company&gov_number=' + find_car
    report_car = await fuel_report_get(token, params)
    if report_car['result']['rows'] != []:
        if report_car['result']['rows'][0]['sensor_finish_value'] == '-':
            report_car['result']['rows'][0]['sensor_finish_value'] = 0
        else:
            pass
        if report_car['result']['rows'][0]['sensor_finish_value'] == 0:
            report_data = {'gov_number': report_car['result']['rows'][0]['car_gov_number'],
                           'fuel_end': report_car['result']['rows'][0]['fact_fuel_end'],
                           'sensor_finish_value': report_car['result']['rows'][0]['sensor_finish_value'],
                           'difference': round(report_car['result']['rows'][0]['fact_fuel_end'] - report_car['result']['rows'][0]['sensor_finish_value'], 2),
                           'status': 'Проблема с ДУТ'}
        else:
            report_data = {'gov_number': report_car['result']['rows'][0]['car_gov_number'],
                           'fuel_end': report_car['result']['rows'][0]['fact_fuel_end'],
                           'sensor_finish_value': report_car['result']['rows'][0]['sensor_finish_value'],
                           'difference': round(report_car['result']['rows'][0]['fact_fuel_end'] - report_car['result']['rows'][0]['sensor_finish_value'], 2),
                           'status': 'ДУТ работает'}
    return report_data


async def car_actual(token):
    cars = await car_actual_get(token, '')
    data = []
    f = 1
    for car in cars['result']['rows']:
        if car['waybill_closing_date'] is not None:
            try:
                date = car['waybill_closing_date']
                result = await fuel_report(token, car['gov_number'], str(date))
                data.append((car['okrug_name'], car['company_name'], car['gov_number'], result['fuel_end'], result['sensor_finish_value'], result['difference'],
                             result['status'], car['waybill_closing_date'], car['level_sensors_num'],
                             'Нет БНСО' if car['gps_code'] is None else car['gps_code']))
                print(f, car['okrug_name'], car['company_name'], car['gov_number'])
                f += 1
            except UnboundLocalError:
                continue
    return data


async def main():
    start = time.time()

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    deficit_fuel_table = 'deficit_fuel'
    deficit_fuel_column = 'okrug_name, company_name, gov_number, fuel_end, sensor_finish_value, difference, status, date, level_sensors_num, gps_code'
    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)
    await data_truncate(deficit_fuel_table, settings.DB_USERS, settings.DB_ACCESS)
    for user in users:
        username = user[1]
        password = user[2]
        token = await token_get(username, password)
        if token is not None:
            data = await car_actual(token)
            await data_post(data, deficit_fuel_table, deficit_fuel_column, settings.DB_USERS, settings.DB_ACCESS)
            print(username + ' остатки загружены')

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
