import asyncio
import time
from datetime import datetime

import settings
from ets_data import token_get
from ets_data import car_actual_get
from ets_data import fuel_companies_report_get
from psd_data import psd_track_get
from db import data_get
from db import data_post
from db import data_truncate


async def main():
    """
        Шаг координат - 0.002 и 0.001 градусов (x +- 0.001, y +- 0.0005)
        Необходимый временной промежуток нахождения на АЗС - 1 час (-30 и +30 минут от времени заправки (1800 секунд))
    """

    start = time.time()

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    gas_station_pass_table = 'gas_station_pass'
    gas_station_pass_column = 'okrug_name, company_name, structure_name, gov_number, fuel_given, fuel_type, operation_at, \
                               gas_station_address, gas_station_name, status'

    date_start = '2022-10-01T00:00:00'
    date_end = '2022-10-08T23:59:00'
    fuel_companies_params = '?date_start=' + date_start + '&date_end=' + date_end + '&level=company'
    data = []

    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)
    for user in users:
        username = user[1]
        password = user[2]
        token = await token_get(username, password)
        if token != None:
            fuel_companies_report = await fuel_companies_report_get(token, fuel_companies_params)
            car_actual = await car_actual_get(token, '')
            for fuel_fill in fuel_companies_report['result']['rows']:
                status = 0

                if fuel_fill['waybill_number_is_colored'] == False and fuel_fill['car_gov_number_text'] != None:
                    asuods_id = next(car['asuods_id'] for car in car_actual['result']['rows'] if car['gov_number'] == fuel_fill['car_gov_number_text'])
                    psd_params = "?version=4&car_id=" + str(asuods_id) + "&from_dt=" + str(int(datetime.fromisoformat(fuel_fill['operation_at']).timestamp()) - 1800) + "&to_dt=" + str(int(datetime.fromisoformat(fuel_fill['operation_at']).timestamp()) + 1800)
                    psd_track = await psd_track_get(token, psd_params)

                    try:
                        for coords in psd_track['track']:
                            if settings.FUEL_STATION_COORDS[str(fuel_fill['gas_station_id'])]['coordinates'][0] - 0.001 < coords['coords'][0] < settings.FUEL_STATION_COORDS[str(fuel_fill['gas_station_id'])]['coordinates'][0] + 0.001 and settings.FUEL_STATION_COORDS[str(fuel_fill['gas_station_id'])]['coordinates'][1] - 0.0005 < coords['coords'][1] < settings.FUEL_STATION_COORDS[str(fuel_fill['gas_station_id'])]['coordinates'][1] + 0.0005:
                                status = 1
                                break

                        if status == 0:
                            data.append((fuel_fill['okrug_name'], fuel_fill['company_name'], fuel_fill['structure_name'],
                                         fuel_fill['car_gov_number_text'], fuel_fill['fuel_given'], fuel_fill['fuel_type'],
                                         fuel_fill['operation_at_text'], fuel_fill['gas_station_address'], fuel_fill['gas_station_name'],
                                         'АЗС единица не посещала'))
                    except KeyError:
                        data.append((fuel_fill['okrug_name'], fuel_fill['company_name'], fuel_fill['structure_name'],
                                     fuel_fill['car_gov_number_text'], fuel_fill['fuel_given'], fuel_fill['fuel_type'],
                                     fuel_fill['operation_at_text'], fuel_fill['gas_station_address'],
                                     fuel_fill['gas_station_name'],
                                     'АЗС нет в реестре, ID:', fuel_fill['gas_station_id'], fuel_fill['gas_station_name']))

    await data_truncate(gas_station_pass_table, settings.DB_USERS, settings.DB_ACCESS)
    await data_post(data, gas_station_pass_table, gas_station_pass_column, settings.DB_USERS, settings.DB_ACCESS)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))


if __name__ == "__main__":
    asyncio.run(main())
