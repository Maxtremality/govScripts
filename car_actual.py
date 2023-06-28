import asyncio
import time
from datetime import datetime

import settings
from ets_data import token_get
from ets_data import car_actual_get
from db2 import data_get
from db2 import data_post
from db2 import data_truncate


async def cars_data_get(token):
    car_actual_table_users = 'car_actual'
    car_actual_columns = 'okrug_name, company_name, asuods_id, gov_number, special_model_name, full_model_name, certificate_number, \
                            is_trailer_required, passport_number, vin, body_number, car_group_name, type_name, condition_text, \
                            garage_number, manufactured_at, company_structure_name, structure_temporary_structure_name, \
                            gps_code, equipment_sensors_str, level_sensors_num_text, season_name, odometer_mileage, \
                            motohours_mileage, motohours_equip_mileage, waybill_closing_date, note'
    cars = await car_actual_get(token, '')
    data = []

    for car in cars['result']['rows']:
        data.append((0 if car['okrug_name'] is None else car['okrug_name'],
                     0 if car['company_name'] is None else car['company_name'],
                     0 if car['asuods_id'] is None else car['asuods_id'],
                     0 if car['gov_number'] is None else car['gov_number'],
                     0 if car['special_model_name'] is None else car['special_model_name'],
                     0 if car['full_model_name'] is None else car['full_model_name'],
                     0 if car['certificate_number'] is None else car['certificate_number'],
                     0 if car['is_trailer_required'] is None else car['is_trailer_required'],
                     0 if car['passport_number'] is None else car['passport_number'],
                     0 if car['vin'] is None else car['vin'],
                     0 if car['body_number'] is None else car['body_number'],
                     0 if car['car_group_name'] is None else car['car_group_name'],
                     0 if car['type_name'] is None else car['type_name'],
                     0 if car['condition_text'] is None else car['condition_text'],
                     0 if car['garage_number'] is None else car['garage_number'],
                     0 if car['manufactured_at'] is None else car['manufactured_at'],
                     0 if car['company_structure_name'] is None else car['company_structure_name'],
                     0 if car['temporary_structure_name'] is None else car['temporary_structure_name'],
                     0 if car['gps_code'] is None else car['gps_code'],
                     0 if car['equipment_sensors_str'] is None else car['equipment_sensors_str'],
                     0 if car['level_sensors_num_text'] is None else car['level_sensors_num_text'],
                     0 if car['season_name'] is None else car['season_name'],
                     0 if car['odometer_mileage'] is None else car['odometer_mileage'],
                     0 if car['motohours_mileage'] is None else car['motohours_mileage'],
                     0 if car['motohours_equip_mileage'] is None else car['motohours_equip_mileage'],
                     datetime(1970, 1, 2, 1, 0, 0).isoformat() if car['waybill_closing_date'] is None else car['waybill_closing_date'],
                     0 if car['note'] is None else car['note']))

    await data_post(data, car_actual_table_users, car_actual_columns, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    await data_post(data, 'api_etscaractual', car_actual_columns, settings.DB_API, 'public', settings.DB_ACCESS)


async def main():
    start = time.time()

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    car_actual_table_users = 'car_actual'
    schema = 'scripts'
    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_SCRIPTS, schema, settings.DB_ACCESS)
    await data_truncate('api_etscaractual', settings.DB_API, 'public', settings.DB_ACCESS)
    await data_truncate(car_actual_table_users, settings.DB_SCRIPTS, schema, settings.DB_ACCESS)
    for user in users:
        username = user[1]
        password = user[2]
        token = await token_get(username, password)
        if token is not None:
            await cars_data_get(token)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
