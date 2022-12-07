import asyncio
import time

import settings
from ets_data import token_get
from ets_data import car_actual_get
from db import data_post
from db import data_truncate


async def main():
    start = time.time()

    token = await token_get('petrenko', 'Petrenkonv!1')
    car_actual_table_users = 'car_actual'
    car_actual_table_parts = 'parts_caractual'
    car_actual_columns = 'okrug_name, company_name, gov_number, special_model_name, full_model_name, certificate_number, \
                        is_trailer_required, passport_number, vin, body_number, car_group_name, type_name, condition_text, \
                        garage_number, manufactured_at, company_structure_name, structure_temporary_structure_name, \
                        gps_code, equipment_sensors_str, level_sensors_num_text, season_name, odometer_mileage, \
                        motohours_mileage, motohours_equip_mileage, waybill_closing_date, note'
    cars = await car_actual_get(token, '')
    data = []

    for car in cars['result']['rows']:
        data.append((car['okrug_name'] == 0 if car['okrug_name'] == None else car['okrug_name'],
                     car['company_name'] == 0 if car['company_name'] == None else car['company_name'],
                     car['gov_number'] == 0 if car['gov_number'] == None else car['gov_number'],
                     car['special_model_name'] == 0 if car['special_model_name'] == None else car['special_model_name'],
                     car['full_model_name'] == 0 if car['full_model_name'] == None else car['full_model_name'],
                     car['certificate_number'] == 0 if car['certificate_number'] == None else car['certificate_number'],
                     car['is_trailer_required'] == 0 if car['is_trailer_required'] == None else car['is_trailer_required'],
                     car['passport_number'] == 0 if car['passport_number'] == None else car['passport_number'],
                     car['vin'] == 0 if car['vin'] == None else car['vin'],
                     car['body_number'] == 0 if car['body_number'] == None else car['body_number'],
                     car['car_group_name'] == 0 if car['car_group_name'] == None else car['car_group_name'],
                     car['type_name'] == 0 if car['type_name'] == None else car['type_name'],
                     car['condition_text'] == 0 if car['condition_text'] == None else car['condition_text'],
                     car['garage_number'] == 0 if car['garage_number'] == None else car['garage_number'],
                     car['manufactured_at'] == 0 if car['manufactured_at'] == None else car['manufactured_at'],
                     car['company_structure_name'] == 0 if car['company_structure_name'] == None else car['company_structure_name'],
                     car['structure_temporary_structure_name'] == 0 if car['structure_temporary_structure_name'] == None else car['structure_temporary_structure_name'],
                     car['gps_code'] == 0 if car['gps_code'] == None else car['gps_code'],
                     car['equipment_sensors_str'] == 0 if car['equipment_sensors_str'] == None else car['equipment_sensors_str'],
                     car['level_sensors_num_text'] == 0 if car['level_sensors_num_text'] == None else car['level_sensors_num_text'],
                     car['season_name'] == 0 if car['season_name'] == None else car['season_name'],
                     car['odometer_mileage'] == 0 if car['odometer_mileage'] == None else car['odometer_mileage'],
                     car['motohours_mileage'] == 0 if car['motohours_mileage'] == None else car['motohours_mileage'],
                     car['motohours_equip_mileage'] == 0 if car['motohours_equip_mileage'] == None else car['motohours_equip_mileage'],
                     car['waybill_closing_date'] == 0 if car['waybill_closing_date'] == None else car['waybill_closing_date'],
                     car['note'] == 0 if car['note'] == None else car['note']))

    await data_truncate(car_actual_table_users, settings.DB_USERS, settings.DB_ACCESS)
    await data_truncate(car_actual_table_parts, settings.DB_DJANGO, settings.DB_ACCESS)
    await data_post(data, car_actual_table_users, car_actual_columns, settings.DB_USERS, settings.DB_ACCESS)
    await data_post(data, car_actual_table_parts, car_actual_columns, settings.DB_DJANGO, settings.DB_ACCESS)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
