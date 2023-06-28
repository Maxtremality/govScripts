import time
import asyncio
from datetime import datetime

import settings
from ets_data import token_get
from ets_data import car_actual_get
from db import data_post
from db import data_get
from db import data_update


async def main():
    start = time.time()

    columns = ['okrug_name', 'company_name', 'asuods_id', 'gov_number', 'special_model_name', 'full_model_name',
               'certificate_number', 'is_trailer_required', 'passport_number', 'vin', 'body_number', 'car_group_name',
               'type_name', 'condition_text', 'garage_number', 'manufactured_at', 'company_structure_name',
               'structure_temporary_structure_name', 'gps_code', 'equipment_sensors_str', 'level_sensors_num_text',
               'season_name', 'odometer_mileage', 'motohours_mileage', 'motohours_equip_mileage',
               'waybill_closing_date', 'note']
    token = await token_get('petrenko', 'Petrenkonv!1')
    sql_data = await data_get(', '.join(columns), 'api_caractual', '', settings.DB_API, settings.DB_ACCESS)
    cars = await car_actual_get(token, '')
    new_data = []
    updated_data = []

    for car in cars['result']['rows']:
        past_data = next((sql_car for sql_car in sql_data if car['gov_number'] == sql_car['gov_number']), None)
        if past_data is None:
            row = []
            for key in columns:
                row.append(0 if car[key] is None else car[key])
            new_data.append(tuple(row))
        else:
            updated_data.append({
                car['asuods_id']: {
                    key: 0 if car[key] is None else car[key]
                    for key in past_data
                    if (past_data[key].isoformat() if isinstance(past_data[key], datetime) is True else past_data[key]) != (0 if car[key] is None else car[key])
                }})
    if new_data:
        await data_post(new_data, 'api_caractual', ', '.join(columns), settings.DB_API, settings.DB_ACCESS)
        print(f'Добавлено {len(new_data)} строк')
    if updated_data:
        await data_update(updated_data, 'api_caractual', 'asuods_id', settings.DB_API, settings.DB_ACCESS)
        print(f'Обновлено {len(updated_data)} строк')

    print(time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

if __name__ == "__main__":
    asyncio.run(main())
