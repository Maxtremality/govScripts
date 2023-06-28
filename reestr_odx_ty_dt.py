import requests
import asyncio
import json

import settings
from db import data_truncate
from db import data_post
from db import data_get
from ets_data import token_get


async def reestr_dt_get(token):
    url = "https://ets.mos.ru/services/geozones/dt?"
    headers = {'Authorization': token}
    way = requests.get(url, headers=headers)
    data_full = json.loads(way.text)
    data_result = data_full['result']
    data_rows = data_result['rows']

    data = []
    for i in data_rows:
        data.append((
            i['company_name'], # Организация
            i['name'], # Название ДТ
            i['total_area'], # Общая площадь (кв.м.)
            i['clean_area'], # Общая уборочная площадь (кв.м.)
            i['auto_area'], # Площадь механизированной уборки (кв.м.)
            i['area_machine_sum'], # Территория уборки усовершенствованных покрытий, все классы, механизированная, кв.м
            i['area_hand_improved_sum'],# Площадь усовершенствованных покрытий для ручной уборки, кв.м
            "Механизированная" if i['auto_area'] > 0 else "Ручная"
        ))

    return data


async def reestr_odx_get(token):
    url = "https://ets.mos.ru/services/geozones/odh?"
    headers = {'Authorization': token}
    way = requests.get(url, headers=headers)
    data_full = json.loads(way.text)
    data_result = data_full['result']
    data_rows = data_result['rows']

    data = []
    for x in data_rows:
        data.append((
            x['company_name'], # Организация
            x['name'], # Название ОДХ
            x['total_area'],  # Общая площадь (кв.м.)
            x['distance'], # Протяженность (п.м.)
            x['roadway_area'], # Площадь мех. уборки проезжей части (кв.м.)
            x['footway_area'], # Площадь тротуаров (кв.м.)
            x['cleaning_area'],# Площадь уборки (кв.м.)
            x['footway_length'], # Длина тротуара (п.м.)
            x['auto_footway_area'], # Площадь механизированной уборки тротуаров (кв.м.)
            x['manual_footway_area'], # Площадь ручной уборки тротуаров (кв.м.)
            x['snow_area'], # Площадь уборки снега (кв.м.)
            x['gutters_length'], # Протяженность лотков (п.м.)
            x['company_structures_text'], #Подразделение
            "Мех. уборка ОДХ" if x['roadway_area'] > 0 else "Ручная уборка",
            "Мех. уборка ТУ" if x['auto_footway_area'] > 0 else "Ручная уборка"
        ))
    return data


async def main():
    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    dt_table = 'geozones_dt'
    odx_table = 'geozones_odx'
    dt_column = 'company_name, name, total_area, clean_area, auto_area, area_machine_sum, area_hand_improved_sum, status'
    odx_column = 'company_name, name, total_area, distance, roadway_area, footway_area, cleaning_area, footway_length, auto_footway_area, manual_footway_area, snow_area, gutters_length, company_structures_text,status_odx, status_ty'
    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)
    await data_truncate(dt_table, settings.DB_USERS, settings.DB_ACCESS)
    await data_truncate(odx_table, settings.DB_USERS, settings.DB_ACCESS)
    for user in users:
        username = user[1]
        password = user[2]
        token = await token_get(username, password)
        if token is not None:
            token = await token_get(username, password)
            reestr_dt = await reestr_dt_get(token)
            reestr_odx = await reestr_odx_get(token)
            await data_post(reestr_dt, dt_table, dt_column, settings.DB_USERS, settings.DB_ACCESS)
            await data_post(reestr_odx, odx_table, odx_column, settings.DB_USERS, settings.DB_ACCESS)

if __name__ == "__main__":
    asyncio.run(main())
