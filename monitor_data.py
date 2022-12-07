import asyncio
from datetime import datetime
import time

from db import data_get
from db import data_copy
from db import data_truncate
import settings

async def main():
    table_name_roadway = []
    table_name_company = []
    table_name_okrug = []

    roadway_table = 'parts_roadway'
    company_table = 'parts_company'
    okrug_table = 'parts_okrug'
    roadway_column = 'okrug_id, okrug_name, company_id, company_name, name, procent, traveled_normalized, road_area, missions, num_exec, structure_name'
    company_column = 'company_name, procent, company_id, okrug_id, okrug_name'
    okrug_column = 'okrug_name, procent, okrug_id'

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)

    for user in users:
        roadway_table_pt = user[1] + '_monitor_roadway'
        company_table_pt = user[1] + '_monitor_company'
        okrug_table_pt = user[1] + '_monitor_okrug'
        if user[1] not in table_name_roadway:
            table_name_roadway.append(roadway_table_pt)
        if user[1] not in table_name_company:
            table_name_company.append(company_table_pt)
        if user[1] not in table_name_okrug:
            table_name_okrug.append(okrug_table_pt)

    while True:
        print('Data upload start')

        await data_truncate(roadway_table, settings.DB_DJANGO, settings.DB_ACCESS)
        await data_copy(roadway_table, table_name_roadway, roadway_column, settings.DB_DJANGO, settings.DB_ACCESS)

        await data_truncate(company_table, settings.DB_DJANGO, settings.DB_ACCESS)
        await data_copy(company_table, table_name_company, company_column, settings.DB_DJANGO, settings.DB_ACCESS)

        # await data_truncate(okrug_table, settings.DB_DJANGO, settings.DB_ACCESS)
        # await data_copy(okrug_table, table_name_okrug, okrug_column, settings.DB_DJANGO, settings.DB_ACCESS)

        print('Данные обновлены', datetime.now())

        time.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
