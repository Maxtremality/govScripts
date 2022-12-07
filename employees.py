import asyncio
import time

import settings
from ets_data import employees_get
from ets_data import token_get
from db import data_get
from db import data_post
from db import data_truncate


async def main():
    start = time.time()

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    employees_table = 'employees'
    employees_column = 'full_name, status, position_name, okrug_name, company_name, company_structure_name'

    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)
    await data_truncate(employees_table, settings.DB_USERS, settings.DB_ACCESS)
    # for user in users:
    #     username = user[1]
    #     password = user[2]
    token = await token_get(settings.TRAVEL_TIME_ACCESS['avdSAO']['login'], settings.TRAVEL_TIME_ACCESS['avdSAO']['password'])
    if token != None:
        employees = await employees_get(token)
        data = []
        for employee in employees['result']['rows']:
            data.append((employee['full_name'], employee['status'],
                         '-' if employee['position_name'] == None else employee['position_name'],
                         employee['okrug_name'], employee['company_name'],
                         '-' if employee['company_structure_name'] == None else employee['company_structure_name']))
        await data_post(data, employees_table, employees_column, settings.DB_USERS, settings.DB_ACCESS)
        print(' сотрудники загружены')

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
