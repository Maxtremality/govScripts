import asyncio
import time
import aiohttp

import settings
from ets_data import token_get
from db2 import data_get
from db2 import data_post
from db2 import data_truncate


async def get_employees_data(session, username, token):
    url = 'https://ets.mos.ru/services/employee?actual=true'
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    async with session.get(url, headers=headers, timeout=600) as resp:
        req = await resp.json()
        print(f'{username} реестр сотрудников загружен')
        result = []
        for employee in req['result']['rows']:
            if employee['full_name'] == 'Букин Павел Вячеславович':
                print(employee)
            result.append((
                employee['okrug_name'],
                employee['company_name'],
                '' if employee['company_structure_name'] is None else employee['company_structure_name'],
                employee['full_name'],
                employee['status'],
                '' if employee['position_name'] is None else employee['position_name']
            ))
        return result


async def main():
    start = time.time()

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    employees_table = 'api_etsemployees'
    employees_column = 'okrug_name, company_name, company_structure_name, full_name, status, position_name'
    tokens = {}
    employees = []

    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)

    for user in users:
        tokens[user[1]] = await token_get(user[1], user[2])

    tasks_employees = []

    async with aiohttp.ClientSession() as session:
        for okrug_name in tokens:
            tasks_employees.append(asyncio.ensure_future(get_employees_data(session, okrug_name, tokens[okrug_name])))
        employees_data = await asyncio.gather(*tasks_employees)

    for item in employees_data:
        employees = employees + item

    await data_post(employees, employees_table, employees_column, settings.DB_API, 'public', settings.DB_ACCESS)

    await data_truncate(employees_table, settings.DB_API, 'public', settings.DB_ACCESS)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
