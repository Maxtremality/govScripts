import asyncio

import db
import db2
import settings


async def main():
    data1 = await db.data_get('*', 'average_consumption', '', settings.DB_USERS, settings.DB_ACCESS)
    await db2.data_post(data1, 'average_consumption', 'id, special_model_name, full_model_name, type_name, average_consumption', settings.DB_SCRIPTS, 'users', settings.DB_ACCESS)

if __name__ == '__main__':
    asyncio.run(main())
