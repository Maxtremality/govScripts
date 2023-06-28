import aiopg

ID_ACCESS = 6      # Строка с данными входа в бд из файла settings


async def data_get(columns: str, table_name: str, parameters: str, db: str, schema: str, db_access: list):
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    await cursor.execute(f'SELECT {columns} FROM {table_name} {parameters}')
    data = await cursor.fetchall()
    await connection.close()

    return data


async def data_post(data: list, table_name: str, columns: str, db: str, schema: str, db_access: list):
    col = f'({columns})' if len(columns) > 0 else ''
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    await cursor.execute(f'INSERT INTO {table_name} {col} VALUES {", ".join(repr(e) for e in data)}')
    await connection.close()


async def data_truncate(table_name: str, db: str, schema: str, db_access: list):
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    await cursor.execute(f'TRUNCATE {table_name}')
    await cursor.execute(f'ALTER SEQUENCE {table_name}_id_seq RESTART WITH 1')
    await connection.close()


async def data_update(data: list, table_name: str, sample: str, db: str, schema: str, db_access: list):
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    query_str = []
    for row in data:
        for key in row:
            set_str = ', '.join(f'{x} = {row[key][x]}' for x in row[key])
            query_str.append(f'UPDATE {table_name} SET {set_str} WHERE {sample} = {key}')
    await cursor.execute("; ".join(q for q in query_str))
    await connection.close()


async def data_copy(table_to: str, tables_from: list, columns: str, db: str, schema: str, db_access: list):
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    for table_from in tables_from:
        await cursor.execute(f'INSERT INTO {table_to} ({columns}) SELECT {columns} FROM {table_from}')
    await connection.close()


async def data_delete(table_name: str, parameters: str, db: str, schema: str, db_access: list):
    connection = await aiopg.connect(database=db,
                                     user=db_access[ID_ACCESS]['user'],
                                     password=db_access[ID_ACCESS]['password'],
                                     host=db_access[ID_ACCESS]['ip'],
                                     timeout=600,
                                     options=f'-c search_path={schema}')
    cursor = await connection.cursor()
    await cursor.execute(f'DELETE FROM {table_name} {parameters}')
    await connection.close()
