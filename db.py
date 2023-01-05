import aiomysql
import requests
import socket
import subprocess


# url = 'https://ifconfig.me/ip'
# req_data = requests.get(url)
# print(req_data.text)
# print(socket.gethostname())

ID_ACCESS = 3   # Строка с данными входа в бд из файла settings


async def data_get(columns: str, table_name: str, parameters: str, db: str, db_access: list):
    internet = False
    while not internet:
        try:
            subprocess.check_call(["ping", "-c 1", "www.google.ru"])
            internet = True
            connection = await aiomysql.connect(host=db_access[ID_ACCESS]['ip'], port=db_access[ID_ACCESS]['port'],
                                                user=db_access[ID_ACCESS]['user'], password=db_access[ID_ACCESS]['password'],
                                                db=db)
            cursor = await connection.cursor()
            await cursor.execute('SELECT ' + columns + ' FROM ' + table_name + ' ' + parameters)
            data = await cursor.fetchall()
            await cursor.close()
            connection.close()
            return data
        except subprocess.CalledProcessError:
            print("Интернет отвалился :(")


async def data_post(data: list, table_name: str, columns: str, db: str, db_access: list):
    internet = False
    while not internet:
        try:
            subprocess.check_call(["ping", "-c 1", "www.google.ru"])
            internet = True
            connection = await aiomysql.connect(host=db_access[ID_ACCESS]['ip'], port=db_access[ID_ACCESS]['port'],
                                                user=db_access[ID_ACCESS]['user'], password=db_access[ID_ACCESS]['password'], db=db)
            cursor = await connection.cursor()
            await cursor.execute('INSERT INTO ' + table_name + '(' + columns + ') VALUES ' + ", ".join(repr(e) for e in data))
            await cursor.close()
            await connection.commit()
            connection.close()
        except subprocess.CalledProcessError:
            print("Интернет отвалился :(")


async def data_copy(table_to: str, tables_from: list, columns: str, db: str, db_access: list):
    internet = False
    while not internet:
        try:
            subprocess.check_call(["ping", "-c 1", "www.google.ru"])
            internet = True
            connection = await aiomysql.connect(host=db_access[ID_ACCESS]['ip'], port=db_access[ID_ACCESS]['port'],
                                                user=db_access[ID_ACCESS]['user'], password=db_access[ID_ACCESS]['password'], db=db)
            cursor = await connection.cursor()
            for table_from in tables_from:
                await cursor.execute('INSERT INTO ' + table_to + ' (' + columns + ')' + ' SELECT ' + columns + ' FROM ' + table_from)
            await cursor.close()
            await connection.commit()
            connection.close()
        except subprocess.CalledProcessError:
            print("Интернет отвалился :(")


async def data_truncate(table_name: str, db: str, db_access: list):
    internet = False
    while not internet:
        try:
            subprocess.check_call(["ping", "-c 1", "www.google.ru"])
            internet = True
            connection = await aiomysql.connect(host=db_access[ID_ACCESS]['ip'], port=db_access[ID_ACCESS]['port'],
                                                user=db_access[ID_ACCESS]['user'], password=db_access[ID_ACCESS]['password'],
                                                db=db)
            cursor = await connection.cursor()
            await cursor.execute('TRUNCATE ' + table_name)
            await cursor.close()
            await connection.commit()
            connection.close()
        except subprocess.CalledProcessError:
            print("Интернет отвалился :(")


async def data_delete(table_name: str, parameters: str, db: str, db_access: list):
    internet = False
    while not internet:
        try:
            subprocess.check_call(["ping", "-c 1", "www.google.ru"])
            internet = True
            connection = await aiomysql.connect(host=db_access[ID_ACCESS]['ip'], port=db_access[ID_ACCESS]['port'],
                                                user=db_access[ID_ACCESS]['user'], password=db_access[ID_ACCESS]['password'],
                                                db=db)
            cursor = await connection.cursor()
            await cursor.execute('DELETE FROM ' + table_name + ' ' + parameters)
            await cursor.close()
            await connection.commit()
            connection.close()
        except subprocess.CalledProcessError:
            print("Интернет отвалился :(")
