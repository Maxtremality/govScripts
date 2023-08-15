import json
import requests
import settings


# Токен для авторизации в api
async def token_get(login: str, password: str):
    url = 'https://ets.mos.ru/services/auth'
    payload = {'login': login, 'password': password}
    req_post = requests.post(url, json=payload)
    req_data = json.loads(req_post.text)
    try:
        return 'Token ' + req_data['token']
    except:
        print("Пароль для аккаунта " + login + " изменён")


# Токен для авторизации в websocket
async def wss_token_get(login: str, password: str):
    url = 'https://ets.mos.ru/services/auth'
    payload = {'login': login, 'password': password}
    req_post = requests.post(url, json=payload)
    req_data = json.loads(req_post.text)
    return req_data['token']


# Получение списка организаций (зависит от уровня доступа аккаунта)
async def company_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.COMPANY_URL + params, headers=headers)
    company = json.loads(req_data.text)

    return company


# Получение списка путевых листов
async def waybill_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.WAYBILL_URL + params, headers=headers)
    waybill = json.loads(req_data.text)

    return waybill


# Создание путевого листа
async def waybill_post(token: str, data: dict):
    json_data = json.dumps(data)
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req = requests.post(settings.WAYBILL_URL, headers=headers, data=json_data)

    if req.status_code == 200:
        print('Путевой лист успешно создан:', 'Гос. номер:', data['gov_number'])
    else:
        print('Отказ сервера:', 'Гос. номер:', data['gov_number'], req.json())
    return None


# Удаление путевого листа
async def waybill_delete(token: str, number: int):
    data = {'id': number}
    json_data = json.dumps(data)
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    requests.delete(settings.WAYBILL_URL, headers=headers, data=json_data)

    print('Путевой лист', 'ID', str(number), 'успешно удален')
    return None


# Получение списка транспортных средств
async def car_actual_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.CAR_ACTUAL_URL + params, headers=headers)
    car_actual = json.loads(req_data.text)

    return car_actual


# Получение списка факсограм (ограничение по датам)
async def orders_get(token: str, date_start: str, date_end: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    params = '?date_start=' + date_start + '&date_end=' + date_end + '&sort_by=create_date:desc&filter={"order_number__like":"%Ц%"}'
    req_data = requests.get(settings.ORDER_URL + params, headers=headers)
    orders = json.loads(req_data.text)

    return orders


# Получение списка ОДХ
async def roads_get(token: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.ODH_URL, headers=headers)
    roads = json.loads(req_data.text)

    return roads


# Получение списка ДТ
async def dts_get(token: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.DT_URL, headers=headers)
    dts = json.loads(req_data.text)

    return dts


# Получение топливного отчета
async def fuel_report_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.FUEL_REPORT_URL + params, headers=headers)
    fuel_report = json.loads(req_data.text)

    return fuel_report


# Получение списка сотрудников
async def employees_get(token: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.EMPLOYEES_URL, headers=headers)
    employees = json.loads(req_data.text)

    return employees


# Получение топливного отчета по данным Роснефти
async def fuel_companies_report_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.FUEL_COMPANIES_REPORT + params, headers=headers)
    fuel_companies_report = json.loads(req_data.text)

    return fuel_companies_report


# Получение номеров заданий
async def mission_list_get(token: str, params: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_data = requests.get(settings.MISSION_LIST_URL + params, headers=headers)
    mission_list = []

    for data in req_data.json()['result']['rows']:
        mission_list.append(data['id'])

    return mission_list


# Получение данных задания
async def mission_data_get(token: str, number: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_get = requests.get(settings.MISSION_DATA_URL + number, headers=headers)
    mission_data = req_get.json()

    return mission_data


# Получение данных норм расхода топлива
async def fuel_consumption_rate_get(token: str):
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req_get = requests.get(settings.FUEL_CONSUMPTION_RATE, headers=headers)
    fuel_consumption_rate = req_get.json()

    return fuel_consumption_rate


# Запись данных норм расхода топлива
async def fuel_consumption_rate_post(token: str, data: dict):
    json_data = json.dumps(data)
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    req = requests.post(settings.FUEL_CONSUMPTION_RATE, headers=headers, data=json_data)

    if req.status_code == 200:
        print('Норма успешно записана:', 'ID Асу ОДС', data['car_id'], 'ID операции', data['operation_id'])
    else:
        print('Отказ сервера:', 'ID Асу ОДС', data['car_id'], 'ID операции', data['operation_id'], req.json())
    return None


# Удаление данных норм расхода топлива
async def fuel_consumption_rate_delete(token: str, number: int):
    data = {'id': number}
    json_data = json.dumps(data)
    headers = {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    requests.delete(settings.FUEL_CONSUMPTION_RATE, headers=headers, data=json_data)

    print('Норма', 'ID', str(number), 'успешно удалена')
    return None
