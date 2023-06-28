import requests
import asyncio
import json
import time
from datetime import datetime, timedelta

import settings
from ets_data import token_get, car_actual_get, waybill_get, orders_get


async def create_mission(token):
    url = 'https://ets.mos.ru/services/mission'
    headers = {'Authorization': token}
    payload = {
        'assign_to_waybill': 'assign_to_active',
        'can_be_closed': 'false',
        'can_be_closed_wb': 'false',
        'can_edit_car_and_route': 'true',
        'car_id': 51656,
        'date_end': '2023-03-15T14:00:00',
        'date_start': '2023-03-15T11:01:00',
        'faxogramm_id': 25750,
        'for_column': 'false',
        'hidden': 'false',
        'is_archive': 'false',
        'is_mission_progress_countable': 'false',
        'is_new': 'true',
        'is_trailer_required': 'false',
        'mission_source_id': 4,
        'municipal_facility_id': 1,
        'norm_id': 132832,
        'order_id': 25750,
        'order_number': '260-Ц',
        'order_operation_id': 138678,
        'order_status': 'published',
        'passes_count': 1,
        'route_id': 17006712,
        'route_type': 'mixed',
        'status': 'not_assigned',
        'status_name': 'Не назначено',
        'structure_id': 108,
        'technical_operation_id': 221,
        }
    req_get = requests.post(url, headers=headers, json=payload)
    print(req_get.text)
    
    
async def orders_check(token):
    technical_operations = {}
    technical_operations['roadway_sweep'] = []
    technical_operations['gutter_sweep'] = []
    technical_operations['distance_sweep'] = []
    technical_operations['footway_sweep'] = []
    technical_operations['roadway_wash'] = []
    technical_operations['footway_wash'] = []
    orders = await orders_get(token,
                              (datetime.now() - timedelta(hours=9)).strftime('%Y-%m-%d') + 'T09:00:00',
                              (datetime.now() + timedelta(hours=15)).strftime('%Y-%m-%d') + 'T08:59:59')
    for x in orders['result']:
        for o in x['technical_operations']:
            if o['object_type_name'] == 'ОДХ':
                if o['tk_operation_name'] == 'Сплошное подметание' and o['elem'] == 'Проезжая часть':
                    technical_operations['roadway_sweep'].append(o)
                elif o['tk_operation_name'] == 'Сплошное подметание' and o['elem'] == 'Лотковая часть':
                    technical_operations['gutter_sweep'].append(o)
                elif o['tk_operation_name'] == 'Сплошное подметание' and o['elem'] == 'Осевая разделительная полоса':
                    technical_operations['distance_sweep'].append(o)
                elif o['tk_operation_name'] == 'Сплошное подметание' and o['elem'] == 'Тротуар':
                    technical_operations['footway_sweep'].append(o)
                elif o['tk_operation_name'] == 'Сплошная мойка' and o['elem'] == 'Проезжая часть':
                    technical_operations['roadway_wash'].append(o)
                elif o['tk_operation_name'] == 'Сплошная мойка' and o['elem'] == 'Тротуар':
                    technical_operations['footway_wash'].append(o)
    for x in technical_operations:
        print(x, technical_operations[x])
    return technical_operations


async def wb_mission_check(token, waybill, technical_operations, exception_cars):
    operations = {}
    # for x in operations:
    #     print(x, operations[x])
    # if next((exception_cars[num][0] for num in exception_cars if num == waybill['gov_number']), 'Нет в списке') == 'gutter_sweep' \
    #         or next((exception_cars[num][0] for num in exception_cars if num == waybill['gov_number']), 'Нет в списке') == 'distance_sweep':
    #     print(waybill['gov_number'])
    # print(exception_cars)
    for mission in waybill['mission_id_list']:
        params = '?is_archive=false&filter={"number__eq":' + str(mission) + '}'
        headers = {'Authorization': token}
        req_data = requests.get(settings.MISSION_LIST_URL + params, headers=headers)
        # for x in json.loads(req_data.text)['result']['rows']:
        #     print(x)


async def exception_car_get():
    data = {}
    url = 'http://xn--90adbtawhcfcwwi.xn--p1ai/api/exception_cars/'
    req_get = requests.get(url)
    exception_cars = json.loads(req_get.text)['results'][-1]
    del exception_cars['url']
    for exception_type in exception_cars:
        for car in eval(exception_cars[exception_type]):
            if car not in data:
                data[car] = [exception_type]
            else:
                data[car].append(exception_type)
    del data['yolo']
    return data


async def main():
    start = time.time()
    token = await token_get(settings.TRAVEL_TIME_ACCESS['avdVAO']['login'],
                            settings.TRAVEL_TIME_ACCESS['avdVAO']['password'])
    cars = await car_actual_get(token, '')
    waybills = await waybill_get(token, '?is_archive=false'
                                        '&filter={"status__in":["active"]}')
    technical_operations = await orders_check(token)
    exception_cars = await exception_car_get()
    # for waybill in waybills['result']:
    #     print(waybill['gov_number'],
    #           next((car['type_name'] for car in cars['result']['rows'] if waybill['car_id'] == car['asuods_id']), None),
    #           next((car['type_name'] for car in cars['result']['rows'] if waybill['car_id'] == car['asuods_id']), None),
    #           next(([car['gov_number'], car['full_model_name']] for car in cars['result']['rows'] if waybill['trailer_id'] == car['asuods_id']), ['Нет прицепа']))
    for waybill in waybills['result']:
        # await wb_mission_check(token, waybill, technical_operations, exception_cars)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(wb_mission_check(token, waybill, technical_operations, exception_cars))

    # print(len(waybills['result']))

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == '__main__':
    asyncio.run(main())
