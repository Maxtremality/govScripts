import json
import time
import asyncio
import aiohttp
from datetime import datetime

from ets_data import token_get
from ets_data import car_actual_get
from ets_data import roads_get
from db2 import data_get
from db2 import data_truncate
from db2 import data_post

import settings


data = []


async def db_datetime():
    time_php = await data_get('from_dt, to_dt', 'api_cartraveltimeform', '', settings.DB_API, 'public', settings.DB_ACCESS)
    if time_php[-1] is None:
        print("Нет данных в БД")
        exit(0)
    from_dt = round(time_php[-1][0].timestamp())
    to_dt = round(time_php[-1][1].timestamp())

    return {'from_dt': from_dt, 'to_dt': to_dt}


async def polygon_convert(objects_data):
    objects_converted = {}

    def polygon_parse(polygon):
        result_polygon = []
        for polygon_point in polygon:
            if isinstance(polygon_point[0], list):
                result_polygon.extend(polygon_parse(polygon_point))
            else:
                result_polygon.append(polygon_point)
        return result_polygon

    for obj in objects_data['result']['rows']:
        if 'coordinates' in json.loads(obj['shape']):
            objects_converted[obj['name']] = polygon_parse(json.loads(obj['shape'])['coordinates'])

    def reverse_coords(coords: list):
        reversed_coords = []
        for x in coords:
            reversed_coords.append([x[1], x[0]])
        return reversed_coords

    objects_converted['На базе'] = reverse_coords(settings.BASE_COORDS[4][9000019])
    objects_converted['На гидранте'] = reverse_coords(settings.HYDRANT_COORDS[4][9000019])

    return objects_converted


async def track_check(gov_number, roads, track_data):
    checking_obj = None
    entry_time = None
    out_time = None
    for track_point in track_data['track']:
        if track_point['speed_avg'] > 0:
            obj = next((road for road in roads if next((True for point in roads[road] if point[0] - 10 < track_point['coords_msk'][1] < point[0] + 10 and point[1] - 10 < track_point['coords_msk'][0] < point[1] + 10), False) is True), False)
            if obj is not False:
                if checking_obj is None and entry_time is None:
                    checking_obj = obj
                    entry_time = datetime.fromtimestamp(track_point['timestamp'])
                elif checking_obj != obj:
                    data.append(('В движении',
                                 gov_number,
                                 checking_obj,
                                 datetime.isoformat(entry_time),
                                 datetime.isoformat(out_time),
                                 round((out_time - entry_time).total_seconds())))
                    checking_obj = obj
                    entry_time = datetime.fromtimestamp(track_point['timestamp'])
                out_time = datetime.fromtimestamp(track_point['timestamp'])
    for parking_point in track_data['parkings']:
        obj = next((road for road in roads if next((True for point in roads[road] if point[0] - 80 < parking_point['start_point']['coords_msk'][1] < point[0] + 80 and point[1] - 80 < parking_point['start_point']['coords_msk'][0] < point[1] + 80), False) is True), str([parking_point['start_point']['coords'][0], parking_point['start_point']['coords'][1]]))
        start_time = datetime.fromtimestamp(parking_point['start_point']['timestamp'])
        end_time = datetime.fromtimestamp(parking_point['end_point']['timestamp'])
        data.append(('Стоянка',
                     gov_number,
                     obj,
                     datetime.isoformat(start_time),
                     datetime.isoformat(end_time),
                     round((end_time - start_time).total_seconds())))


async def get_track_data(session, token, car, period):
    url = f'https://psd.mos.ru/tracks-caching/tracks?version=4&car_id={car["asuods_id"]}&from_dt={period["from_dt"]}&to_dt={period["to_dt"]}&sensors=1'
    headers = {'Authorization': token}
    try:
        async with session.get(url, headers=headers, timeout=600) as resp:
            track = await resp.json()
            if len(track['track']) < 10:
                print(f'Нет трека {car["gov_number"]}')
            else:
                return {car['gov_number']: track}
    except:
        print(f'Отказ сервера {car["gov_number"]}')
        return await get_track_data(session, token, car, period)


async def main():
    start = time.time()

    table_column = 'status, gov_number, object_text, from_dt, to_dt, diff_dt'
    token = await token_get(settings.TRAVEL_TIME_ACCESS['avdVAO']['login'], settings.TRAVEL_TIME_ACCESS['avdVAO']['password'])
    period = await db_datetime()
    cars = await car_actual_get(token, '')
    roads = await roads_get(token)
    roads_converted = await polygon_convert(roads)
    
    async with aiohttp.ClientSession(trust_env=True) as session:
        tasks = []
        for car in cars['result']['rows']:
            if car['gps_code'] is not None:
                tasks.append(asyncio.ensure_future(get_track_data(session, token, car, period)))
        td = await asyncio.gather(*tasks)

    print('Треки собраны за:', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

    tracks_data = {}
    for obj in td:
        if obj is not None:
            for gov_number in obj:
                tracks_data[gov_number] = obj[gov_number]

    for gov_number in tracks_data:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(track_check(gov_number, roads_converted, tracks_data[gov_number]))

    # await data_truncate('api_psdtrackcheck', settings.DB_API, settings.DB_ACCESS)
    # await data_post(data, 'api_psdtrackcheck', table_column, settings.DB_API, settings.DB_ACCESS)

    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

if __name__ == '__main__':
    asyncio.run(main())
