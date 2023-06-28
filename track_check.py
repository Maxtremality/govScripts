import json
import time
import asyncio
from datetime import datetime
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import settings
from ets_data import roads_get
from ets_data import dts_get
from ets_data import token_get
from ets_data import car_actual_get
from psd_data import psd_track_get
from db2 import data_get
from db2 import data_post
from db2 import data_truncate


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
    obj = False
    for track_point in track_data['track']:
        if track_point['speed_avg'] > 0:
            for name, coords in roads.items():
                for coord in coords:
                    if (track_point['coords_msk'][0] - 10 <= coord[1] <= track_point['coords_msk'][0] + 10 and track_point['coords_msk'][0] - 10 <= coord[1] <= track_point['coords_msk'][1] + 10):
                        obj = name
                        break
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
        for name, coords in roads.items():
            for coord in coords:
                if (parking_point['start_point']['coords_msk'][1] - 10 <= coord[0] <= parking_point['start_point']['coords_msk'][1] + 10 and parking_point['start_point']['coords_msk'][0] - 10 <= coord[1] <= parking_point['start_point']['coords_msk'][0] + 10):
                    obj = name
                    break
        start_time = datetime.fromtimestamp(parking_point['start_point']['timestamp'])
        end_time = datetime.fromtimestamp(parking_point['end_point']['timestamp'])
        data.append(('Стоянка',
                     gov_number,
                     obj,
                     datetime.isoformat(start_time),
                     datetime.isoformat(end_time),
                     round((end_time - start_time).total_seconds())))


async def main():
    start = time.time()

    table_column = 'status, gov_number, object_text, from_dt, to_dt, diff_dt'
    tracks_data = {}
    token = await token_get(settings.TRAVEL_TIME_ACCESS['avdVAO']['login'], settings.TRAVEL_TIME_ACCESS['avdVAO']['password'])
    period = await db_datetime()
    cars = await car_actual_get(token, '')
    roads = await roads_get(token)
    dts = await dts_get(token)
    roads_converted = await polygon_convert(roads)
    dts_converted = {}
    
    for car in cars['result']['rows']:
        tracks_data[car['gov_number']] = await psd_track_get(token, "?version=4&sensors=1&car_id=" + str(car['asuods_id']) + "&from_dt=" + str(period['from_dt']) + "&to_dt=" + str(period['to_dt']))

    print('Треки собраны за:', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

    for gov_number in tracks_data:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(track_check(gov_number, roads_converted, tracks_data[gov_number]))

    await data_truncate('api_psdtrackcheck', settings.DB_API, 'public', settings.DB_ACCESS)
    await data_post(data, 'api_psdtrackcheck', table_column, settings.DB_API, 'public', settings.DB_ACCESS)

    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

if __name__ == "__main__":
    asyncio.run(main())
