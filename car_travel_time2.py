import asyncio
import time
from datetime import datetime

import settings
from ets_data import token_get
from ets_data import car_actual_get
from psd_data import psd_track_get
from db2 import data_post
from db2 import data_get
from db2 import data_truncate


result = []


async def get_data(token, x, from_dt, to_dt):
    params = "?version=4&car_id=" + str(x['asuods_id']) + "&from_dt=" + str(from_dt) + "&to_dt=" + str(to_dt)
    psd_track = await psd_track_get(token, params)
    if psd_track['travel_time'] is None:
        psd_track['travel_time'] = 0
    if psd_track['distance'] is None:
        psd_track['distance'] = 0
    result.append((x['gov_number'],
                   round(psd_track['distance'] / 1000, 2),
                   time.strftime('%H:%M:%S', time.gmtime(psd_track['travel_time'])),
                   datetime.fromtimestamp(from_dt).isoformat(),
                   datetime.fromtimestamp(to_dt).isoformat()))


async def ts_ets(token):
    cars = await car_actual_get(token, '')
    time_php = await data_get('from_dt, to_dt', 'api_cartraveltimeform2', '', settings.DB_API, 'public', settings.DB_ACCESS)
    if time_php[-1] is None:
        print("Нет данных в БД")
        exit(0)
    from_dt = round(time_php[-1][0].timestamp())
    to_dt = round(time_php[-1][1].timestamp())
    await data_truncate('car_travel_time2', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    for x in cars['result']['rows']:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(get_data(token, x, from_dt, to_dt))


async def main():
    start = time.time()

    login = settings.TRAVEL_TIME_ACCESS['avdVAO']['login']
    password = settings.TRAVEL_TIME_ACCESS['avdVAO']['password']
    car_time_table = 'car_travel_time2'
    car_time_column = 'gov_number, distance, travel_time, from_dt, to_dt'

    token = await token_get(login, password)
    await ts_ets(token)
    if len(result) > 0:
        await data_post(result, car_time_table, car_time_column, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)

    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

if __name__ == "__main__":
    asyncio.run(main())
