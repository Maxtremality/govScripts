import asyncio
import time
from datetime import datetime

import settings
from ets_data import token_get
from ets_data import car_actual_get
from psd_data import psd_track_get
from db import data_post
from db import data_get
from db import data_truncate

async def ts_ets(token):
    cars = await car_actual_get(token, '')
    time_php = await data_get('from_dt, to_dt', 'car_travel_time_php', '', settings.DB_USERS, settings.DB_ACCESS)
    if time_php[0] == None:
        print("Нет данных в БД")
        exit(0)
    from_dt = time_php[0][0]
    to_dt = time_php[0][1]
    last_time = await data_get('from_dt, to_dt', 'car_travel_time', '', settings.DB_USERS, settings.DB_ACCESS)

    result = []
    if last_time[0] == None:
        last_time[0] = [datetime(1970, 2, 1, 0, 0), datetime(1970, 2, 1, 0, 0)]
    if int(round(last_time[0][0].timestamp())) != from_dt or int(round(last_time[0][1].timestamp())) != to_dt:
        print("Даты разные. Запись...")
        await data_truncate('car_travel_time', settings.DB_USERS, settings.DB_ACCESS)
        for x in cars['result']['rows']:
            params = "?version=4&car_id=" + str(x['asuods_id']) + "&from_dt=" + str(from_dt) + "&to_dt=" + str(to_dt)
            psd_track = await psd_track_get(token, params)
            if psd_track['travel_time'] == None:
                psd_track['travel_time'] = 0
            if psd_track['distance'] == None:
                psd_track['distance'] = 0
            result.append((x['gov_number'],
                           round(psd_track['distance'] / 1000, 2),
                           time.strftime('%H:%M:%S',
                           time.gmtime(psd_track['travel_time'])),
                           datetime.fromtimestamp(from_dt).isoformat(),
                           datetime.fromtimestamp(to_dt).isoformat()))

        return result
    else:
        print("Даты совпадают")
        pass

async def main():
    start = time.time()

    login = settings.CHECKOUT_ACCESS['SAO']['login']
    password = settings.CHECKOUT_ACCESS['SAO']['password']
    car_time_table = 'car_travel_time'
    car_time_column = 'gov_number, distance, travel_time, from_dt, to_dt'

    token = await token_get(login, password)
    data = await ts_ets(token)
    if data != None:
        await data_post(data, car_time_table, car_time_column, settings.DB_USERS, settings.DB_ACCESS)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end-start)))

if __name__ == "__main__":
    asyncio.run(main())
