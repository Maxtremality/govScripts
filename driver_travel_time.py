import asyncio
import time
from datetime import datetime

import settings
from db import data_get
from db import data_post
from db import data_truncate
from ets_data import token_get
from ets_data import waybill_get
from psd_data import psd_track_get


async def waybills_get(token, date_start, date_end):
    params = '?offset=0&sort_by=number:desc&filter={"date_create__gt":"' + date_start + '","closing_date__lt":"' + date_end + '"}'
    waybills = await waybill_get(token, params)
    waybill = {}
    for waybill_data in waybills['result']:
        try:
            waybill[waybill_data['driver_id']]['waybill_data'].append({"okrug_name": waybill_data['okrug_name'], "company_name": waybill_data['company_name'], "car_id": waybill_data['car_id'], "track_length_km": waybill_data['track_length_km'], "activating_date": waybill_data['activating_date'], "closing_date": waybill_data['closing_date']})
        except:
            waybill[waybill_data['driver_id']] = {"driver_name": waybill_data['driver_name'], "waybill_data": [{"okrug_name": waybill_data['okrug_name'], "company_name": waybill_data['company_name'], "car_id": waybill_data['car_id'], "track_length_km": waybill_data['track_length_km'], "activating_date": waybill_data['activating_date'], "closing_date": waybill_data['closing_date']}]}

    return waybill


async def time_get(token, waybill):
    data = {}

    for driver in waybill:

        distance = 0.0
        travel_time = 0.0
        parkings_time_base = 0.0
        parkings_time_city = 0.0
        waybill_count = 0
        waybill_duty = 0
        driver_name = waybill[driver]['driver_name']

        for waybill_data in waybill[driver]['waybill_data']:

            if waybill_data['track_length_km'] == 0 or waybill_data['track_length_km'] == None:
                waybill_duty += 1

            params = "?version=4&car_id=" + str(waybill_data['car_id']) + "&from_dt=" + str(int(datetime.fromisoformat(waybill_data['activating_date']).timestamp())) + "&to_dt=" + str(int(datetime.fromisoformat(waybill_data['closing_date']).timestamp()))
            psd_track = await psd_track_get(token, params)
            okrug_name = waybill_data['okrug_name']
            company_name = waybill_data['company_name']
            waybill_count += 1
            try:
                distance += psd_track['distance'] / 1000
                travel_time += psd_track['travel_time']
                for parking in psd_track['parkings']:
                    for base in settings.BASE_COORDS:
                        if base[0] - 100 < parking['end_point']['coords_msk'][0] < base[0] + 100 and base[1] - 100 < parking['end_point']['coords_msk'][1] < base[1] + 100:
                            parkings_time_base += parking['sec']
                            break
                    else:
                        parkings_time_city += parking['sec']
            except TypeError:
                distance += 0.0
                travel_time += 0.0
                parkings_time_base += 0.0
                parkings_time_city += 0.0

        data[driver] = {"okrug_name": okrug_name, "company_name": company_name, "driver_name": driver_name, "distance": distance, "travel_time": travel_time, 'parkings_time_base': parkings_time_base, 'parkings_time_city': parkings_time_city, "waybill_count": waybill_count, "waybill_duty": waybill_duty}

    return data


async def report_get(time_report):
    report = []
    for x in time_report:
        report.append((time_report[x]['okrug_name'], time_report[x]['company_name'], time_report[x]['driver_name'], time_report[x]['distance'], time_report[x]['travel_time'], time_report[x]['parkings_time_base'], time_report[x]['parkings_time_city'], time_report[x]['waybill_count'], time_report[x]['waybill_duty']))

    return report


async def main():
    start = time.time()
    print(datetime.now())

    login = settings.TRAVEL_TIME_ACCESS['avdSAO']['login']
    password = settings.TRAVEL_TIME_ACCESS['avdSAO']['password']
    date_php = await data_get('date_start, date_end', 'driver_travel_time_php', '', settings.DB_USERS, settings.DB_ACCESS)
    date_start = date_php[0][0]
    date_end = date_php[0][1]
    driver_time_table = 'driver_travel_time'
    driver_time_column = 'okrug_name, company_name, driver_name, distance, travel_time, parkings_time_base, parkings_time_city, waybill_count, waybill_duty'

    token = await token_get(login, password)
    waybill = await waybills_get(token, date_start, date_end)
    time_report = await time_get(token, waybill)
    report = await report_get(time_report)
    await data_truncate(driver_time_table, settings.DB_USERS, settings.DB_ACCESS)
    await data_post(report, driver_time_table, driver_time_column, settings.DB_USERS, settings.DB_ACCESS)

    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
