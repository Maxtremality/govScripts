from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from datetime import datetime
from openpyxl import load_workbook
from datetime import timedelta
import time
import asyncio
import os
import sys
import traceback

import settings
from db import data_post
from db import data_delete


async def remove_file(obj: str):
    if os.path.isfile(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx') is True:
        os.remove(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')


async def rename_file(obj: str):
    if os.path.isfile(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_отчет.xlsx') is True:
        os.rename(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_отчет.xlsx', os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')


async def remove_rows(obj: str):
    wb = load_workbook(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')
    ws = wb.active
    for merge in list(ws.merged_cells):
        ws.unmerge_cells(range_string=str(merge))
    ws.delete_rows(1, 5)
    ws.delete_rows(ws.max_row, 50)
    ws.delete_cols(9, 1)
    ws.delete_cols(31, 1)
    wb.save('Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')


async def null_rows(obj: str):
    wb = load_workbook(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')
    ws = wb.active
    for row in range(1, ws.max_row+1):
        ws.cell(row=row, column=8).value = 'cat0'

    for row in range(1, ws.max_row+1):
        for col in range(15, 30):
            ws.cell(row=row, column=col).value = 0.0

    wb.save('Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')


async def get_file(obj: str, date: str):
    options = Options()
    options.add_argument('--headless')
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", os.getcwd())
    driver = webdriver.Firefox(options=options)
    timeout = 30
    sleep = 35
    do = True
    i = 1

    while do:
        if i > 3:
            os.execl(sys.executable, sys.executable, * sys.argv)
        if obj == 'ПЧ':
            try:
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'Попытка #' + str(i))
                driver.set_page_load_timeout(timeout)
                driver.get('http://185.173.2.48/analytics/saw.dll?bipublisherEntry&Action=open&itemType=.xdo&bipPath=/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo&bipParams={"_xmode":"4","_xpf":"","_xpt":"0","_xdo":"/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo","_paramsp_target_date":"' + date + '","xdo:xdo:_paramsp_subject_contract_div_input":"ОДХ","_paramsp_subject_contract":"1","xdo:xdo:_paramsp_element_div_input":"Проезжая часть","_paramsp_element":"1","xdo:xdo:_paramsp_customer_id_div_input":"Все","_paramsp_customer_id":"*","_xt":"отчет","_xf":"xlsx","_xautorun":"false"}&NQUser=ASBURMISTROV&NQPassword=ASBURMISTROV123')
                time.sleep(sleep)
                driver.close()
                driver.quit()
                await rename_file('ПЧ')
                await remove_rows('ПЧ')
                do = False
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'файл создан')
            except:
                print('Ошибка:\n', traceback.format_exc())
                do = True
                i += 1
                timeout += 10
                sleep += 10
                continue

        elif obj == 'Тротуары':
            try:
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'Попытка #' + str(i))
                driver.set_page_load_timeout(timeout)
                driver.get('http://185.173.2.48/analytics/saw.dll?bipublisherEntry&Action=open&itemType=.xdo&bipPath=/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo&bipParams={"_xmode":"4","_xpf":"1","_xpt":"0","_xdo":"/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo","_paramsp_target_date":"' + date + '","xdo:xdo:_paramsp_subject_contract_div_input":"ОДХ","_paramsp_subject_contract":"1","xdo:xdo:_paramsp_element_div_input":"Тротуар","_paramsp_element":"2","xdo:xdo:_paramsp_customer_id_div_input":"Все","_paramsp_customer_id":"*","_xt":"отчет","_xf":"xlsx","_xautorun":"false"}&NQUser=ASBURMISTROV&NQPassword=ASBURMISTROV123')
                time.sleep(sleep)
                driver.close()
                driver.quit()
                await rename_file('Тротуары')
                await remove_rows('Тротуары')
                do = False
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'файл создан')
            except:
                print('Ошибка:\n', traceback.format_exc())
                do = True
                i += 1
                timeout += 10
                sleep += 10
                continue

        elif obj == 'Дворы':
            try:
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'Попытка #' + str(i))
                driver.set_page_load_timeout(timeout)
                driver.get('http://185.173.2.48:80/analytics/saw.dll?bipublisherEntry&Action=open&itemType=.xdo&bipPath=/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo&bipParams={"_xmode":"4","_xpf":"1","_xpt":"0","_xdo":"/Publisher/Монитор/Посещение объектов/Отчет Посещение объектов уборочной техникой по заказчику учредителю.xdo","_paramsp_target_date":"' + date + '","xdo:xdo:_paramsp_subject_contract_div_input":"ДТ","_paramsp_subject_contract":"2","xdo:xdo:_paramsp_element_div_input":"Все","_paramsp_element":"*","xdo:xdo:_paramsp_customer_id_div_input":"Все","_paramsp_customer_id":"*","_xt":"отчет","_xf":"xlsx","_xautorun":"false"}&NQUser=ASBURMISTROV&NQPassword=ASBURMISTROV123')
                time.sleep(sleep)
                driver.close()
                driver.quit()
                await rename_file('Дворы')
                await remove_rows('Дворы')
                await null_rows('Дворы')
                do = False
                print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', obj, 'файл создан')
            except:
                print('Ошибка:\n', traceback.format_exc())
                do = True
                i += 1
                timeout += 10
                sleep += 10
                continue


async def data_to_list(obj: str):
    wb = load_workbook(os.getcwd() + '\Отчет Посещение объектов уборочной техникой по заказчику учредителю_' + obj + '.xlsx')
    ws = wb.active
    data = list(ws.iter_rows(values_only=True))

    return data


async def data_remove(table_name: str, date: str):
    params = 'WHERE TIMESTAMPDIFF(DAY, date, "' + str(datetime.now().date()) + '") > 7 OR date = "' + str((datetime.now()-timedelta(hours=9)).date()) + '"'
    await data_delete(table_name, params, settings.DB_USERS, settings.DB_ACCESS)
    print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', 'Лишние данные из', table_name, 'удалены')


async def main():
    start = time.time()
    date = (datetime.now()-timedelta(hours=9)).strftime('%d-%m-%Y')
    roadway_table_name = 'oracle_monitor_roadway'
    footway_table_name = 'oracle_monitor_footway'
    dt_table_name = 'oracle_monitor_dt'
    column = 'datetime_upload, date, object_id, name, okrug, customer_id, customer, contractor_id, contractor, \
                         category, area, fact_traveled_exec, norm_traveled_exec, traveled_area, plan_area, procent, \
                         area_distance, area_gutter, fact_distance_gutter_exec, fact_distance_exec, fact_gutter_exec, \
                         norm_distance_exec, norm_gutter_exec, norm_distance_gutter_exec, plan_distance_exec, plan_gutter_exec, \
                         plan_distance_gutter_exec, traveled_area_distance, traveled_area_gutter, procent_distance_gutter, \
                         procent_all, status'

    # Проезжая часть
    await remove_file('ПЧ')
    await get_file('ПЧ', date)
    roadway_data = await data_to_list('ПЧ')
    roadway = []
    roadway_check = False
    if roadway_data != []:
        roadway_check = True
    for r in roadway_data:
        data = (datetime.now().isoformat(), datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d'), *r)
        roadway.append(data)
    if roadway_check is True:
        await data_remove(roadway_table_name, date)
        await data_post(roadway, roadway_table_name, column, settings.DB_USERS, settings.DB_ACCESS)
        print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', 'Данные по проезжей части загружены')

    # Тротуары
    await remove_file('Тротуары')
    await get_file('Тротуары', date)
    footway_data = await data_to_list('Тротуары')
    footway = []
    footway_check = False
    if footway_data != []:
        footway_check = True
    for f in footway_data:
        data = (datetime.now().isoformat(), datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d'), *f)
        footway.append(data)
    if footway_check is True:
        await data_remove(footway_table_name, date)
        await data_post(footway, footway_table_name, column, settings.DB_USERS, settings.DB_ACCESS)
        print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', 'Данные по тротуарам загружены')

    # Дворы
    await remove_file('Дворы')
    await get_file('Дворы', date)
    dt_data = await data_to_list('Дворы')
    dt = []
    dt_check = False
    if dt_data != [(None, None, None, None, None, None, None, 'cat0', None, None, None, None, None, None, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)]:
        dt_check = True
    for d in dt_data:
        data = (datetime.now().isoformat(), datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d'), *d)
        dt.append(data)
    if dt_check is True:
        await data_remove(dt_table_name, date)
        await data_post(dt, dt_table_name, column, settings.DB_USERS, settings.DB_ACCESS)
        print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'), '|', 'Данные по дворам загружены')

    await remove_file('ПЧ')
    await remove_file('Тротуары')
    await remove_file('Дворы')
    end = time.time()
    print('Время выполнения:', time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
