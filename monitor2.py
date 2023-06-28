import json
import asyncio
from datetime import datetime, timedelta
import time

from ets_data import token_get
from ets_data import company_get
from ets_data import orders_get
from ets_data import roads_get
from ets_data import dts_get
from ets_data import mission_list_get
from ets_data import mission_data_get
from db import data_get
from db import data_post
from db import data_truncate
import settings


async def orders_number_get(orders):
    orders_number = []
    for order in orders['result']:
        orders_number.append(order['order_number'])

    return orders_number


async def mission_get(token, mission_list):
    missions = {}

    for mission in mission_list:
        mission_report = await mission_data_get(token, str(mission))
        car_data = mission_report['result']['car_data']
        report_data = mission_report['result']['report_data']
        mission_data = mission_report['result']['mission_data']
        technical_operation_data = mission_report['result']['technical_operation_data']
        if 'entries' in report_data:
            missions[mission] = {'car_data': car_data, 'report_data': report_data, 'mission_data': mission_data, 'technical_operation_data': technical_operation_data}

    return missions


async def technical_operations_calc(orders):
    roadway_wash = {}               # Мойка проезжей части
    footway_wash = {}               # Мойка тротуара
    dt_wash = {}                    # Мойка двора
    roadway_sweep = {}              # Подметание проезжей части
    footway_sweep = {}              # Подметание тротуара
    gutter_sweep = {}               # Подметание лотковой части
    distance_sweep = {}             # Подметание осевой
    dt_sweep = {}                   # Подметание двора
    roadway_treatment_LAIM = {}     # Обработка проезжей части жидким ПГМ
    roadway_treatment_SAIM = {}     # Обработка проезжей части твердым ПГМ
    footway_treatment_CAIM = {}     # Обработка тротуаров комбинированным ПГМ

    for order_data in orders['result']:
        for technical_operations in order_data['technical_operations']:
            if technical_operations['id'] == 216 and technical_operations['municipal_facility_id'] == 1:        # roadway_wash - Мойка проезжей части
                if roadway_wash == {}:
                    roadway_wash = technical_operations
                else:
                    roadway_wash['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 216 and technical_operations['municipal_facility_id'] == 2:        # footway_wash - Мойка тротуара
                if footway_wash == {}:
                    footway_wash = technical_operations
                else:
                    footway_wash['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 216 and technical_operations['municipal_facility_id'] == 18:       # dt_wash - Мойка двора
                if dt_wash == {}:
                    dt_wash = technical_operations
                else:
                    dt_wash['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 204 and technical_operations['municipal_facility_id'] == 1:        # roadway_sweep - Подметание проезжей части
                if roadway_sweep == {}:
                    roadway_sweep = technical_operations
                else:
                    roadway_sweep['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 204 and technical_operations['municipal_facility_id'] == 2:        # footway_sweep - Подметание тротуара
                if footway_sweep == {}:
                    footway_sweep = technical_operations
                else:
                    footway_sweep['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 204 and technical_operations['municipal_facility_id'] == 3:        # gutter_sweep - Подметание лотковой части
                if gutter_sweep == {}:
                    gutter_sweep = technical_operations
                else:
                    gutter_sweep['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 204 and technical_operations['municipal_facility_id'] == 6:        # distance_sweep - Подметание осевой
                if distance_sweep == {}:
                    distance_sweep = technical_operations
                else:
                    distance_sweep['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 204 and technical_operations['municipal_facility_id'] == 18:       # dt_sweep - Подметание двора
                if dt_sweep == {}:
                    dt_sweep = technical_operations
                else:
                    dt_sweep['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 199 and technical_operations['municipal_facility_id'] == 1:        # roadway_treatment_LAIM - Обработка проезжей части жидким ПГМ
                if roadway_treatment_LAIM == {}:
                    roadway_treatment_LAIM = technical_operations
                else:
                    roadway_treatment_LAIM['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 201 and technical_operations['municipal_facility_id'] == 1:        # roadway_treatment_SAIM - Обработка проезжей части твердым ПГМ
                if roadway_treatment_SAIM == {}:
                    roadway_treatment_SAIM = technical_operations
                else:
                    roadway_treatment_SAIM['num_exec'] += technical_operations['num_exec']
            elif technical_operations['id'] == 197 and technical_operations['municipal_facility_id'] == 2:        # footway_treatment_CAIM - Обработка тротуаров комбинированным ПГМ
                if footway_treatment_CAIM == {}:
                    footway_treatment_CAIM = technical_operations
                else:
                    footway_treatment_CAIM['num_exec'] += technical_operations['num_exec']

    return {"roadway_wash": roadway_wash, "footway_wash": footway_wash, "dt_wash": dt_wash,"roadway_sweep": roadway_sweep, "footway_sweep": footway_sweep,
            "gutter_sweep": gutter_sweep, "distance_sweep": distance_sweep, "dt_sweep": dt_sweep,
            "roadway_treatment_LAIM": roadway_treatment_LAIM, "roadway_treatment_SAIM": roadway_treatment_SAIM,
            "footway_treatment_CAIM": footway_treatment_CAIM}


async def roads_area_calc(roads, company_list, technical_operations):
    roads_area = {}
    for company in company_list:
        for road in roads['result']['rows']:
            if company['company_id'] == road['company_id']:
                roads_area[road['id']] = {"okrug_id": road['okrug_id'], "okrug_name": road['okrug_name'], "company_id": road['company_id'],
                                          "company_name": road['company_name'], "name": road['name'], "distance": road['distance'], "structure_name": road['company_structures_text'],
                                          "auto_footway_area": road['auto_footway_area'], "roadway_area": road['roadway_area'], "gutters_length": road['gutters_length'],
                                          "roadway_wash": road['roadway_area'] * (0 if technical_operations['roadway_wash'] == {} else technical_operations['roadway_wash']['num_exec']),
                                          "footway_wash": road['auto_footway_area'] * (0 if technical_operations['footway_wash'] == {} else technical_operations['footway_wash']['num_exec']),
                                          "roadway_sweep": road['roadway_area'] * (0 if technical_operations['roadway_sweep'] == {} else technical_operations['roadway_sweep']['num_exec']),
                                          "footway_sweep": road['auto_footway_area'] * (0 if technical_operations['footway_sweep'] == {} else technical_operations['footway_sweep']['num_exec']),
                                          "gutter_sweep": road['gutters_length'] * (0 if technical_operations['gutter_sweep'] == {} else technical_operations['gutter_sweep']['num_exec']),
                                          "distance_sweep": road['distance'] * (0 if technical_operations['distance_sweep'] == {} else technical_operations['distance_sweep']['num_exec']),
                                          "roadway_treatment_LAIM": road['roadway_area'] * (0 if technical_operations['roadway_treatment_LAIM'] == {} else technical_operations['roadway_treatment_LAIM']['num_exec']),
                                          "roadway_treatment_SAIM": road['roadway_area'] * (0 if technical_operations['roadway_treatment_SAIM'] == {} else technical_operations['roadway_treatment_SAIM']['num_exec']),
                                          "footway_treatment_CAIM": road['auto_footway_area'] * (0 if technical_operations['footway_treatment_CAIM'] == {} else technical_operations['footway_treatment_CAIM']['num_exec'])}

    return roads_area


async def dt_area_calc(dts, company_list, technical_operations):
    dt_area = {}

    for company in company_list:
        for dt in dts['result']['rows']:
            if company['company_id'] == dt['company_id']:
                dt_area[dt['id']] = {"okrug_id": dt['okrug_id'], "okrug_name": dt['okrug_name'], "company_id": dt['company_id'],
                                     "company_name": dt['company_name'], "name": dt['name'], "mechanical_clean_area": dt['mechanical_clean_area'],
                                     "dt_wash": dt['mechanical_clean_area'] * (0 if technical_operations['dt_wash'] == {} else technical_operations['dt_wash']['num_exec']),
                                     "dt_sweep": dt['mechanical_clean_area'] * (0 if technical_operations['dt_sweep'] == {} else technical_operations['dt_sweep']['num_exec'])}

    return dt_area


async def traveled_normalized_calculation(mission_data):
    roadway_wash = {}                   # Мойка проезжей части
    footway_wash = {}                   # Мойка тротуара
    dt_wash = {}                        # Мойка двора
    roadway_sweep = {}                  # Подметание проезжей части
    footway_sweep = {}                  # Подметание тротуара
    gutter_sweep = {}                   # Подметание лотковой части
    distance_sweep = {}                 # Подметание осевой
    dt_sweep = {}                       # Подметание двора
    roadway_treatment_LAIM = {}         # Обработка проезжей части жидким ПГМ
    roadway_treatment_SAIM = {}         # Обработка проезжей части твердым ПГМ
    footway_treatment_CAIM = {}         # Обработка тротуаров комбинированным ПГМ

    f = 1
    for number in mission_data:

        norm = {}

        if mission_data[number]['technical_operation_data']['id'] == 216 and mission_data[number]['mission_data']['element_id'] == 1:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    roadway_wash[m] += norm[m]
                except KeyError:
                    roadway_wash[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 216 and mission_data[number]['mission_data']['element_id'] == 2:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    footway_wash[m] += norm[m]
                except KeyError:
                    footway_wash[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 216 and mission_data[number]['mission_data']['element_id'] == 18:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    dt_wash[m] += norm[m]
                except KeyError:
                    dt_wash[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 204 and mission_data[number]['mission_data']['element_id'] == 1:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    roadway_sweep[m] += norm[m]
                except KeyError:
                    roadway_sweep[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 204 and mission_data[number]['mission_data']['element_id'] == 2:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    footway_sweep[m] += norm[m]
                except KeyError:
                    footway_sweep[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 204 and mission_data[number]['mission_data']['element_id'] == 3:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    gutter_sweep[m] += norm[m]
                except KeyError:
                    gutter_sweep[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 204 and mission_data[number]['mission_data']['element_id'] == 6:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    distance_sweep[m] += norm[m]
                except KeyError:
                    distance_sweep[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 204 and mission_data[number]['mission_data']['element_id'] == 18:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    dt_sweep[m] += norm[m]
                except KeyError:
                    dt_sweep[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 199 and mission_data[number]['mission_data']['element_id'] == 1:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    roadway_treatment_LAIM[m] += norm[m]
                except KeyError:
                    roadway_treatment_LAIM[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 201 and mission_data[number]['mission_data']['element_id'] == 1:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    roadway_treatment_SAIM[m] += norm[m]
                except KeyError:
                    roadway_treatment_SAIM[m] = norm[m]
        if mission_data[number]['technical_operation_data']['id'] == 197 and mission_data[number]['mission_data']['element_id'] == 2:
            for x in mission_data[number]['report_data']['entries']:
                try:
                    norm[x['object_id']] += x['traveled_normalized']
                except KeyError:
                    norm[x['object_id']] = x['traveled_normalized']
            for m in norm:
                try:
                    footway_treatment_CAIM[m] += norm[m]
                except KeyError:
                    footway_treatment_CAIM[m] = norm[m]

    return {"roadway_wash": roadway_wash, "footway_wash": footway_wash, "dt_wash": dt_wash,
            "roadway_sweep": roadway_sweep, "footway_sweep": footway_sweep, "gutter_sweep": gutter_sweep, "distance_sweep": distance_sweep,
            "dt_sweep": dt_sweep, "roadway_treatment_LAIM": roadway_treatment_LAIM, "roadway_treatment_SAIM": roadway_treatment_SAIM,
            "footway_treatment_CAIM": footway_treatment_CAIM}


async def object_calculation(roads_area, dt_area, traveled_normalized, mission_data, orders):
    def roadway_wash_calc():
        roadway_wash = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 1 and mission_data[mission]['technical_operation_data']['id'] == 216:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next((round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['roadway_wash'] and roads_area[road]['roadway_wash'] > 0:
                if traveled_normalized['roadway_wash'][road] > roads_area[road]['roadway_wash']:
                    traveled_normalized['roadway_wash'][road] = roads_area[road]['roadway_wash']
                roadway_wash[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': round(traveled_normalized['roadway_wash'][road] * 100 / roads_area[road]['roadway_wash'], 2),
                                      'traveled_normalized': traveled_normalized['roadway_wash'][road],
                                      'road_area': roads_area[road]['roadway_wash'],
                                      'missions': missions,
                                      'num_exec': 1}
            else:
                roadway_wash[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': 0.0,
                                      'traveled_normalized': 0.0,
                                      'road_area': roads_area[road]['roadway_wash'],
                                      'missions': missions,
                                      'num_exec': 0}
        return roadway_wash

    def roadway_sweep_calc():
        roadway_sweep = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 1 and mission_data[mission]['technical_operation_data']['id'] == 204:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next(
                                             (round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for
                                              entry in mission_data[mission]['report_data']['entries'] if
                                              entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['roadway_sweep'] and roads_area[road]['roadway_sweep'] > 0:
                if traveled_normalized['roadway_sweep'][road] > roads_area[road]['roadway_sweep']:
                    traveled_normalized['roadway_sweep'][road] = roads_area[road]['roadway_sweep']
                roadway_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': round(traveled_normalized['roadway_sweep'][road] * 100 / roads_area[road]['roadway_sweep'], 2),
                                      'traveled_normalized': traveled_normalized['roadway_sweep'][road],
                                      'road_area': roads_area[road]['roadway_sweep'],
                                      'missions': missions,
                                      'num_exec': 1}
            else:
                roadway_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': 0.0,
                                      'traveled_normalized': 0.0,
                                      'road_area': roads_area[road]['roadway_sweep'],
                                      'missions': missions,
                                      'num_exec': 0}
        return roadway_sweep
    
    def gutter_sweep_calc():
        gutter_sweep = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 3 and mission_data[mission]['technical_operation_data']['id'] == 204:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next((round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['gutter_sweep'] and roads_area[road]['gutter_sweep'] > 0:
                if traveled_normalized['gutter_sweep'][road] > roads_area[road]['gutter_sweep']:
                    traveled_normalized['gutter_sweep'][road] = roads_area[road]['gutter_sweep']
                gutter_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': round(traveled_normalized['gutter_sweep'][road] * 100 / roads_area[road]['gutter_sweep'], 2),
                                      'traveled_normalized': traveled_normalized['gutter_sweep'][road],
                                      'road_area': roads_area[road]['gutter_sweep'],
                                      'missions': missions,
                                      'num_exec': 1}
            else:
                gutter_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                      'okrug_name': roads_area[road]['okrug_name'],
                                      'company_id': roads_area[road]['company_id'],
                                      'company_name': roads_area[road]['company_name'],
                                      'name': roads_area[road]['name'],
                                      'procent': 0.0,
                                      'traveled_normalized': 0.0,
                                      'road_area': roads_area[road]['gutter_sweep'],
                                      'missions': missions,
                                      'num_exec': 0}
        return gutter_sweep

    def distance_sweep_calc():
        distance_sweep = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 6 and mission_data[mission]['technical_operation_data']['id'] == 204:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next((round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['distance_sweep'] and roads_area[road]['distance_sweep'] > 0:
                if traveled_normalized['distance_sweep'][road] > roads_area[road]['distance_sweep']:
                    traveled_normalized['distance_sweep'][road] = roads_area[road]['distance_sweep']
                distance_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                        'okrug_name': roads_area[road]['okrug_name'],
                                        'company_id': roads_area[road]['company_id'],
                                        'company_name': roads_area[road]['company_name'],
                                        'name': roads_area[road]['name'],
                                        'procent': round(traveled_normalized['distance_sweep'][road] * 100 / roads_area[road]['distance_sweep'], 2),
                                        'traveled_normalized': traveled_normalized['distance_sweep'][road],
                                        'road_area': roads_area[road]['distance_sweep'],
                                        'missions': missions,
                                        'num_exec': 1}
            else:
                distance_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                        'okrug_name': roads_area[road]['okrug_name'],
                                        'company_id': roads_area[road]['company_id'],
                                        'company_name': roads_area[road]['company_name'],
                                        'name': roads_area[road]['name'],
                                        'procent': 0.0,
                                        'traveled_normalized': 0.0,
                                        'road_area': roads_area[road]['distance_sweep'],
                                        'missions': missions,
                                        'num_exec': 0}
        return distance_sweep

    def roadway_treatment_LAIM_calc():
        roadway_treatment_LAIM = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 1 and mission_data[mission]['technical_operation_data']['id'] == 199:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next(
                                             (round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for
                                              entry in mission_data[mission]['report_data']['entries'] if
                                              entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['roadway_treatment_LAIM'] and roads_area[road]['roadway_treatment_LAIM'] > 0:
                if traveled_normalized['roadway_treatment_LAIM'][road] > roads_area[road]['roadway_treatment_LAIM']:
                    traveled_normalized['roadway_treatment_LAIM'][road] = roads_area[road]['roadway_treatment_LAIM']
                roadway_treatment_LAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': round(traveled_normalized['roadway_treatment_LAIM'][road] * 100 / roads_area[road]['roadway_treatment_LAIM'], 2),
                                                'traveled_normalized': traveled_normalized['roadway_treatment_LAIM'][road],
                                                'road_area': roads_area[road]['roadway_treatment_LAIM'],
                                                'missions': missions,
                                                'num_exec': 1}
            else:
                roadway_treatment_LAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': 0.0,
                                                'traveled_normalized': 0.0,
                                                'road_area': roads_area[road]['roadway_treatment_LAIM'],
                                                'missions': missions,
                                                'num_exec': 0}
        return roadway_treatment_LAIM

    def roadway_treatment_SAIM_calc():
        roadway_treatment_SAIM = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 1 and mission_data[mission]['technical_operation_data']['id'] == 201:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next(
                                             (round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for
                                              entry in mission_data[mission]['report_data']['entries'] if
                                              entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['roadway_treatment_SAIM'] and roads_area[road]['roadway_treatment_SAIM'] > 0:
                if traveled_normalized['roadway_treatment_SAIM'][road] > roads_area[road]['roadway_treatment_SAIM']:
                    traveled_normalized['roadway_treatment_SAIM'][road] = roads_area[road]['roadway_treatment_SAIM']
                roadway_treatment_SAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': round(traveled_normalized['roadway_treatment_SAIM'][road] * 100 / roads_area[road]['roadway_treatment_SAIM'], 2),
                                                'traveled_normalized': traveled_normalized['roadway_treatment_SAIM'][road],
                                                'road_area': roads_area[road]['roadway_treatment_SAIM'],
                                                'missions': missions,
                                                'num_exec': 1}
            else:
                roadway_treatment_SAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': 0.0,
                                                'traveled_normalized': 0.0,
                                                'road_area': roads_area[road]['roadway_treatment_SAIM'],
                                                'missions': missions,
                                                'num_exec': 0}
        return roadway_treatment_SAIM

    def footway_treatment_CAIM_calc():
        footway_treatment_CAIM = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 2 and mission_data[mission]['technical_operation_data']['id'] == 197:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next(
                                             (round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for
                                              entry in mission_data[mission]['report_data']['entries'] if
                                              entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(
                                             mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['footway_treatment_CAIM'] and roads_area[road]['footway_treatment_CAIM'] > 0:
                if traveled_normalized['footway_treatment_CAIM'][road] > roads_area[road]['footway_treatment_CAIM']:
                    traveled_normalized['footway_treatment_CAIM'][road] = roads_area[road]['footway_treatment_CAIM']
                footway_treatment_CAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': round(traveled_normalized['footway_treatment_CAIM'][road] * 100 / roads_area[road]['footway_treatment_CAIM'], 2),
                                                'missions': missions,
                                                'num_exec': 1}
            else:
                footway_treatment_CAIM[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': 0.0,
                                                'missions': missions,
                                                'num_exec': 0}
        return footway_treatment_CAIM

    def footway_wash_calc():
        footway_wash = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 2 and mission_data[mission]['technical_operation_data']['id'] == 216:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next((round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['footway_wash'] and roads_area[road]['footway_wash'] > 0:
                if traveled_normalized['footway_wash'][road] > roads_area[road]['footway_wash']:
                    traveled_normalized['footway_wash'][road] = roads_area[road]['footway_wash']
                footway_wash[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': round(traveled_normalized['footway_wash'][road] * 100 / roads_area[road]['footway_wash'], 2),
                                                'missions': missions,
                                                'num_exec': 1}
            else:
                footway_wash[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': 0.0,
                                                'missions': missions,
                                                'num_exec': 0}
        return footway_wash

    def footway_sweep_calc():
        footway_sweep = {}
        for road in roads_area:
            missions = []
            for mission in mission_data:
                if next((entry for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None) is not None \
                        and mission_data[mission]['mission_data']['element_id'] == 2 and mission_data[mission]['technical_operation_data']['id'] == 204:
                    try:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': next((round(entry['traveled_normalized'] / entry['check_value'] * 100, 2) for entry in mission_data[mission]['report_data']['entries'] if entry.get('object_id') == road), None),
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': ''})
                    except ZeroDivisionError:
                        missions.append({'order_number': mission_data[mission]['mission_data']['order_number'],
                                         'mission_id': mission_data[mission]['mission_data']['id'],
                                         'gov_number': mission_data[mission]['car_data']['gov_number'],
                                         'operation_name': mission_data[mission]['mission_data']['name'],
                                         'element': mission_data[mission]['mission_data']['element'],
                                         'procent': 0.0,
                                         'mission_url': 'https://ets.mos.ru/#/missions/missions/' + str(mission_data[mission]['mission_data']['id']) + '/info',
                                         'note': 'В задании объект с площадью уборки 0'})
            if road in traveled_normalized['footway_sweep'] and roads_area[road]['footway_sweep'] > 0:
                if traveled_normalized['footway_sweep'][road] > roads_area[road]['footway_sweep']:
                    traveled_normalized['footway_sweep'][road] = roads_area[road]['footway_sweep']
                footway_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': round(traveled_normalized['footway_sweep'][road] * 100 / roads_area[road]['footway_sweep'], 2),
                                                'missions': missions,
                                                'num_exec': 1}
            else:
                footway_sweep[road] = {'okrug_id': roads_area[road]['okrug_id'],
                                                'okrug_name': roads_area[road]['okrug_name'],
                                                'company_id': roads_area[road]['company_id'],
                                                'company_name': roads_area[road]['company_name'],
                                                'name': roads_area[road]['name'],
                                                'procent': 0.0,
                                                'missions': missions,
                                                'num_exec': 0}
        return footway_sweep

    def dt_wash_calc():
        pass

    def dt_sweep_calc():
        pass

    def roadway_calc():
        roadway_wash = roadway_wash_calc()
        roadway_sweep = roadway_sweep_calc()
        distance_sweep = distance_sweep_calc()
        gutter_sweep = gutter_sweep_calc()
        roadway_treatment_LAIM = roadway_treatment_LAIM_calc()
        roadway_treatment_SAIM = roadway_treatment_SAIM_calc()

        roadway_data = {}

        for object_id in roadway_wash:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += roadway_wash[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += roadway_wash[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += roadway_wash[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *roadway_wash[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += roadway_wash[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': roadway_wash[object_id]['okrug_id'],
                                           'okrug_name': roadway_wash[object_id]['okrug_name'],
                                           'company_id': roadway_wash[object_id]['company_id'],
                                           'company_name': roadway_wash[object_id]['company_name'],
                                           'name': roadway_wash[object_id]['name'],
                                           'procent': roadway_wash[object_id]['procent'],
                                           'traveled_normalized': roadway_wash[object_id]['traveled_normalized'],
                                           'road_area': roadway_wash[object_id]['road_area'],
                                           'missions': roadway_wash[object_id]['missions'],
                                           'num_exec': roadway_wash[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}
        
        for object_id in roadway_sweep:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += roadway_sweep[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += roadway_sweep[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += roadway_sweep[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *roadway_sweep[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += roadway_sweep[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': roadway_sweep[object_id]['okrug_id'],
                                           'okrug_name': roadway_sweep[object_id]['okrug_name'],
                                           'company_id': roadway_sweep[object_id]['company_id'],
                                           'company_name': roadway_sweep[object_id]['company_name'],
                                           'name': roadway_sweep[object_id]['name'],
                                           'procent': roadway_sweep[object_id]['procent'],
                                           'traveled_normalized': roadway_sweep[object_id]['traveled_normalized'],
                                           'road_area': roadway_sweep[object_id]['road_area'],
                                           'missions': roadway_sweep[object_id]['missions'],
                                           'num_exec': roadway_sweep[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}
        
        for object_id in distance_sweep:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += distance_sweep[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += distance_sweep[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += distance_sweep[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *distance_sweep[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += distance_sweep[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': distance_sweep[object_id]['okrug_id'],
                                           'okrug_name': distance_sweep[object_id]['okrug_name'],
                                           'company_id': distance_sweep[object_id]['company_id'],
                                           'company_name': distance_sweep[object_id]['company_name'],
                                           'name': distance_sweep[object_id]['name'],
                                           'procent': distance_sweep[object_id]['procent'],
                                           'traveled_normalized': distance_sweep[object_id]['traveled_normalized'],
                                           'road_area': distance_sweep[object_id]['road_area'],
                                           'missions': distance_sweep[object_id]['missions'],
                                           'num_exec': distance_sweep[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}

        for object_id in gutter_sweep:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += gutter_sweep[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += gutter_sweep[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += gutter_sweep[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *gutter_sweep[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += gutter_sweep[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': gutter_sweep[object_id]['okrug_id'],
                                           'okrug_name': gutter_sweep[object_id]['okrug_name'],
                                           'company_id': gutter_sweep[object_id]['company_id'],
                                           'company_name': gutter_sweep[object_id]['company_name'],
                                           'name': gutter_sweep[object_id]['name'],
                                           'procent': gutter_sweep[object_id]['procent'],
                                           'traveled_normalized': gutter_sweep[object_id]['traveled_normalized'],
                                           'road_area': gutter_sweep[object_id]['road_area'],
                                           'missions': gutter_sweep[object_id]['missions'],
                                           'num_exec': gutter_sweep[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}

        for object_id in roadway_treatment_LAIM:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += roadway_treatment_LAIM[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += roadway_treatment_LAIM[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += roadway_treatment_LAIM[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *roadway_treatment_LAIM[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += roadway_treatment_LAIM[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': roadway_treatment_LAIM[object_id]['okrug_id'],
                                           'okrug_name': roadway_treatment_LAIM[object_id]['okrug_name'],
                                           'company_id': roadway_treatment_LAIM[object_id]['company_id'],
                                           'company_name': roadway_treatment_LAIM[object_id]['company_name'],
                                           'name': roadway_treatment_LAIM[object_id]['name'],
                                           'procent': roadway_treatment_LAIM[object_id]['procent'],
                                           'traveled_normalized': roadway_treatment_LAIM[object_id]['traveled_normalized'],
                                           'road_area': roadway_treatment_LAIM[object_id]['road_area'],
                                           'missions': roadway_treatment_LAIM[object_id]['missions'],
                                           'num_exec': roadway_treatment_LAIM[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}

        for object_id in roadway_treatment_SAIM:
            if object_id in roadway_data:
                roadway_data[object_id]['procent'] += roadway_treatment_SAIM[object_id]['procent']
                roadway_data[object_id]['traveled_normalized'] += roadway_treatment_SAIM[object_id]['traveled_normalized']
                roadway_data[object_id]['road_area'] += roadway_treatment_SAIM[object_id]['road_area']
                roadway_data[object_id]['missions'] = [*roadway_data[object_id]['missions'], *roadway_treatment_SAIM[object_id]['missions']]
                roadway_data[object_id]['num_exec'] += roadway_treatment_SAIM[object_id]['num_exec']
            else:
                roadway_data[object_id] = {'okrug_id': roadway_treatment_SAIM[object_id]['okrug_id'],
                                           'okrug_name': roadway_treatment_SAIM[object_id]['okrug_name'],
                                           'company_id': roadway_treatment_SAIM[object_id]['company_id'],
                                           'company_name': roadway_treatment_SAIM[object_id]['company_name'],
                                           'name': roadway_treatment_SAIM[object_id]['name'],
                                           'procent': roadway_treatment_SAIM[object_id]['procent'],
                                           'traveled_normalized': roadway_treatment_SAIM[object_id]['traveled_normalized'],
                                           'road_area': roadway_treatment_SAIM[object_id]['road_area'],
                                           'missions': roadway_treatment_SAIM[object_id]['missions'],
                                           'num_exec': roadway_treatment_SAIM[object_id]['num_exec'],
                                           'structure_name': next(roads_area[id]['structure_name'] for id in roads_area if id == object_id)}

        for object_id in roadway_data:
            if roadway_data[object_id]['num_exec'] == 0:
                continue
            roadway_data[object_id]['procent'] = roadway_data[object_id]['procent'] / roadway_data[object_id]['num_exec']

        return roadway_data

    footway_wash_calc()
    footway_sweep_calc()
    footway_treatment_CAIM_calc()

    return {'roadway_data': roadway_calc()}


async def company_calculation(object_data):
    roadway = {}
    for road in object_data['roadway_data']:
        if object_data['roadway_data'][road]['company_id'] in roadway and object_data['roadway_data'][road]['num_exec'] > 0:
            roadway[object_data['roadway_data'][road]['company_id']]['procent'] += object_data['roadway_data'][road]['procent']
            roadway[object_data['roadway_data'][road]['company_id']]['traveled_normalized'] += object_data['roadway_data'][road]['traveled_normalized']
            roadway[object_data['roadway_data'][road]['company_id']]['road_area'] += object_data['roadway_data'][road]['road_area']
            roadway[object_data['roadway_data'][road]['company_id']]['num_exec'] += object_data['roadway_data'][road]['num_exec']
        elif object_data['roadway_data'][road]['num_exec'] > 0:
            roadway[object_data['roadway_data'][road]['company_id']] = {'okrug_id': object_data['roadway_data'][road]['okrug_id'],
                                                                        'okrug_name': object_data['roadway_data'][road]['okrug_name'],
                                                                        'company_id': object_data['roadway_data'][road]['company_id'],
                                                                        'company_name': object_data['roadway_data'][road]['company_name'],
                                                                        'procent': object_data['roadway_data'][road]['procent'],
                                                                        'traveled_normalized': object_data['roadway_data'][road]['traveled_normalized'],
                                                                        'road_area': object_data['roadway_data'][road]['road_area'],
                                                                        'num_exec': object_data['roadway_data'][road]['num_exec']}

    for x in roadway:
        roadway[x]['procent'] = round(roadway[x]['procent'] / roadway[x]['num_exec'], 2)

    return roadway


async def okrug_calculation():
    pass


async def main():
    start = time.time()

    table_name_roadway = []
    table_name_company = []
    table_name_okrug = []

    ets_user_table = 'ets_users'
    ets_user_columns = '*'
    roadway_column = 'okrug_id, okrug_name, company_id, company_name, name, procent, traveled_normalized, road_area, missions, num_exec, structure_name'
    company_column = 'company_name, procent, company_id, okrug_id, okrug_name'

    date_start = ('2022-12-11' + "T09:00:00")
    date_end = ('2022-12-12' + "T08:59:00")
    users = await data_get(ets_user_columns, ets_user_table, '', settings.DB_USERS, settings.DB_ACCESS)

    while True:
        for user in users:
            objects = []
            company = []

            # date = datetime.now() - timedelta(hours=9)
            # date_start = (str(date.date()) + "T09:00:00")
            # date_end = (str(date.date() + timedelta(days=1)) + "T08:59:00")

            login = user[1]
            password = user[2]
            roadway_table_pt = user[1] + '_monitor_roadway'
            company_table_pt = user[1] + '_monitor_company'
            okrug_table_pt = user[1] + '_monitor_okrug'
            if user[1] not in table_name_roadway:
                table_name_roadway.append(roadway_table_pt)
            if user[1] not in table_name_company:
                table_name_roadway.append(company_table_pt)
            if user[1] not in table_name_okrug:
                table_name_roadway.append(okrug_table_pt)

            token = await token_get(login, password)
            company_list = await company_get(token, '')
            orders = await orders_get(token, date_start, date_end)
            orders_number = await orders_number_get(orders)
            roads = await roads_get(token)
            dts = await dts_get(token)

            mission_list_params = '?is_archive=false&offset=0&filter={"status__in":["complete","in_progress","fail","expired"],"order_number__in":' + json.dumps(orders_number) + '}'
            mission_list = await mission_list_get(token, mission_list_params)
            mission_data = await mission_get(token, mission_list)
            technical_operations = await technical_operations_calc(orders)
            roads_area = await roads_area_calc(roads, company_list['result'], technical_operations)
            dt_area = await dt_area_calc(dts, company_list['result'], technical_operations)

            traveled_normalized = await traveled_normalized_calculation(mission_data)
            object_data = await object_calculation(roads_area, dt_area, traveled_normalized, mission_data, orders)
            company_data = await company_calculation(object_data)

            for object_id in object_data['roadway_data']:
                objects.append((object_data['roadway_data'][object_id]['okrug_id'],
                                object_data['roadway_data'][object_id]['okrug_name'],
                                object_data['roadway_data'][object_id]['company_id'],
                                object_data['roadway_data'][object_id]['company_name'],
                                object_data['roadway_data'][object_id]['name'],
                                round(object_data['roadway_data'][object_id]['procent'], 2),
                                object_data['roadway_data'][object_id]['traveled_normalized'],
                                object_data['roadway_data'][object_id]['road_area'],
                                json.dumps(object_data['roadway_data'][object_id]['missions'], ensure_ascii=False),
                                object_data['roadway_data'][object_id]['num_exec'],
                                object_data['roadway_data'][object_id]['structure_name']))

            for company_id in company_data:
                company.append((company_data[company_id]['company_name'],
                                company_data[company_id]['procent'],
                                company_data[company_id]['company_id'],
                                company_data[company_id]['okrug_id'],
                                company_data[company_id]['okrug_name']))

            await data_truncate(roadway_table_pt, settings.DB_DJANGO, settings.DB_ACCESS)
            await data_truncate(company_table_pt, settings.DB_DJANGO, settings.DB_ACCESS)
            await data_post(objects, roadway_table_pt, roadway_column, settings.DB_DJANGO, settings.DB_ACCESS)
            await data_post(company, company_table_pt, company_column, settings.DB_DJANGO, settings.DB_ACCESS)

            print(login, datetime.now())


    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
