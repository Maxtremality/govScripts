import asyncio
import json
import pyproj
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from db import data_post
from db import data_truncate
from ets_data import token_get
from ets_data import roads_get
from ets_data import dts_get
import settings


async def main():
    table_name = 'polygon'
    column = 'object_id, name, polygon'

    proj_6335000 = pyproj.Proj('+proj=tmerc +ellps=bessel +towgs84=316.151,78.924,589.65,-1.57273,2.69209,2.34693,8.4507 +units=m +lon_0=37.5 +lat_0=55.66666666667 +k_0=1 +x_0=0 +y_0=0')

    await data_truncate(table_name, settings.DB_USERS, settings.DB_ACCESS)

    roads = await roads_get(await token_get(settings.ALL_ACCESS['ALL']['login'], settings.ALL_ACCESS['ALL']['password']))
    dt = await dts_get(await token_get(settings.ALL_ACCESS['ALL']['login'], settings.ALL_ACCESS['ALL']['password']))
    roadway_data = []
    dt_data = []
    f = 1
    for road in roads['result']['rows']:
        object_id = road['id']
        name = road['name']
        if 'shape' in road:
            shape = json.loads(road['shape'])
            polygon_points = []
            if 'coordinates' in shape:
                for polygon_point in shape['coordinates'][0]:
                    proj_point = pyproj.transformer.transform(proj_6335000, 'WGS84', polygon_point[0], polygon_point[1])
                    polygon_x = proj_point[0]
                    polygon_y = proj_point[1]
                    polygon_points.append([polygon_x, polygon_y])
                roadway_data.append((object_id, name, str([polygon_points])))
                print("\n" * 99999)
                print(f, '/', len(roads['result']['rows']) + len(dt['result']['rows']), round(f / (len(roads['result']['rows']) + len(dt['result']['rows'])) * 100, 2), '%')
                f += 1

    await data_post(roadway_data, table_name, column, settings.DB_USERS, settings.DB_ACCESS)

    for obj in dt['result']['rows']:
        object_id = obj['id']
        name = obj['name']
        if 'shape' in obj:
            shape = json.loads(obj['shape'])
            polygon_points = []
            if 'coordinates' in shape:
                for polygon_point in shape['coordinates'][0]:
                    proj_point = pyproj.transformer.transform(proj_6335000, 'WGS84', polygon_point[0], polygon_point[1])
                    polygon_x = proj_point[0]
                    polygon_y = proj_point[1]
                    polygon_points.append([polygon_x, polygon_y])
                dt_data.append((object_id, name, str([polygon_points])))
                print("\n" * 99999)
                print(f, '/', len(roads['result']['rows']) + len(dt['result']['rows']), round(f / (len(roads['result']['rows']) + len(dt['result']['rows'])) * 100, 2), '%')
                f += 1

    await data_post(dt_data, table_name, column, settings.DB_USERS, settings.DB_ACCESS)


if __name__ == "__main__":
    asyncio.run(main())
