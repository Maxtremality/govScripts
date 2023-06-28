import json
import requests
import asyncio
import aiopg
import datetime

import settings
from db2 import data_post
from db2 import data_truncate


async def master_auth():
    url = 'https://vao.infocity.me/api/v1/auth/token'
    payload = {
        "login": "avd_vao_dir",
        "password": "12345678"
    }
    req_post = requests.post(url, json=payload)
    token = 'Bearer ' + json.loads(req_post.text)['accessToken']

    return token


async def get_file1(token):

    datethen = datetime.date.today() - datetime.timedelta(days=7)
    datenow = datetime.date.today() + datetime.timedelta(days=1)

    url = 'https://vao.infocity.me/api/v1/tasks?skip=0&take=100&requireTotalCount=true&sort=[{"selector":"createdDate","desc":true}]&filter=[["createdDate",">=","'+str(datethen)+'T21:00:00.000Z"],"and",["createdDate","<","'+str(datenow)+'T21:00:00.000Z"]]&totalSummary=[]&searchOperation="contains"'

    headers = {'Authorization': token}
    req_data = requests.get(url, headers=headers)
    master_data = json.loads(req_data.text)
    data_result = master_data['totalCount']
    ai = ((int(data_result / 100) + 1) * 100)+1
    data1 = []
    for cicle in range(0,ai,100):
        data1.append(cicle)
    return data1


async def get_file(token, df1):
    data = []
    datethen = datetime.date.today() - datetime.timedelta(days=7)
    datenow = datetime.date.today() + datetime.timedelta(days=1)
    for i in df1:
        url = 'https://vao.infocity.me/api/v1/tasks?skip='+str(i)+'&take=100&requireTotalCount=true&sort=[{"selector":"createdDate","desc":true}]&filter=[["createdDate",">=","'+str(datethen)+'T21:00:00.000Z"],"and",["createdDate","<","'+str(datenow)+'T21:00:00.000Z"]]&totalSummary=[]&searchOperation="contains"'
        headers = {'Authorization': token}
        req_data = requests.get(url, headers=headers)
        master_data = json.loads(req_data.text)
        data_result = master_data['data']
        for x in data_result:

            data.append((
            datetime.datetime.fromisoformat(str(x['createdDate'])).strftime('%Y.%m.%d %H:%M:%S'),
            datetime.datetime.fromisoformat(str(x['updatedAt'])).strftime('%Y.%m.%d %H:%M:%S'),
            datetime.datetime.fromisoformat(str(x['lifeTimeDate'])).strftime('%Y.%m.%d %H:%M:%S'),
            x['number'],
            x['userExecutor']['personFullName'],
                        x['companyUserExecutor']['name'],
                        x['serviceObject']['name'],
                        x['stage']['name'],
                        x['executorStage']['name'],
                        0 if x['moderatorStageId'] == None else x['moderatorStage']['name'],
                        0 if x['closingResultId'] == None else x['closingResult']['name'],
                        0 if x['moderatorClosingResultId'] == None else x['moderatorClosingResult']['name'],
                        0 if x['closingDescriptionId'] == None else x['closingDescription']['name'],
                        x['fileCount'],
                        x['photoForRakursCount'],
                        x['rakursCount']

                    ))
    return data


async def main():
    columns = 'createdDate, updatedAt, lifeTimeDate, number_task, userExecutor, companyUserExecutor, serviceObject, stage, executorStage, moderatorStage, closingResult, moderatorClosingResult, closingDescription, fileCount, photoForRakursCount, rakursCount'
    token = await master_auth()
    df1 = await get_file1(token)
    df = await get_file(token, df1)
    await data_truncate('imaster', settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)
    await data_post(df, 'imaster', columns, settings.DB_SCRIPTS, 'scripts', settings.DB_ACCESS)


if __name__ == "__main__":
    asyncio.run(main())

