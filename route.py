import requests
import json
import asyncio

async def routes_get(token: str):
    headers = {'Authorization': token}
    req_data = requests.get('https://ets.mos.ru/services/route?is_archive=false', headers=headers)
    routes = json.loads(req_data.text)

    return routes

async def main():
    token = 'Token eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxMDYzMjIsImNvbXBhbnlfaWQiOm51bGwsImNvbXBhbmllc19pZHMiOls5MDAwMDI2XSwiZW52IjoicHJvZCIsInRpbWVzdGFtcCI6MTY3MDg1MjE1My40NTI0MDF9.QhYPwYMxzT5qzZgjMaV1Zfr4lJ5Zh_NpRHiGF_N5vX4'

    routes = await routes_get(token)
    for route in routes['result']:
        headers = {'Authorization': token}
        req_data = requests.get('https://ets.mos.ru/services/route?id=' + str(route['id']), headers=headers)
        rou = json.loads(req_data.text)

        for x in rou['result'][0]['object_list']:
            if x['name'] == 'Петровский парк':
                print(route['name'])

if __name__ == "__main__":
    asyncio.run(main())
