import requests
import json
import asyncio

import settings


async def token_get(login: str, password: str):
    url = 'https://puos.mos.ru/auth/v1/tokens/access'
    payload = {'user': login, 'password': password}
    req_post = requests.post(url, json=payload)
    req_data = json.loads(req_post.text)
    try:
        return 'Bearer ' + req_data['accessToken']
    except:
        print("Пароль для аккаунта " + login + " изменён")


async def main():
    token = await token_get(settings.PUOS_ACCESS['login'], settings.PUOS_ACCESS['password'])

    url = 'https://puos.mos.ru/main/v1/tickets?page=0&size=99999&sortBy=piip_create_date&sortDir=DESC&season=3&cargoSendCode=118577'
    headers = {'Authorization': token}
    req_data = requests.get(url, headers=headers)
    puos_data = json.loads(req_data.text)

    f = 1
    for x in puos_data['items']:
        print(f, x)
        f += 1

if __name__ == "__main__":
    asyncio.run(main())
