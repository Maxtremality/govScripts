import json

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from seleniumrequests import Firefox
from datetime import datetime
from openpyxl import load_workbook
from datetime import timedelta
import requests
import time
import asyncio
import os
import sys
import traceback

import settings
from db import data_post
from db import data_delete


async def master_auth():
    url = 'https://vao.infocity.me/api/v1/auth/token'
    payload = {
        "login": "avd_vao_dir",
        "password": "12345678"
    }
    req_post = requests.post(url, json=payload)
    token = 'Bearer ' + json.loads(req_post.text)['accessToken']

    return token


async def get_file(token):
    print(token)
    url = 'https://vao.infocity.me/api/v1/tasks?skip=50&take=25'
    headers = {'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiYXZkX3Zhb19kaXIiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjJiOTE3MzliLWY0MDEtNDViMi04NjQxLWNiYWIwNTc2ZjkzZSIsIkNvbXBhbnlJZCI6IjY1MTFmMjQ3LTJhNGUtNGJmMC1iNTk2LWVhMGQxZWMzOWZmZCIsIk1lbWJlcklkIjoiZjQwZTYxNGEtOGY0Yi00ZGQ4LWJjYzQtOWFmNzljNzhlYmY4IiwiUG9zaXRpb25JZCI6IjAzZDE3ZmM1LTFhMzctNDkxZi1iNjk2LTg3ZGQ2NDA1ZjQ4NCIsIlJvbGVJZCI6ImY1MjNkZDI3LTk1MDQtNDI4OC1iNDY1LTQxYjY3YzYwYWRhZiIsIlNlc3Npb25JZCI6IjAiLCJleHAiOjE2ODE0MDY2ODQsImlzcyI6IiR7RElTUF9JTlNUQU5DRTotbG9jYWx9IiwiYXVkIjoiYXBpOi8vZGVmYXVsdCJ9.X3TBaq2rHId21qKrHZoB3ByCThNHyQZU_pZrQaJ2tqd-bSgObutDEVF_IEpM7dkc2xV55aHBmnlu0dhlnzPgAxi2g7ovs8Qqova2RCuG9OxwGiPjmCj7jZvklXsSia7_YjvXFZajMIm29ZxD9RuyQIrjEksL2qYX1jYFMlcODMN9AlGpEx0Dq0DIBFLrl60dRtsvHWvDJW9uZfn1T3V6RFl-I6Lo5ElJbbP9T_G54aohFJhgIzcBCVIcw8zEi_vseuii23LAbsQqN2IUdHBRewMdf1ISQVGFw87_jfSgnP5p--aRT_sbguXdsEL16WXKvlsriQb5SV9HBw7emCHn7lvqVawz1Hm9kR6epKOg43ap3f71gCp69BfMPl69a23bR83dEQLfKCUpTVyjACpzOqkEZW8k9-Dfi0Pz8jA4vgBSUqSSUxrdN-N7-cAcO4wQqRtRJOXimm-sGgN_qBEraRGOMpLglmSMWUGyDvj2a1OpgIjpJ5XkyfbtwNtZZQd0T7fzwYFkWWLrqOz-FQKkM5Xi5y3oGx-O7wYX85xr6ulPoi1FvK2wfRHUCTrDHJui0xj6bt_qcX7-HNZhCic2qrOgoRaSRlPbuFLRDpPSe-Z_eXMjCyOKvi5iRsdzRfv6MeipQnsQhFruw1DrX15oKMO0SsWDfKCyzt3HAEZ9v2U'}
    req = requests.get(url, headers=headers)
    print(req)


async def main():
    start = time.time()
    token = await master_auth()
    await get_file(token)
    end = time.time()
    print(time.strftime("%H:%M:%S", time.gmtime(end - start)))

if __name__ == "__main__":
    asyncio.run(main())
