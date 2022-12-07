import json
import requests
import settings


# Трек из PSD
async def psd_track_get(token: str, params: str):
    headers = {'Authorization': token}
    req_data = requests.get(settings.PDS_TRACK_URL + params, headers=headers)
    roads = json.loads(req_data.text)

    return roads
