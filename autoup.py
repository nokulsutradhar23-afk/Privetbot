import requests
from google_play_scraper import app

def AuToUpDaTE():
    result = app('com.dts.freefireth', lang='fr', country='fr')
    version = result['version']
    r = requests.get(
        f'https://version.ggwhitehawk.com/live/ver.php?version={version}&lang=en&device=android&channel=android&appstore=googleplay&region=ME&release_version=OB53&whitelist_version=1.6.0&whitelist_sp_version=1.0.0&device_name=samsung%20SM-S9180&device_CPU=x86-64%20SSE3%20SSE4.1%20SSE4.2%20AVX&device_GPU=Adreno%20%28TM%29%20640&device_mem=3004'
    ).json()
    return r['server_url'], r['latest_release_version'], version