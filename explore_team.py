import requests, json

URL = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/tournamentstats?tmcl=bmmk637l2a33h90zlu36kx8no"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://theanalyst.com/competition/english-championship/stats",
    "x-sdapi-token": "LRkJ2MjwlC8RxUfVkne4",
}

r = requests.get(URL, headers=HEADERS, timeout=30)
data = r.json()

team = data.get("team", {})
print("TEAM TOP LEVEL KEYS:", list(team.keys()))
print()
for section, val in team.items():
    if isinstance(val, dict):
        print(f"team.{section} sub-keys: {list(val.keys())}")
        for sub, arr in val.items():
            if isinstance(arr, list) and arr:
                print(f"  team.{section}.{sub}[0] fields: {list(arr[0].keys())}")
    elif isinstance(val, list) and val:
        print(f"team.{section}[0] fields: {list(val[0].keys())}")
