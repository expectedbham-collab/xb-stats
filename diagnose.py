import requests, json

URL = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/tournamentstats?tmcl=bmmk637l2a33h90zlu36kx8no"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://theanalyst.com/",
}

r = requests.get(URL, headers=HEADERS, timeout=30)
data = r.json()

player = data.get("player", {})
print("TOP LEVEL KEYS:", list(data.keys()))
print("PLAYER KEYS:", list(player.keys()))
for key, val in player.items():
    if key in ("lastUpdated", "league"):
        print(f"  {key}: {val}")
    elif isinstance(val, dict):
        print(f"  player.{key} sub-keys: {list(val.keys())}")
        for sub, arr in val.items():
            if isinstance(arr, list) and arr:
                print(f"    player.{key}.{sub}[0] fields: {list(arr[0].keys())}")
