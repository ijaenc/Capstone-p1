import time
import requests

url = "http://router.project-osrm.org/route/v1/driving/-70.66,-33.45;-70.65,-33.44?overview=false"

session = requests.Session()
session.trust_env = False

t0 = time.perf_counter()
resp = session.get(url, timeout=10)
dt = time.perf_counter() - t0

print("status:", resp.status_code)
print("tiempo:", round(dt, 2), "s")
print(resp.text[:300])