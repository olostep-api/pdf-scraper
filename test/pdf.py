import requests
import json

url = "https://api.olostep.com/v1/scrapes"
payload = {
    "url_to_scrape": "https://arxiv.org/pdf/2602.15705.pdf",
    "formats": ["markdown"],
}
headers = {
    "Authorization": "Bearer olostep_djPCM0UtthFb5KnfLuQrUFJB7mTzEPmWwNOK",
    "Content-Type": "application/json",
}

print("Starting Olostep API call...")
response = requests.post(
    url, json=payload, headers=headers, timeout=(10, 60)
)  # 10s connect, 60s read
# print(json.dumps(response.json(), indent=4))
with open("output.json", "w") as f:
    json.dump(response.json(), f, indent=4)
print(f"response saved to output.json")

# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# session = requests.Session()
# retries = Retry(
#     total=5,
#     connect=5,
#     read=5,
#     backoff_factor=0.5,
#     status_forcelist=[429, 500, 502, 503, 504],
#     allowed_methods=["POST"],
# )
# session.mount("https://", HTTPAdapter(max_retries=retries))
# session.mount("http://", HTTPAdapter(max_retries=retries))

# print("Starting Olostep API call...")
# resp = session.post(
#     url, json=payload, headers=headers, timeout=(10, 60)
# )  # 10s connect, 60s read
# print(resp.status_code, resp.text[:500])
