import requests
import json

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',

}

url = 'https://www.onetwotrip.com/_avia/deals_v4/directApiTop?origin=CEK&destinations=MOW&departure_date_from=2022-10-02&departure_date_to=2022-10-05&roundtrip_flights=true&noPricing=false&group_by_date=true&deals_limit=50&all_combinations=true&source=yandex_direct&return_date_from=2022-10-07&return_date_to=2022-10-11'
response = requests.get(url=url, headers=headers).json()
with open('res.json', 'w', encoding='utf-8') as file:
    json.dump(response, file, indent=1, ensure_ascii=False)
print(response)