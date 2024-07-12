import os
from random import choice

import requests
from fake_useragent import UserAgent
from psycopg2.extensions import AsIs


def get_data(db_connection, table):
    """Возвращает данные из БД"""
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM %(table_name)s;", {"table_name": AsIs(table)})
    return cursor.fetchall()


def get_flight_data(url, request_data, city_codes):
    """Осуществляет запрос к поисковому сервису и возвращает данные о всех вариантах перелета"""
    ua = UserAgent()
    my_headers = {
        "User-Agent": ua.random,
    }

    proxy_list = os.getenv("proxy_list")[1:-1].split(", ")
    proxy_ip = choice(proxy_list)
    my_proxies = {
        "http": f"http://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}",
        # 'https': f"https://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}"
    }

    origin_city_code = dest_city_code = ""
    for i in city_codes:
        if i["city_rus"] == request_data["depart_city"]:
            origin_city_code = i["code_eng"]
            break

    for i in city_codes:
        if i["city_rus"] == request_data["dest_city"]:
            dest_city_code = i["code_eng"]
            break

    params = {
        "route": f'{str(request_data["depart_date"].day).rjust(2, "0") + str(request_data["depart_date"].month).rjust(2, "0")}{origin_city_code}{dest_city_code}{str(request_data["return_date"].day).rjust(2, "0") + str(request_data["return_date"].month).rjust(2, "0") if request_data["return_date"] is not None else ""}',
        "ad": request_data["num_adults"],
        "cn": request_data["num_children"],
        "in": request_data["num_infants"],
        "showDeeplink": "false",
        "cs": "E",
        "source": "yandex_direct",
        "priceIncludeBaggage": "true",
        "noClearNoBags": "true",
        "noMix": "true",
        "srcmarker": "airlines_airport_desk_all_agency1_cpa_k36332661070",
        "cryptoTripsVersion": 61,
        "doNotMap": "true",
    }
    # print("Страница запроса с IP:", requests.get("http://icanhazip.com", proxies=my_proxies).text.strip())
    all_flights_data = requests.get(
        url=url, params=params, proxies=my_proxies, headers=my_headers
    ).json()
    if not all_flights_data["transportationVariants"]:
        return None
    return all_flights_data


def get_column_names(db_connection, table):
    """Возвращает названия столбцов таблицы"""
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM %(table_name)s LIMIT 0;", {"table_name": AsIs(table)})
    return [desc[0] for desc in cursor.description]
