"""
Реализация через telethon & schedule
https://www.onetwotrip.com/ru/f/search/0110CEKMOW0710?sc=E&ac=2&ca=0_4&srcmarker2=newindex
Запрос все рейсы
https://www.onetwotrip.com/_avia-search-proxy/search/v3?route=0110CEKMOW0710&ad=2&cn=1&in=1&showDeeplink=false&cs=E&source=yandex_direct&priceIncludeBaggage=true&noClearNoBags=true&noMix=true&srcmarker=airlines_airport_desk_all_agency1_cpa_k36332661070&cryptoTripsVersion=61&doNotMap=true

Лучший рейс на плавающие даты
https://www.onetwotrip.com/_avia/deals_v4/directApiTop?origin=CEK&destinations=MOW&departure_date_from=2022-09-29&departure_date_to=2022-10-03&roundtrip_flights=true&noPricing=false&group_by_date=true&deals_limit=50&all_combinations=true&source=yandex_direct&return_date_from=2022-10-05&return_date_to=2022-10-09
"""
import os
import psycopg2 as ps2
import schedule
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from telethon import TelegramClient, events, sync, connection as tel_connection
from dotenv import load_dotenv
from psycopg2.extensions import AsIs


load_dotenv()
options_chrome = webdriver.ChromeOptions()


def get_price(url):
    with webdriver.Chrome(options=options_chrome) as browser:
        browser.get(url)
        if WebDriverWait(browser, 100, poll_frequency=0.5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'c27ZC'))):
            div = browser.find_element(By.CLASS_NAME, 'jC6yz')
            return div.find_elements(By.CLASS_NAME, '_4-iO8')[1].text


def send_result(or_city, des_city, dep_date, ret_date):
    ua = UserAgent()
    user_agent = ua.random
    options_chrome.add_argument(f'user-agent={user_agent}')
    price = get_price(search_link)
    if ret_date is not None:
        print(f'Перелет {or_city} - {des_city} {dep_date} - {ret_date} цена {price} руб.')
        with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
            client.send_message(os.getenv('TELEGRAM_USER_NAME'),
                                message=f'Перелет {or_city} - {des_city}\nвылет {dep_date}\nвозвращение {ret_date}\nцена {price} руб.')
    else:
        print(f'Перелет {or_city} - {des_city} вылет {dep_date} цена {price} руб.')
        with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
            client.send_message(os.getenv('TELEGRAM_USER_NAME'),
                                message=f'Перелет {or_city} - {des_city}\nвылет {dep_date}\nцена {price} руб.')


def set_connection():
    try:
        db_connection = ps2.connect(
            host=os.getenv('db_host'),
            user=os.getenv('db_user'),
            password=os.getenv('db_password'),
            port=os.getenv('db_port'),
            database=os.getenv('db_name')
        )
        db_connection.autocommit = True
        return db_connection

    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL', _ex)
        raise _ex

def get_data(db_connection, table):
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM %(table_name)s;", {"table_name": AsIs(table)})
    return cursor.fetchall()


if __name__ == '__main__':

    with set_connection() as conn:
        search_data = get_data(conn, 'flight_search_search')
        values = get_data(conn, 'flight_search_citycode')


    # try:
    #     db_connection = ps2.connect(
    #         host=os.getenv('db_host'),
    #         user=os.getenv('db_user'),
    #         password=os.getenv('db_password'),
    #         port=os.getenv('db_port'),
    #         database=os.getenv('db_name')
    #     )
    #     db_connection.autocommit = True
    #
    #     with db_connection.cursor() as cursor:
    #         cursor.execute("SELECT * FROM flight_search_search;")
    #         search_data = cursor.fetchall()
    #         cursor.execute("SELECT * FROM flight_search_citycode;")
    #         values = cursor.fetchall()
    #         keys = ['city_eng', 'city_rus', 'code_eng', 'code_rus']
    #
    #
    #
    # except Exception as _ex:
    #     print('[INFO] Error while working with PostgreSQL', _ex)
    #
    #
    # finally:
    #     if db_connection:
    #         db_connection.close()
    #         print('[INFO] PostgreSQL connection closed')

    keys = ['city_eng', 'city_rus', 'code_eng', 'code_rus']
    city_codes = [dict(zip(keys, values[v][1:])) for v in range(len(values))]
    for record in search_data:
        depart_date = str(record[3].day).rjust(2, '0') + str(record[3].month).rjust(2, '0')
        if record[4] is not None:
            return_date = str(record[4].day).rjust(2, '0') + str(record[4].month).rjust(2, '0')
        for i in city_codes:
            if i['city_rus'] == record[-2]:
                origin_city_code = i['code_eng']
                break
        for i in city_codes:
            if i['city_rus'] == record[-1]:
                dest_city_code = i['code_eng']
                break
        num_adults = record[5]

        search_link = f'https://www.onetwotrip.com/ru/f/search/{depart_date}{dest_city_code}{origin_city_code}{return_date}?s=true&sc=E&ac={num_adults}'
        print(search_link)
        send_result(or_city=record[-2], des_city=record[-1], dep_date=record[3], ret_date=record[4])

    # schedule.every().minute.do(main)
    # while True:
    #     schedule.run_pending()
