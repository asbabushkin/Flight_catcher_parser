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

from fl_catcher_data import city_codes

from dotenv import load_dotenv

load_dotenv()
options_chrome = webdriver.ChromeOptions()


# options_chrome.add_extension('/home/asb/Python/Parsing_course/coordinates.crx')


def get_one_two_trip(url):
    with webdriver.Chrome(options=options_chrome) as browser:
        browser.get(url)
        if WebDriverWait(browser, 100, poll_frequency=0.5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'c27ZC'))):
            div = browser.find_element(By.CLASS_NAME, 'jC6yz')
            return div.find_elements(By.CLASS_NAME, '_4-iO8')[1].text


def main():
    ua = UserAgent()
    user_agent = ua.random
    print(user_agent)
    options_chrome.add_argument(f'user-agent={user_agent}')
    price = get_one_two_trip(search_link)
    print(price)
    with TelegramClient('flight_catcher', os.getenv('TELEGRAM_API'), os.getenv('TELEGRAM_HASH')) as client:
        client.send_message(os.getenv('TELEGRAM_USER_NAME'), message=f'Flight catcher: цена перелета {price}')


if __name__ == '__main__':

    try:
        db_connection = ps2.connect(
            host=os.getenv('db_host'),
            user=os.getenv('db_user'),
            password=os.getenv('db_password'),
            port=os.getenv('db_port'),
            database=os.getenv('db_name')
        )
        db_connection.autocommit = True

        with db_connection.cursor() as cursor:
            # [print(i) for i in cursor.execute("SELECT * FROM flight_search_search;")]
            cursor.execute(
                "SELECT * FROM flight_search_search;"
            )
            data = cursor.fetchall()
            print(data)

            # url = 'https://www.onetwotrip.com/ru/f/search/1010CEKLED?s=true&sc=E&ac=1&tr=0'



    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL', _ex)


    finally:
        if db_connection:
            db_connection.close()
            print('[INFO] PostgreSQL connection closed')

    for record in data:
        return_date = ''
        depart_date = str(record[3].day).rjust(2, '0') + str(record[3].month).rjust(2, '0')
        print(depart_date)
        if record[4] is not None:
            return_date = str(record[4].day).rjust(2, '0') + str(record[4].month).rjust(2, '0')
            print(return_date)
        for i in city_codes:
            if i['city_rus'] == record[-2]:
                origin_city_code = i['code_eng']
                print(origin_city_code)
                break
        for i in city_codes:
            if i['city_rus'] == record[-1]:
                dest_city_code = i['code_eng']
                print(dest_city_code)
                break
        num_adults = record[5]
        print(f'{num_adults} passengers')
        search_link = f'https://www.onetwotrip.com/ru/f/search/{depart_date}{dest_city_code}{origin_city_code}{return_date}?s=true&sc=E&ac={num_adults}'
        print(search_link)
        main()

    # schedule.every().minute.do(main)
    # while True:
    #     schedule.run_pending()
