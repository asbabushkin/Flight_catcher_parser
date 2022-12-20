"""
Implemented using telethon & schedule
"""
import os
import time
import psycopg2 as ps2
import requests
import schedule
from fake_useragent import UserAgent
# from selenium import webdriver
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from telethon import TelegramClient, events, sync, connection as tel_connection
from dotenv import load_dotenv
from psycopg2.extensions import AsIs
from random import choice

load_dotenv()


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



def delete_depart_date_expired_records(db_connection, table, archive):
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, phone_num, email) SELECT * FROM %(table_name)s WHERE depart_date < CURRENT_DATE;",
        {"archive": AsIs(archive), "table_name": AsIs(table)})
    cursor.execute("DELETE FROM %(table_name)s WHERE depart_date < CURRENT_DATE;",
                   {"table_name": AsIs(table)})
    return None


def delete_old_records(db_connection, table, archive):
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, phone_num, email) SELECT * FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 72;",
        {"archive": AsIs(archive), "table_name": AsIs(table)})
    cursor.execute("DELETE FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 72;",
                   {"table_name": AsIs(table)})



def get_flight_price_json(url, depart_date, origin_city_code, dest_city_code, return_date, tranship_limit):
    print(url)
    params = {
        'route': f'{depart_date}{origin_city_code}{dest_city_code}{return_date}',
        'ad': 1,
        'cn': 0,
        'in': 0,
        'showDeeplink': 'false',
        'cs': 'E',
        'source': 'yandex_direct',
        'priceIncludeBaggage': 'true',
        'noClearNoBags': 'true',
        'noMix': 'true',
        'srcmarker': 'airlines_airport_desk_all_agency1_cpa_k36332661070',
        'cryptoTripsVersion': 61,
        'doNotMap': 'true',
    }
    all_flights_data = requests.get(url=url, params=params).json()
    # filter on transhipmentts limit
    transport_var_filtered = []
    for item in all_flights_data['transportationVariants']:
        if len(all_flights_data['transportationVariants'][item]['tripRefs']) <= tranship_limit + 1:
            transport_var_filtered.append(item)
    print(transport_var_filtered)

    # getting best price
    transp_variant_prices = {}
    for item in all_flights_data['prices']:
        if all_flights_data['prices'][item]['transportationVariantIds'][0] in transport_var_filtered:
            transp_variant_prices[all_flights_data['prices'][item]['transportationVariantIds'][0]] = all_flights_data['prices'][item]['totalAmount']
    print('Transportation variant prices prices:', transp_variant_prices)

    cheapest_transport_var_id = []
    best_price = min(transp_variant_prices.values())
    print(f'Best price: {best_price}')
    for key, value in transp_variant_prices.items():
        if value == best_price:
            cheapest_transport_var_id.append(key)

    print(f'Cheapest transhipment variant id: {cheapest_transport_var_id}')

    best_transp_variants = []
    for item in all_flights_data['transportationVariants']:
        for transp_id in cheapest_transport_var_id:
            if item == transp_id:
                trip_ids = []
                for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                    trip_ids.append(all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                best_transp_variants.append([all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                                 trip_ids])
    print(f'Best transportation trips and time: {best_transp_variants}')
    best_flights_info = []
    for trip in best_transp_variants:
        if len(trip[1]) == 1:
            flight_info = {
                'depart_date_time': all_flights_data['trips'][trip[1][0]]['startDateTime'],
                'arrive_date_time': all_flights_data['trips'][trip[1][0]]['endDateTime'],
                'carrier': all_flights_data['trips'][trip[1][0]]['carrier'],
                'flight_number': all_flights_data['trips'][trip[1][0]]['carrierTripNumber'],
                'orig_city': all_flights_data['trips'][trip[1][0]]['from'],
                'dest_city': all_flights_data['trips'][trip[1][0]]['to'],
                'num_tranship': 0,
                'total_flight_time': trip[0],
            }
            best_flights_info.append(flight_info)
            print(f'Best flights info: {best_flights_info}')

        else:
            for i in range(len(trip[1])):
                flight_info = {
                    'depart_date_time': all_flights_data['trips'][trip[1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][-1]]['to'],
                    'num_tranship': len(trip[1])-1,
                    'tranship_cities': [all_flights_data['trips'][trip[1][i]]['to'] for i in range(len(trip[1])-1)],
                    'total_flight_time': trip[0],
                }
                best_flights_info.append(flight_info)
            print(f'Best flights info: {best_flights_info}')
    return best_flights_info








def get_flight_price_selenium(url):
    proxy_list = os.getenv('proxy_list')[1:-1].split(', ')
    proxy_ip = choice(proxy_list)
    ua = UserAgent()
    options = {
        'proxy': {
            'http': f"http://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}",
            'https': f"https://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}"
        },
    }
    opts = webdriver.ChromeOptions()
    opts.add_argument(f'user-agent={ua.random}')
    opts.add_argument('--headless')

    with webdriver.Chrome(seleniumwire_options=options, options=opts) as browser:
        browser.get(url)
        try:
            if WebDriverWait(browser, 20, poll_frequency=0.5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, '_4eVQ4'))):
                div = browser.find_element(By.CLASS_NAME, '_4eVQ4')
                return div.find_elements(By.CLASS_NAME, '_4-iO8')[0].text
        except TimeoutException:
            return False


def send_result(or_city, des_city, dep_date, ret_date, price, telegram_user):
    if not price:
        print(f'Перелет {or_city} - {des_city} с указанными параметрами не найден.')
        with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
            client.send_message(telegram_user,
                                message=f'Перелет {or_city} - {des_city}\n с указанными параметрами не найден.')
        return 'Flight not found'
    else:
        if ret_date is not None:
            print(f'Перелет {or_city} - {des_city} {dep_date} - {ret_date} цена {price} руб.')
            with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
                client.send_message(telegram_user,
                                    message=f'Перелет {or_city} - {des_city}\nвылет {dep_date}\nвозвращение {ret_date}\nцена {price} руб.')
            return 'Round flight'
        else:
            print(f'Перелет {or_city} - {des_city} вылет {dep_date} цена {price} руб.')
            with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
                client.send_message(telegram_user,
                                    message=f'Перелет {or_city} - {des_city}\nвылет {dep_date}\nцена {price} руб.')
            return 'Oneway flight'


def main():
    with set_connection() as conn:
        delete_depart_date_expired_records(conn, 'flight_search_search', 'flight_search_searcharchive')
        delete_old_records(conn, 'flight_search_search', 'flight_search_searcharchive')
        search_data = get_data(conn, 'flight_search_search')
        city_data = get_data(conn, 'flight_search_citycode')

    keys = ['city_eng', 'city_rus', 'code_eng', 'code_rus']
    city_codes = [dict(zip(keys, city_data[c][1:])) for c in range(len(city_data))]
    for record in search_data:
        dest_city_code = origin_city_code = return_date = depart_date = num_adults = telegram_user = ''
        depart_date = str(record[4].day).rjust(2, '0') + str(record[4].month).rjust(2, '0')
        if record[5] is not None:
            return_date = str(record[5].day).rjust(2, '0') + str(record[5].month).rjust(2, '0')
        for i in city_codes:
            if i['city_rus'] == record[1]:
                origin_city_code = i['code_eng']
                break
        for i in city_codes:
            if i['city_rus'] == record[2]:
                dest_city_code = i['code_eng']
                break
        tranship_limit = record[3]
        num_adults = record[6]
        telegram_user = record[11]
        print(telegram_user)
        #search_link = f'https://www.onetwotrip.com/ru/f/search/{depart_date}{origin_city_code}{dest_city_code}{return_date}?s=true&sc=E&ac={num_adults}&tr={tranship_limit}'
        search_link_json = f'https://www.onetwotrip.com/_avia-search-proxy/search/v3'
        #print(search_link)
        #price = get_flight_price_selenium(search_link)
        price = get_flight_price_json(search_link_json, depart_date, origin_city_code, dest_city_code, return_date, tranship_limit)
        #send_result(or_city=record[1], des_city=record[2], dep_date=record[4], ret_date=record[5], price=price, telegram_user=telegram_user)
    return True




if __name__ == '__main__':
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
