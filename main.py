"""
Implemented using telethon & schedule
"""
import os
import time
import psycopg2 as ps2
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


def get_flight_price(url):
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
        #        browser.get('https://httpbin.org/user-agent')
        ##        time.sleep(5)
        print('selenium',  url)
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
        num_tranship = record[3]
        num_adults = record[6]
        telegram_user = record[11]
        print(telegram_user)
        search_link = f'https://www.onetwotrip.com/ru/f/search/{depart_date}{origin_city_code}{dest_city_code}{return_date}?s=true&sc=E&ac={num_adults}&tr={num_tranship}'
        print(search_link)
        price = get_flight_price(search_link)
        send_result(or_city=record[1], des_city=record[2], dep_date=record[4], ret_date=record[5],
                    price=price, telegram_user=telegram_user)
    return True


if __name__ == '__main__':
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
