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
        "INSERT INTO %(archive)s (old_id, oneway_flight, max_transhipments, depart_date, return_date, num_adults, num_children, luggage, search_init_date, telegr_acc, phone_num, email, depature_city, dest_city) SELECT * FROM %(table_name)s WHERE depart_date < CURDATE();",
        {"archive": AsIs(archive), "table_name": AsIs(table)})
    cursor.execute("DELETE FROM %(table_name)s WHERE depart_date < CURDATE();",
                   {"table_name": AsIs(table)})


def delete_old_records(db_connection, table, archive):
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, oneway_flight, max_transhipments, depart_date, return_date, num_adults, num_children, luggage, search_init_date, telegr_acc, phone_num, email, depature_city, dest_city) SELECT * FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 72;",
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
        browser.get(url)
        if WebDriverWait(browser, 100, poll_frequency=0.5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'c27ZC'))):
            div = browser.find_element(By.CLASS_NAME, 'jC6yz')
            return div.find_elements(By.CLASS_NAME, '_4-iO8')[-1].text


def send_result(or_city, des_city, dep_date, ret_date, price):
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


def main():
    dest_city_code = origin_city_code = return_date = ''

    with set_connection() as conn:
        delete_depart_date_expired_records(conn, 'flight_search_search', 'flight_search_archive')
        delete_old_records(conn, 'flight_search_search', 'flight_search_archive')
        search_data = get_data(conn, 'flight_search_search')
        city_data = get_data(conn, 'flight_search_citycode')

    keys = ['city_eng', 'city_rus', 'code_eng', 'code_rus']
    city_codes = [dict(zip(keys, city_data[c][1:])) for c in range(len(city_data))]
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
        price = get_flight_price(search_link)
        send_result(or_city=record[-2], des_city=record[-1], dep_date=record[3], ret_date=record[4],
                    price=price)
    return True


if __name__ == '__main__':
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
