"""
Implemented using telethon & schedule
"""

import os
import time
import psycopg2 as ps2
import requests
import schedule
from fake_useragent import UserAgent
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


def get_flight_data(url, depart_date, origin_city_code, dest_city_code, return_date):
    ua = UserAgent()
    my_headers = {
        'User-Agent': ua.random,
    }

    proxy_list = os.getenv('proxy_list')[1:-1].split(', ')
    proxy_ip = choice(proxy_list)
    my_proxies = {
        'http': f"http://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}",
        # 'https': f"https://{os.getenv('proxy_login')}:{os.getenv('proxy_password')}@{proxy_ip}:{os.getenv('http_port')}"
    }

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
    # print("Страница запроса с IP:", requests.get("http://icanhazip.com", proxies=my_proxies).text.strip())
    all_flights_data = requests.get(url=url, params=params, proxies=my_proxies, headers=my_headers).json()
    # print(f'Got all_flights_data: {all_flights_data} ')
    return all_flights_data


def tranship_limit_filter(all_flights_data, tranship_limit):
    transport_variants_tranship_limit_filtered = []
    for item in all_flights_data['transportationVariants']:
        if len(all_flights_data['transportationVariants'][item]['tripRefs']) <= tranship_limit + 1:
            transport_variants_tranship_limit_filtered.append(item)
    # print(f'Transportation variants filtered according to transhipments limit (tranship_limit_filter()): {transport_variants_tranship_limit_filtered}')
    return transport_variants_tranship_limit_filtered


def round_flights_filter(all_flights_data):
    transport_variants_round_flight_filtered = []
    for item in all_flights_data['prices']:
        if len(all_flights_data['prices'][item]['transportationVariantIds']) == 2:
            transport_variants_round_flight_filtered.append(
                all_flights_data['prices'][item]['transportationVariantIds'])
    #  print(f'Transportation variants filtered according to round flight condition (round_flights_filter()): {transport_variants_round_flight_filtered}')
    return transport_variants_round_flight_filtered


def get_transport_variant_prices(all_flights_data, transport_var_filtered):
    transp_variant_prices = {}
    for item in all_flights_data['prices']:
        if len(all_flights_data['prices'][item]['transportationVariantIds']) == 1:
            if all_flights_data['prices'][item]['transportationVariantIds'][0] in transport_var_filtered:
                transp_variant_prices[all_flights_data['prices'][item]['transportationVariantIds'][0]] = \
                    all_flights_data['prices'][item]['totalAmount']
        elif len(all_flights_data['prices'][item]['transportationVariantIds']) == 2:
            for var in transport_var_filtered:
                if all_flights_data['prices'][item]['transportationVariantIds'][0] in var and \
                        all_flights_data['prices'][item]['transportationVariantIds'][1] in var:
                    transp_variant_prices[tuple(all_flights_data['prices'][item]['transportationVariantIds'])] = \
                        all_flights_data['prices'][item]['totalAmount']
                    continue
    #    print(f'Transportation variant prices (get_transport_variant_prices): {transp_variant_prices}')
    return transp_variant_prices


def get_cheapest_transport_variants(all_flights_data, transp_variant_prices):
    best_price = min(transp_variant_prices.values())
    # print(f'best_price: {best_price}')
    cheapest_transport_var_id = []
    for key, value in transp_variant_prices.items():
        if value == best_price:
            cheapest_transport_var_id.append(key)
    # print(f'cheapest_transport_var_id: {cheapest_transport_var_id}')
    cheapest_transp_variants = []
    for transp_id in cheapest_transport_var_id:
        for item in all_flights_data['transportationVariants']:
            if isinstance(transp_id, str):
                if item == transp_id:
                    trip_ids = []
                    for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                        trip_ids.append(all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                    cheapest_transp_variants.append(
                        [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                         trip_ids])
            elif isinstance(transp_id, tuple):
                trip_ids = []
                if item == transp_id[0]:
                    lst_forvard_way = []
                    for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                        lst_forvard_way.append(
                            all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                    lst_forvard_way_and_time = list(
                        [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                         lst_forvard_way])

                elif item == transp_id[1]:
                    lst_back_way = []
                    for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                        lst_back_way.append(all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                    lst_back_way_and_time = list(
                        [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                         lst_back_way])
                    cheapest_transp_variants.append([lst_forvard_way_and_time[0] + lst_back_way_and_time[0],
                                                     [lst_forvard_way_and_time, lst_back_way_and_time]])
                # trip_ids = (lst_forvard_way_and_time, lst_back_way_and_time)
                # cheapest_transp_variants.append(
                #     [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                #      trip_ids])
    #  print(f'Cheapest transportation variants (get_cheapest_transport_variants()): {cheapest_transp_variants}')
    #  print(f'Best price (get_cheapest_transport_variants(): {best_price}')
    return cheapest_transp_variants, best_price


def get_best_flights_info(all_flights_data, cheapest_transp_variants, best_price, return_date):
    # print(f'return_date: {return_date}')
    if return_date is None:
        # one way flight
        #    print('return date is none')
        best_flights_info = []
        for trip in cheapest_transp_variants:
            #        print(f'trip {trip}')
            if len(trip[1]) == 1:
                # no transhipments one way flight DONE!
                flight_info = {
                    'flight_type': 'one way',
                    'price': best_price,
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
            else:
                # one way flight with transhipments DONE!
                flight_info = {
                    'flight_type': 'one way',
                    'price': best_price,
                    'depart_date_time': all_flights_data['trips'][trip[1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][-1]]['to'],
                    'num_tranship': len(trip[1]) - 1,
                    'tranship_cities': [all_flights_data['trips'][trip[1][i]]['to'] for i in
                                        range(len(trip[1]) - 1)],
                    'total_flight_time': trip[0],
                }
                best_flights_info.append(flight_info)

    # round trip
    else:
        #    print('round trip')
        best_flights_info = []
        for trip in cheapest_transp_variants:
            #        print(f'trip {trip}')
            best_flight_i_info = []
            if len(trip[1][0][1]) == 1:
                # round trip forward flight no transhipments
                first_flight_info = {
                    'flight_type': 'round flight',
                    'price': best_price,
                    'depart_date_time': all_flights_data['trips'][trip[1][0][1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][0][1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][0][1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][0][1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][0][1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][0][1][-1]]['to'],
                    'num_tranship': len(trip[1][0][1]) - 1,
                    'total_flight_time': trip[1][0][0],
                }
                best_flight_i_info.append(first_flight_info)
            else:
                # round trip forward flight with transhipments
                first_flight_info = {
                    'flight_type': 'round flight',
                    'price': best_price,
                    'depart_date_time': all_flights_data['trips'][trip[1][0][1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][0][1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][0][1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][0][1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][0][1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][0][1][-1]]['to'],
                    'num_tranship': len(trip[1][0][1]) - 1,
                    'tranship_cities': [all_flights_data['trips'][trip[1][0][1][i]]['to'] for i in
                                        range(len(trip[1][0][1]) - 1)],
                    'total_flight_time': trip[1][0][0],
                }
                best_flight_i_info.append(first_flight_info)

            if len(trip[1][1][1]) == 1:
                # round trip back flight no transhipments
                back_flight_info = {
                    'flight_type': 'round flight',
                    'price': best_price,
                    'depart_date_time': all_flights_data['trips'][trip[1][1][1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][1][1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][1][1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][1][1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][1][1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][1][1][-1]]['to'],
                    'num_tranship': len(trip[1][1][1]) - 1,
                    'total_flight_time': trip[1][1][0],
                }
                best_flight_i_info.append(back_flight_info)

            else:
                # round trip back flight with transhipments
                back_flight_info = {
                    'flight_type': 'round flight',
                    'price': best_price,
                    'depart_date_time': all_flights_data['trips'][trip[1][1][1][0]]['startDateTime'],
                    'arrive_date_time': all_flights_data['trips'][trip[1][1][1][-1]]['endDateTime'],
                    'carrier': all_flights_data['trips'][trip[1][1][1][0]]['carrier'],
                    'flight_number': all_flights_data['trips'][trip[1][1][1][0]]['carrierTripNumber'],
                    'orig_city': all_flights_data['trips'][trip[1][1][1][0]]['from'],
                    'dest_city': all_flights_data['trips'][trip[1][1][1][-1]]['to'],
                    'num_tranship': len(trip[1][1][1]) - 1,
                    'tranship_cities': [all_flights_data['trips'][trip[1][1][1][i]]['to'] for i in
                                        range(len(trip[1][1][1]) - 1)],
                    'total_flight_time': trip[1][0][0],
                }
                best_flight_i_info.append(back_flight_info)
            best_flights_info.append(best_flight_i_info)

        # for i in best_flights_info:
        #     print(f'beat flight: {i}')
    # print(f'Best flights info (get_best_flights_info): {best_flights_info}')
    return best_flights_info


def send_result(best_flights_info, telegram_user):
    if isinstance(best_flights_info[0], dict):
       # if best_flights_info[0]['flight_type'] == 'one way':
        for i in range(len(best_flights_info)):
            if best_flights_info[i]["num_tranship"] == 0:
                with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')),
                                    os.getenv('TELEGRAM_HASH')) as client:
                    client.send_message(telegram_user,
                                        message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.')
            else:
                with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')),
                                    os.getenv('TELEGRAM_HASH')) as client:
                    client.send_message(telegram_user,
                                        message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]} пересадки: {str(*best_flights_info[i]["tranship_cities"])}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.')
    elif isinstance(best_flights_info[0], list):
    #elif best_flights_info[0][0]['flight_type'] == 'round flight':
        for i in range(len(best_flights_info)):
            with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
                client.send_message(telegram_user,
                                    message=f'Перелет {best_flights_info[i][0]["orig_city"]} - {best_flights_info[i][0]["dest_city"]} - {best_flights_info[i][0]["orig_city"]} цена {best_flights_info[i][0]["price"]} руб.:\nТуда:\nавиакомпания {best_flights_info[i][0]["carrier"]} рейс № {best_flights_info[i][0]["flight_number"]}\nвылет: {best_flights_info[i][0]["depart_date_time"]} прибытие: {best_flights_info[i][0]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][0]["tranship_cities"]) if best_flights_info[i][0]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][0]["total_flight_time"]) // 60} ч. {(best_flights_info[i][0]["total_flight_time"]) % 60} мин.\nНазад:\nавиакомпания {best_flights_info[i][1]["carrier"]} рейс № {best_flights_info[i][1]["flight_number"]}\nвылет: {best_flights_info[i][1]["depart_date_time"]} прибытие: {best_flights_info[i][1]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][1]["tranship_cities"]) if best_flights_info[i][1]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][1]["total_flight_time"]) // 60} ч. {(best_flights_info[i][1]["total_flight_time"]) % 60} мин.')


def main():
    with set_connection() as conn:
        delete_depart_date_expired_records(conn, 'flight_search_search', 'flight_search_searcharchive')
        delete_old_records(conn, 'flight_search_search', 'flight_search_searcharchive')
        search_data = get_data(conn, 'flight_search_search')
        city_data = get_data(conn, 'flight_search_citycode')

    keys = ['city_eng', 'city_rus', 'code_eng', 'code_rus']
    city_codes = [dict(zip(keys, city_data[c][1:])) for c in range(len(city_data))]
    for record in search_data:
        dest_city_code = origin_city_code = return_date = depart_date = num_adults = telegram_user = None
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
        search_link_json = f'https://www.onetwotrip.com/_avia-search-proxy/search/v3'
        print(
            f'Перелет {record[1]}-{record[2]} вылет: {depart_date} возвращение: {return_date} пересадок не более: {tranship_limit}')
        all_flights_data = get_flight_data(search_link_json, depart_date, origin_city_code, dest_city_code, return_date)
        transport_var_tranship_limit_filtered = tranship_limit_filter(all_flights_data, tranship_limit)

        if return_date is not None:
            round_flights = round_flights_filter(all_flights_data)
            transport_var_filtered = []
            for i in round_flights:
                if i[0] in transport_var_tranship_limit_filtered and i[1] in transport_var_tranship_limit_filtered:
                    transport_var_filtered.append(i)
            # print(f'transport_var_filtered: {transport_var_filtered}')
        else:
            transport_var_filtered = transport_var_tranship_limit_filtered
        # print(f'Transportation variants filtered (main()): {transport_var_filtered}')
        transp_variant_prices = get_transport_variant_prices(all_flights_data, transport_var_filtered)
        cheapest_transp_variants, best_price = get_cheapest_transport_variants(all_flights_data, transp_variant_prices)
        best_flights_info = get_best_flights_info(all_flights_data, cheapest_transp_variants, best_price, return_date)
        print(f'Best flights info: {best_flights_info}')
        print('')
        send_result(best_flights_info, telegram_user)

        # send_result(or_city=record[1], des_city=record[2], dep_date=best_flights_info[i]['depart_date_time'],
        #             ret_date=best_flights_info[i]['arrive_date_time'], price=best_flights_info[i]['price'],
        #             telegram_user=telegram_user, tranship_limit=tranship_limit)
    return True


if __name__ == '__main__':
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
