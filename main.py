"""
Implemented using telethon & schedule
"""
import os
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


def get_column_names(db_connection, table):
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM %(table_name)s LIMIT 0;", {"table_name": AsIs(table)})
    return [desc[0] for desc in cursor.description]


def del_dep_date_expired_rec(db_connection, table, archive):
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, phone_num, email) SELECT * FROM %(table_name)s WHERE depart_date < CURRENT_DATE;",
        {"archive": AsIs(archive), "table_name": AsIs(table)})
    cursor.execute("DELETE FROM %(table_name)s WHERE depart_date < CURRENT_DATE;",
                   {"table_name": AsIs(table)})
    return None


def del_old_rec(db_connection, table, archive):
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, phone_num, email) SELECT * FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 72;",
        {"archive": AsIs(archive), "table_name": AsIs(table)})
    cursor.execute("DELETE FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 72;",
                   {"table_name": AsIs(table)})


def get_flight_data(url, request_data, city_codes):
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

    origin_city_code = dest_city_code = ''
    for i in city_codes:
        if i['city_rus'] == request_data['depart_city']:
            origin_city_code = i['code_eng']
            break

    for i in city_codes:
        if i['city_rus'] == request_data['dest_city']:
            dest_city_code = i['code_eng']
            break

    params = {
        'route': f'{str(request_data["depart_date"].day).rjust(2, "0") + str(request_data["depart_date"].month).rjust(2, "0")}{origin_city_code}{dest_city_code}{str(request_data["return_date"].day).rjust(2, "0") + str(request_data["return_date"].month).rjust(2, "0") if request_data["return_date"] is not None else ""}',
        'ad': request_data["num_adults"],
        'cn': request_data["num_children"],
        'in': request_data["num_infants"],
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
    if len(all_flights_data['prices']) == 0:
        return {}
    else:
        return all_flights_data


def tranship_lim_filt(all_flights_data, tranship_limit):
    transport_variants_tranship_limit_filtered = []
    for item in all_flights_data['transportationVariants']:
        if len(all_flights_data['transportationVariants'][item]['tripRefs']) <= tranship_limit + 1:
            transport_variants_tranship_limit_filtered.append(item)
    return transport_variants_tranship_limit_filtered


def filter_round_flights(all_flights_data):
    transport_variants_round_flight_filtered = []
    for item in all_flights_data['prices']:
        if len(all_flights_data['prices'][item]['transportationVariantIds']) == 2:
            transport_variants_round_flight_filtered.append(
                all_flights_data['prices'][item]['transportationVariantIds'])
    return transport_variants_round_flight_filtered


def get_transp_var_prices(all_flights_data, transport_var_filtered):
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
    return transp_variant_prices


def get_cheapest_transp_vars(all_flights_data, transp_variant_prices):
    best_price = min(transp_variant_prices.values())
    cheapest_transport_var_id = []
    for key, value in transp_variant_prices.items():
        if value == best_price:
            cheapest_transport_var_id.append(key)
    cheapest_transp_variants = []
    for transp_id in cheapest_transport_var_id:
        for item in all_flights_data['transportationVariants']:
            # one way flight
            if isinstance(transp_id, str):
                if item == transp_id:
                    trip_ids = []
                    for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                        trip_ids.append(all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                    cheapest_transp_variants.append(
                        [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                         trip_ids])
            # round flight
            elif isinstance(transp_id, tuple):
                if transp_id[0] == item:
                    lst_forvard_way = []
                    for i in range(len(all_flights_data['transportationVariants'][item]['tripRefs'])):
                        lst_forvard_way.append(
                            all_flights_data['transportationVariants'][item]['tripRefs'][i]['tripId'])
                    lst_forvard_way_and_time = list(
                        [all_flights_data['transportationVariants'][item]['totalJourneyTimeMinutes'],
                         lst_forvard_way])
                    for j in all_flights_data['transportationVariants']:
                        if transp_id[1] == j:
                            lst_back_way = []
                            for i in range(len(all_flights_data['transportationVariants'][j]['tripRefs'])):
                                lst_back_way.append(
                                    all_flights_data['transportationVariants'][j]['tripRefs'][i]['tripId'])
                            lst_back_way_and_time = list(
                                [all_flights_data['transportationVariants'][j]['totalJourneyTimeMinutes'],
                                 lst_back_way])
                    cheapest_transp_variants.append([lst_forvard_way_and_time[0] + lst_back_way_and_time[0],
                                                     [lst_forvard_way_and_time, lst_back_way_and_time]])
    return cheapest_transp_variants, best_price


def get_best_flights_info(all_flights_data, cheapest_transp_variants, best_price, return_date):
    if return_date is None:
        # one way flight
        best_flights_info = []
        for trip in cheapest_transp_variants:
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
                'tranship_cities': None if len(trip[1]) == 1 else [all_flights_data['trips'][trip[1][i]]['to'] for i in
                                                                   range(len(trip[1]) - 1)],
                'total_flight_time': trip[0],
            }
            best_flights_info.append(flight_info)

    # round trip
    else:
        best_flights_info = []
        for trip in cheapest_transp_variants:
            best_flight_i_info = []
            # round trip forward flight
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
                'tranship_cities': None if len(trip[1][0][1]) == 1 else [
                    all_flights_data['trips'][trip[1][0][1][i]]['to'] for i in range(len(trip[1][0][1]) - 1)],
                'total_flight_time': trip[1][0][0],
            }

            # round trip back flight
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
                'tranship_cities': None if len(trip[1][1][1]) == 1 else [
                    all_flights_data['trips'][trip[1][1][1][i]]['to'] for i in range(len(trip[1][1][1]) - 1)],
                'total_flight_time': trip[1][1][0],
            }
            best_flight_i_info.extend([first_flight_info, back_flight_info])
            best_flights_info.append(best_flight_i_info)
    return best_flights_info


def send_result(best_flights_info, request_data, empty_data):
    if empty_data:
        with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')), os.getenv('TELEGRAM_HASH')) as client:
            client.send_message(request_data['telegr_acc'],
                                message=f'Перелет {request_data["depart_city"]} - {request_data["dest_city"]} на дату {request_data["depart_date"]} не найден.')
        return False
    else:
        if isinstance(best_flights_info[0], dict):
            for i in range(len(best_flights_info)):
                if best_flights_info[i]["num_tranship"] == 0:
                    with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')),
                                        os.getenv('TELEGRAM_HASH')) as client:
                        client.send_message(request_data['telegr_acc'],
                                            message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.')
                else:
                    with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')),
                                        os.getenv('TELEGRAM_HASH')) as client:
                        client.send_message(request_data['telegr_acc'],
                                            message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]} пересадки: {str(*best_flights_info[i]["tranship_cities"])}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.')
        elif isinstance(best_flights_info[0], list):
            for i in range(len(best_flights_info)):
                with TelegramClient('flight_catcher', int(os.getenv('TELEGRAM_API')),
                                    os.getenv('TELEGRAM_HASH')) as client:
                    client.send_message(request_data['telegr_acc'],
                                        message=f'Перелет {best_flights_info[i][0]["orig_city"]} - {best_flights_info[i][0]["dest_city"]} - {best_flights_info[i][0]["orig_city"]} цена {best_flights_info[i][0]["price"]} руб.:\nТуда:\nавиакомпания {best_flights_info[i][0]["carrier"]} рейс № {best_flights_info[i][0]["flight_number"]}\nвылет: {best_flights_info[i][0]["depart_date_time"]} прибытие: {best_flights_info[i][0]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][0]["tranship_cities"]) if best_flights_info[i][0]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][0]["total_flight_time"]) // 60} ч. {(best_flights_info[i][0]["total_flight_time"]) % 60} мин.\nНазад:\nавиакомпания {best_flights_info[i][1]["carrier"]} рейс № {best_flights_info[i][1]["flight_number"]}\nвылет: {best_flights_info[i][1]["depart_date_time"]} прибытие: {best_flights_info[i][1]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][1]["tranship_cities"]) if best_flights_info[i][1]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][1]["total_flight_time"]) // 60} ч. {(best_flights_info[i][1]["total_flight_time"]) % 60} мин.')
        return True


def main():
    with set_connection() as conn:
        del_dep_date_expired_rec(conn, 'flight_search_search', 'flight_search_searcharchive')
        del_old_rec(conn, 'flight_search_search', 'flight_search_searcharchive')
        search_data = get_data(conn, 'flight_search_search')
        city_data = get_data(conn, 'flight_search_citycode')
        search_colnames = get_column_names(conn, 'flight_search_search')
        citycode_colnames = get_column_names(conn, 'flight_search_citycode')

    city_codes = [dict(zip(citycode_colnames, city_data[i])) for i in range(len(city_data))]
    for record in search_data:
        request_data = dict(zip(search_colnames, record))
        tranship_limit = request_data['max_transhipments']
        search_link_json = f'https://www.onetwotrip.com/_avia-search-proxy/search/v3'
        print(
            f'Перелет {request_data["depart_city"]}-{request_data["dest_city"]} вылет: {request_data["depart_date"]} возвращение: {request_data["return_date"]} пересадок не более: {tranship_limit}')
        all_flights_data = get_flight_data(search_link_json, request_data, city_codes)
        if len(all_flights_data) == 0:
            send_result(None, request_data, empty_data=True)
            continue
        transp_var_tranship_lim_filtrd = tranship_lim_filt(all_flights_data, tranship_limit)
        if request_data['return_date'] is not None:
            round_flights = filter_round_flights(all_flights_data)
            transp_var_filtrd = []
            for i in round_flights:
                if i[0] in transp_var_tranship_lim_filtrd and i[1] in transp_var_tranship_lim_filtrd:
                    transp_var_filtrd.append(i)
        else:
            transp_var_filtrd = transp_var_tranship_lim_filtrd
        transp_var_prices = get_transp_var_prices(all_flights_data, transp_var_filtrd)
        cheapest_transp_vars, best_price = get_cheapest_transp_vars(all_flights_data, transp_var_prices)
        best_flights_info = get_best_flights_info(all_flights_data, cheapest_transp_vars, best_price,
                                                  request_data["return_date"])
        send_result(best_flights_info, request_data, empty_data=False)
    return True


if __name__ == '__main__':
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
