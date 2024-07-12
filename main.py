"""
Implemented using telethon & schedule
"""

import schedule
from dotenv import load_dotenv

from data import citycode_keys, search_keys, search_link_json
from injektors import clean_expired_flights, clean_expired_search
from maintainers import send_result, set_connection
from processors import (
    filter_round_flights,
    get_best_flights_info,
    get_cheapest_transp_vars,
    get_transp_var_prices,
    filter_transfer_lim,
)
from selektors import get_data, get_flight_data

load_dotenv()


def main():
    with set_connection() as conn:
        clean_expired_flights(
            conn, "flight_search_search", "flight_search_searcharchive"
        )
        clean_expired_search(
            conn, "flight_search_search", "flight_search_searcharchive"
        )
        search_data = get_data(conn, "flight_search_search")
        city_data = get_data(conn, "flight_search_citycode")

    city_codes = [dict(zip(citycode_keys, city_data[i])) for i in range(len(city_data))]
    for record in search_data:
        request_data = dict(zip(search_keys, record))
        tranship_limit = request_data["max_transhipments"]

        print(
            f'Перелет {request_data["depart_city"]}-{request_data["dest_city"]} вылет: {request_data["depart_date"]} возвращение: {request_data["return_date"]} пересадок не более: {tranship_limit}'
        )
        all_flights_data = get_flight_data(search_link_json, request_data, city_codes)
        if not all_flights_data:
            send_result(None, request_data)
            continue
        transp_var_tranship_lim_filtrd = filter_transfer_lim(
            all_flights_data, tranship_limit
        )
        if request_data["return_date"] is not None:
            round_flights = filter_round_flights(all_flights_data)
            transp_var_filtrd = []
            for i in round_flights:
                if (
                    i[0] in transp_var_tranship_lim_filtrd
                    and i[1] in transp_var_tranship_lim_filtrd
                ):
                    transp_var_filtrd.append(i)
        else:
            transp_var_filtrd = transp_var_tranship_lim_filtrd
        transp_var_prices = get_transp_var_prices(all_flights_data, transp_var_filtrd)
        cheapest_transp_vars, best_price = get_cheapest_transp_vars(
            all_flights_data, transp_var_prices
        )
        best_flights_info = get_best_flights_info(
            all_flights_data,
            cheapest_transp_vars,
            best_price,
            request_data["return_date"],
        )
        send_result(best_flights_info, request_data, empty_data=False)
    return True


if __name__ == "__main__":
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
