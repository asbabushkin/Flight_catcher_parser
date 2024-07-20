import schedule
from dotenv import load_dotenv

from data import citycode_keys, search_keys, search_link_json
from injektors import clean_expired_flights, clean_expired_search
from maintainers import send_result, set_connection
from processors import (filter_transfer_lim, get_best_flights_descr,
                        get_cheapest_journeys, get_journey_prices)
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
        transfer_limit = request_data["max_transhipments"]

        print(
            f'Перелет {request_data["depart_city"]}-{request_data["dest_city"]} вылет: '
            f'{request_data["depart_date"]} возвращение: {request_data["return_date"]} '
            f"пересадок не более: {transfer_limit}"
        )
        all_flights = get_flight_data(search_link_json, request_data, city_codes)
        if not all_flights:
            send_result(None, request_data)
            continue

        flights_transfer_filtered = filter_transfer_lim(all_flights, transfer_limit)
        if not flights_transfer_filtered:
            send_result(None, request_data)
            continue

        journey_prices = get_journey_prices(all_flights, flights_transfer_filtered)
        cheapest_journeys = get_cheapest_journeys(all_flights, journey_prices)
        best_flights_info = get_best_flights_descr(all_flights, cheapest_journeys)
        send_result(best_flights_info, request_data)


if __name__ == "__main__":
    schedule.every().minute.do(main)
    while True:
        schedule.run_pending()
