import os

import psycopg2 as ps2
from telethon import TelegramClient
from telethon import connection as tel_connection
from telethon import events, sync


def send_result(best_flights_info, request_data):
    """Отправляет данные о самом дешевом рейсе в Телеграм"""
    if not best_flights_info:
        with TelegramClient(
            "flight_catcher", int(os.getenv("TELEGRAM_API")), os.getenv("TELEGRAM_HASH")
        ) as client:
            client.send_message(
                request_data["telegr_acc"],
                message=f'Перелет {request_data["depart_city"]} - {request_data["dest_city"]} на дату '
                        f'{request_data["depart_date"]} не найден. Измените дату или условия поиска',
            )
        return False

    if isinstance(best_flights_info[0], dict):
        for i in range(len(best_flights_info)):
            if best_flights_info[i]["num_tranship"] == 0:
                with TelegramClient(
                    "flight_catcher",
                    int(os.getenv("TELEGRAM_API")),
                    os.getenv("TELEGRAM_HASH"),
                ) as client:
                    client.send_message(
                        request_data["telegr_acc"],
                        message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.',
                    )
            else:
                with TelegramClient(
                    "flight_catcher",
                    int(os.getenv("TELEGRAM_API")),
                    os.getenv("TELEGRAM_HASH"),
                ) as client:
                    client.send_message(
                        request_data["telegr_acc"],
                        message=f'Перелет {best_flights_info[i]["orig_city"]} - {best_flights_info[i]["dest_city"]} цена {best_flights_info[i]["price"]} руб.: \nавиакомпания {best_flights_info[i]["carrier"]} рейс № {best_flights_info[i]["flight_number"]}\nвылет: {best_flights_info[i]["depart_date_time"]} прибытие: {best_flights_info[i]["arrive_date_time"]} пересадки: {str(*best_flights_info[i]["tranship_cities"])}\nпродолжительность {(best_flights_info[i]["total_flight_time"]) // 60} ч. {(best_flights_info[i]["total_flight_time"]) % 60} мин.',
                    )
    elif isinstance(best_flights_info[0], list):
        for i in range(len(best_flights_info)):
            with TelegramClient(
                "flight_catcher",
                int(os.getenv("TELEGRAM_API")),
                os.getenv("TELEGRAM_HASH"),
            ) as client:
                client.send_message(
                    request_data["telegr_acc"],
                    message=f'Перелет {best_flights_info[i][0]["orig_city"]} - {best_flights_info[i][0]["dest_city"]} - {best_flights_info[i][0]["orig_city"]} цена {best_flights_info[i][0]["price"]} руб.:\nТуда:\nавиакомпания {best_flights_info[i][0]["carrier"]} рейс № {best_flights_info[i][0]["flight_number"]}\nвылет: {best_flights_info[i][0]["depart_date_time"]} прибытие: {best_flights_info[i][0]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][0]["tranship_cities"]) if best_flights_info[i][0]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][0]["total_flight_time"]) // 60} ч. {(best_flights_info[i][0]["total_flight_time"]) % 60} мин.\nНазад:\nавиакомпания {best_flights_info[i][1]["carrier"]} рейс № {best_flights_info[i][1]["flight_number"]}\nвылет: {best_flights_info[i][1]["depart_date_time"]} прибытие: {best_flights_info[i][1]["arrive_date_time"]} пересадки: {str(*best_flights_info[i][1]["tranship_cities"]) if best_flights_info[i][1]["num_tranship"] != 0 else "нет"}\nпродолжительность {(best_flights_info[i][1]["total_flight_time"]) // 60} ч. {(best_flights_info[i][1]["total_flight_time"]) % 60} мин.',
                )
    return True


def set_connection():
    try:
        db_connection = ps2.connect(
            host=os.getenv("db_host"),
            user=os.getenv("db_user"),
            password=os.getenv("db_password"),
            port=os.getenv("db_port"),
            database=os.getenv("db_name"),
        )
        db_connection.autocommit = True
        return db_connection

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
        raise _ex
