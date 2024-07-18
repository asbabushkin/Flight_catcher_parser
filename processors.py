def filter_transfer_lim(all_flights_data, tranship_limit):
    """Фильтрует рейсы по количеству пересадок"""
    flights_transfer_filtered = []
    for item in all_flights_data["transportationVariants"]:
        if (
            len(all_flights_data["transportationVariants"][item]["tripRefs"])
            < tranship_limit + 2
        ):
            flights_transfer_filtered.append(item)
    if not flights_transfer_filtered:
        return None
    return flights_transfer_filtered

def get_transp_var_prices(all_flights, transport_var_filtered):
    """Возвращает цены для каждого варианта перелета"""
    transp_variant_prices = dict()
    for item in all_flights["prices"]:
        if (
            all_flights["prices"][item]["transportationVariantIds"][0]
            in transport_var_filtered
        ):
            transp_variant_prices[
                all_flights["prices"][item]["transportationVariantIds"][0]
            ] = all_flights["prices"][item]["totalAmount"]

    return transp_variant_prices


def get_cheapest_transp_vars(all_flights_data, transp_variant_prices):
    """Возвращает самые дешевые варианты перелетов и их цену"""
    try:
        best_price = min(transp_variant_prices.values())
    except ValueError:
        print("Перелеты не найдены. Измените условия поиска")
    cheapest_transport_var_id = []
    for key, value in transp_variant_prices.items():
        if value == best_price:
            cheapest_transport_var_id.append(key)
    cheapest_transp_variants = []
    for transp_id in cheapest_transport_var_id:
        for item in all_flights_data["transportationVariants"]:
            # one way flight
            if isinstance(transp_id, str):
                if item == transp_id:
                    trip_ids = []
                    for i in range(
                        len(
                            all_flights_data["transportationVariants"][item]["tripRefs"]
                        )
                    ):
                        trip_ids.append(
                            all_flights_data["transportationVariants"][item][
                                "tripRefs"
                            ][i]["tripId"]
                        )
                    cheapest_transp_variants.append(
                        [
                            all_flights_data["transportationVariants"][item][
                                "totalJourneyTimeMinutes"
                            ],
                            trip_ids,
                        ]
                    )
            # round flight
            elif isinstance(transp_id, tuple):
                if transp_id[0] == item:
                    lst_forvard_way = []
                    for i in range(
                        len(
                            all_flights_data["transportationVariants"][item]["tripRefs"]
                        )
                    ):
                        lst_forvard_way.append(
                            all_flights_data["transportationVariants"][item][
                                "tripRefs"
                            ][i]["tripId"]
                        )
                    lst_forvard_way_and_time = list(
                        [
                            all_flights_data["transportationVariants"][item][
                                "totalJourneyTimeMinutes"
                            ],
                            lst_forvard_way,
                        ]
                    )
                    for j in all_flights_data["transportationVariants"]:
                        if transp_id[1] == j:
                            lst_back_way = []
                            for i in range(
                                len(
                                    all_flights_data["transportationVariants"][j][
                                        "tripRefs"
                                    ]
                                )
                            ):
                                lst_back_way.append(
                                    all_flights_data["transportationVariants"][j][
                                        "tripRefs"
                                    ][i]["tripId"]
                                )
                            lst_back_way_and_time = list(
                                [
                                    all_flights_data["transportationVariants"][j][
                                        "totalJourneyTimeMinutes"
                                    ],
                                    lst_back_way,
                                ]
                            )
                    cheapest_transp_variants.append(
                        [
                            lst_forvard_way_and_time[0] + lst_back_way_and_time[0],
                            [lst_forvard_way_and_time, lst_back_way_and_time],
                        ]
                    )
    return cheapest_transp_variants, best_price


def get_best_flights_info(
    all_flights_data, cheapest_transp_variants, best_price
):
    """Возвращает данные о самых дешевых рейсах"""

    best_flights_info = []
    for trip in cheapest_transp_variants:
        flight_info = {
            "flight_type": "one way",
            "price": best_price,
            "depart_date_time": all_flights_data["trips"][trip[1][0]][
                "startDateTime"
            ],
            "arrive_date_time": all_flights_data["trips"][trip[1][-1]][
                "endDateTime"
            ],
            "carrier": all_flights_data["trips"][trip[1][0]]["carrier"],
            "flight_number": all_flights_data["trips"][trip[1][0]][
                "carrierTripNumber"
            ],
            "orig_city": all_flights_data["trips"][trip[1][0]]["from"],
            "dest_city": all_flights_data["trips"][trip[1][-1]]["to"],
            "num_tranship": len(trip[1]) - 1,
            "tranship_cities": (
                None
                if len(trip[1]) == 1
                else [
                    all_flights_data["trips"][trip[1][i]]["to"]
                    for i in range(len(trip[1]) - 1)
                ]
            ),
            "total_flight_time": trip[0],
        }
        best_flights_info.append(flight_info)

    return best_flights_info

