def filter_transfer_lim(all_flights, tranship_limit):
    """Фильтрует рейсы по количеству пересадок"""
    flights_transfer_filtered = []
    for item in all_flights["transportationVariants"]:
        if (
            len(all_flights["transportationVariants"][item]["tripRefs"])
            < tranship_limit + 2
        ):
            flights_transfer_filtered.append(item)
    if not flights_transfer_filtered:
        return None
    return flights_transfer_filtered

def get_journey_prices(all_flights, transport_var_filtered):
# region func_description
    """
    Returns transportationVariant key and price for every journe.
    transp_prices example {'RTJWn8': 12035.21, 'CYU9vu': 19232.2, 'b1W7wQ': 12383.96}
    """
# endregion
    transp_prices = dict()
    for item in all_flights["prices"]:
        if (
            all_flights["prices"][item]["transportationVariantIds"][0]
            in transport_var_filtered
        ):
            transp_prices[
                all_flights["prices"][item]["transportationVariantIds"][0]
            ] = all_flights["prices"][item]["totalAmount"]

    return transp_prices


def get_cheapest_journeys(all_flights, journey_prices):
# region func_description
    """
    Returns cheapest journey variants and their prices.
    journey_prices example {'RTJWn8': 12035.21, 'CYU9vu': 19232.2, 'b1W7wQ': 12383.96}"
    """
# endregion
    best_price = min(journey_prices.values())
    cheapest_journey_id = []
    for key, value in journey_prices.items():
        if value == best_price:
            cheapest_journey_id.append(key)
    cheapest_transp_variants = []
    for transp_id in cheapest_journey_id:
        for item in all_flights["transportationVariants"]:
            if item == transp_id:
                trip_ids = []
                for i in range(
                    len(
                        all_flights["transportationVariants"][item]["tripRefs"]
                    )
                ):
                    trip_ids.append(
                        all_flights["transportationVariants"][item][
                            "tripRefs"
                        ][i]["tripId"]
                    )
                cheapest_transp_variants.append(
                    [
                        all_flights["transportationVariants"][item][
                            "totalJourneyTimeMinutes"
                        ],
                        trip_ids,
                    ]
                )

    return cheapest_transp_variants, best_price


def get_best_flights(
    all_flights, cheapest_transp_variants, best_price
):
    """Возвращает данные о самых дешевых рейсах"""

    res_lst = []
    for trip in cheapest_transp_variants:
        flight_info = {
            "flight_type": "one way",
            "price": best_price,
            "depart_date_time": all_flights["trips"][trip[1][0]][
                "startDateTime"
            ],
            "arrive_date_time": all_flights["trips"][trip[1][-1]][
                "endDateTime"
            ],
            "carrier": all_flights["trips"][trip[1][0]]["carrier"],
            "flight_number": all_flights["trips"][trip[1][0]][
                "carrierTripNumber"
            ],
            "orig_city": all_flights["trips"][trip[1][0]]["from"],
            "dest_city": all_flights["trips"][trip[1][-1]]["to"],
            "num_tranship": len(trip[1]) - 1,
            "tranship_cities": (
                None
                if len(trip[1]) == 1
                else [
                    all_flights["trips"][trip[1][i]]["to"]
                    for i in range(len(trip[1]) - 1)
                ]
            ),
            "total_flight_time": trip[0],
        }
        res_lst.append(flight_info)

    return res_lst

