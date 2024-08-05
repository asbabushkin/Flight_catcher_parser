from psycopg2.extensions import AsIs


def clean_expired_search(db_connection, table, archive):
    """Переносит в архив запросы, созданные более 3 суток назад и удаляет их из поиска"""
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, "
        "return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, "
        "phone_num, email) SELECT * FROM %(table_name)s "
        "WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 720;",
        {"archive": AsIs(archive), "table_name": AsIs(table)},
    )
    cursor.execute(
        "DELETE FROM %(table_name)s WHERE EXTRACT(EPOCH FROM now() - search_init_date)/3600 > 720;",
        {"table_name": AsIs(table)},
    )


def clean_expired_flights(db_connection, table, archive):
    """Переносит в архив запросы с истекшей датой вылета и удаляет их из поиска"""

    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO %(archive)s (old_id, depart_city, dest_city, max_transhipments, depart_date, "
        "return_date, num_adults, num_children, num_infants, luggage, search_init_date, telegr_acc, "
        "phone_num, email) SELECT * FROM %(table_name)s "
        "WHERE depart_date < CURRENT_DATE;",
        {"archive": AsIs(archive), "table_name": AsIs(table)},
    )
    cursor.execute(
        "DELETE FROM %(table_name)s WHERE depart_date < CURRENT_DATE;",
        {"table_name": AsIs(table)},
    )
    return None
