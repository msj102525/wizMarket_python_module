import logging
from app.schemas.sub_district import SubDistrict
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_or_create_sub_district(sub_district_data: SubDistrict) -> SubDistrict:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = """
        SELECT sub_district_id, sub_district_name, district_id, city_id
        FROM sub_district
        WHERE sub_district_name = %s AND district_id = %s AND city_id = %s
        """
        cursor.execute(
            select_query,
            (
                sub_district_data.sub_district_name,
                sub_district_data.district_id,
                sub_district_data.city_id,
            ),
        )
        result = cursor.fetchone()

        if result:
            return SubDistrict(
                sub_district_id=result[0],
                sub_district_name=result[1],
                district_id=result[2],
                city_id=result[3],
            )
        else:
            insert_query = """
            INSERT INTO sub_district (sub_district_name, district_id, city_id)
            VALUES (%s, %s, %s)
            """
            cursor.execute(
                insert_query,
                (
                    sub_district_data.sub_district_name,
                    sub_district_data.district_id,
                    sub_district_data.city_id,
                ),
            )
            commit(connection)
            return SubDistrict(
                sub_district_id=cursor.lastrowid,
                sub_district_name=sub_district_data.sub_district_name,
                district_id=sub_district_data.district_id,
                city_id=sub_district_data.city_id,
            )
    except Exception as e:
        rollback(connection)
        raise e
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_or_create_sub_district_id(
    city_id: int, district_id: int, sub_district_name: str
) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = """
        SELECT sub_district_id
        FROM sub_district
        WHERE sub_district_name = %s AND district_id = %s AND city_id = %s
        ;
        """
        cursor.execute(
            select_query,
            (
                sub_district_name,
                district_id,
                city_id,
            ),
        )
        result = cursor.fetchone()

        # logger.info(
        #     f"Executing query: {select_query % (sub_district_name, district_id, city_id)}"
        # )

        if result:
            return result[0]
        else:
            insert_query = """
            INSERT INTO sub_district (sub_district_name, district_id, city_id)
            VALUES (%s, %s, %s)
            ;
            """
            cursor.execute(
                insert_query,
                (
                    sub_district_name,
                    district_id,
                    city_id,
                ),
            )
            commit(connection)
            return cursor.lastrowid
    except Exception as e:
        rollback(connection)
        print(f"get_or_create_sub_district_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_sub_district_id_by(
    city_id: int, district_id: int, sub_district_name: str
) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = """
        SELECT sub_district_id
        FROM sub_district
        WHERE sub_district_name = %s AND district_id = %s AND city_id = %s
        ;
        """
        cursor.execute(
            select_query,
            (
                sub_district_name,
                district_id,
                city_id,
            ),
        )
        result = cursor.fetchone()

        # logger.info(
        #     f"Executing query: {select_query % (sub_district_name, district_id, city_id)}"
        # )

        if result:
            return result[0]
        else:
            return 0
    except Exception as e:
        rollback(connection)
        print(f"get_sub_district_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_sub_district_name_by_sub_district_id(sub_district_id: int) -> str:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = (
            "SELECT sub_district_name FROM sub_district WHERE sub_district_id = %s;"
        )
        cursor.execute(select_query, (sub_district_id,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return ""
    except Exception as e:
        rollback(connection)
        print(f"get_sub_district_name:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


# if __name__ == "__main__":
#     print(get_or_create_sub_district_id(1, 1, "강남동"))
