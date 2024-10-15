import logging
from app.schemas.sub_district import SubDistrict
from app.db.connect import *

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


