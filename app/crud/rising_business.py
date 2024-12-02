import logging
from pymysql import MySQLError
from typing import List, Optional

import pymysql
from app.schemas.rising_business import (
    RisingBusinessInsert,
    RisingBusinessOutput,
)
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def insert_rising_business(data_list: List[RisingBusinessInsert]):
    connection = get_db_connection()
    cursor = None
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO RISING_BUSINESS (city_id, district_id, sub_district_id, biz_main_category_id, biz_sub_category_id, biz_detail_category_id, growth_rate, sub_district_rank, y_m)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            for data in data_list:
                values = (
                    data.city_id,
                    data.district_id,
                    data.sub_district_id,
                    data.biz_main_category_id,
                    data.biz_sub_category_id,
                    data.biz_detail_category_id,
                    data.growth_rate if data.growth_rate is not None else 0.0,
                    data.sub_district_rank if data.sub_district_rank is not None else 0,
                    data.y_m,
                )
                logger.info(f"Executing query: {insert_query % values}")
                cursor.execute(insert_query, values)

            commit(connection)

    except MySQLError as e:
        print(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        print(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)
