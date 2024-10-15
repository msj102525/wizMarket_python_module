import logging
import pymysql
import pandas as pd
import os
from app.schemas.city import City
from dotenv import load_dotenv
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def get_or_create_city(city_data: City) -> City:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # 먼저 해당 도시가 존재하는지 확인
        select_query = "SELECT city_id, city_name FROM city WHERE city_name = %s"
        cursor.execute(select_query, (city_data.city_name,))
        result = cursor.fetchone()

        if result:
            # 존재하면 해당 city_id와 name 반환
            return City(city_id=result[0], city_name=result[1])
        else:
            # 존재하지 않으면 새로 삽입 후 city_id 반환
            insert_query = "INSERT INTO city (city_name) VALUES (%s)"
            cursor.execute(insert_query, (city_data.city_name,))
            connection.commit()

            # 새로 삽입된 row의 ID를 포함한 City 스키마를 반환
            return City(city_id=cursor.lastrowid, city_name=city_data.city_name)
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()


def get_or_create_city_id(city_name: str) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = "SELECT city_id FROM city WHERE city_name = %s;"
        cursor.execute(select_query, (city_name,))
        result = cursor.fetchone()

        # logger.info(f"Executing query: {select_query % (city_name)}")

        if result:
            return result[0]
        else:
            insert_query = "INSERT INTO city (city_name) VALUES (%s);"
            cursor.execute(insert_query, (city_name))
            commit()

            return cursor.lastrowid
    except Exception as e:
        rollback(connection)
        print(f"get_or_create_city_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_city_id(city_name: str) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = "SELECT city_id FROM city WHERE city_name = %s;"
        cursor.execute(select_query, (city_name,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return 0
    except Exception as e:
        rollback(connection)
        print(f"get_city_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_city_name_by_city_id(city_id: int) -> str:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = "SELECT city_name FROM city WHERE city_id = %s;"
        cursor.execute(select_query, (city_id,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return ""
    except Exception as e:
        rollback(connection)
        print(f"get_city_name:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


# if __name__ == "__main__":
#     print(get_or_create_city_id("충청북도"))
