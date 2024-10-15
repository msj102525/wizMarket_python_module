import logging
from app.schemas.district import District
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def get_or_create_district(district_data: District) -> District:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # 해당 구역이 존재하는지 확인
        select_query = "SELECT district_id, district_name, city_id FROM district WHERE district_name = %s AND city_id = %s"
        cursor.execute(select_query, (district_data.district_name, district_data.city_id))
        result = cursor.fetchone()

        if result:
            # 존재하면 해당 district_id와 name 반환
            return District(district_id=result[0], district_name=result[1], city_id=result[2])
        else:
            # 존재하지 않으면 새로 삽입 후 district_id 반환
            insert_query = (
                "INSERT INTO district (district_name, city_id) VALUES (%s, %s)"
            )
            cursor.execute(insert_query, (district_data.district_name, district_data.city_id))
            commit(connection)  # 트랜잭션 커밋

            # 새로 삽입된 row의 ID를 포함한 District 스키마를 반환
            return District(
                district_id=cursor.lastrowid,
                district_name=district_data.district_name,
                city_id=district_data.city_id,
            )
    except Exception as e:
        rollback(connection)  # 예외 발생 시 롤백
        raise e
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_or_create_district_id(city_id: int, district_name: str) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = "SELECT district_id FROM district WHERE district_name = %s AND city_id = %s;"
        cursor.execute(select_query, (district_name, city_id))
        result = cursor.fetchone()

        # logger.info(f"Executing query: {select_query % (district_name, city_id)}")

        if result:
            return result[0]
        else:
            insert_query = (
                "INSERT INTO district (district_name, city_id) VALUES (%s, %s);"
            )
            cursor.execute(insert_query, (district_name, city_id))
            commit(connection)

            return cursor.lastrowid
    except Exception as e:
        rollback(connection)
        print(f"get_or_create_district_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_district_id(city_id: int, district_name: str) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = "SELECT district_id FROM district WHERE district_name = %s AND city_id = %s;"
        cursor.execute(select_query, (district_name, city_id))
        result = cursor.fetchone()

        # logger.info(f"Executing query: {select_query % (district_name, city_id)}")

        if result:
            return result[0]
        else:
            return 0
    except Exception as e:
        rollback(connection)
        print(f"get_district_id:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_district_name_by_district_id(district_id: int) -> str:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = "SELECT district_name FROM district WHERE district_id = %s;"
        cursor.execute(select_query, (district_id,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return ""
    except Exception as e:
        rollback(connection)
        print(f"get_district_name:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


# if __name__ == "__main__":
#     print(get_or_create_district_id(1, "강릉시"))
