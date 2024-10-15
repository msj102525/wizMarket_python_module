from app.schemas.district import District
from app.db.connect import *


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


