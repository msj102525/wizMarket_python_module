from app.db.connect import *
from app.schemas.loc_info import UpdateLocInfoApartPrice
import logging


def select_all_region():
    """DB에서 지역 목록을 가져와 city_id, district_id, sub_district_id와 합쳐진 지역명을 포함한 리스트 생성"""
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = """
            SELECT city.city_name, city.city_id,
                   district.district_name, district.district_id,
                   sub_district.sub_district_name, sub_district.sub_district_id
            FROM city
            JOIN district ON city.city_id = district.city_id
            JOIN sub_district ON district.district_id = sub_district.district_id
        """
        cursor.execute(query)
        all_region_list = cursor.fetchall()

        # 키워드 리스트 생성
        keywords = []
        for region in all_region_list:
            # 지역명을 합치기
            keywords.append({
                'city_id': region['city_id'],
                'city_name': region['city_name'],
                'district_id': region['district_id'],
                'district_name': region['district_name'],
                'sub_district_id': region['sub_district_id'],
                'sub_district_name': region['sub_district_name']
            })

    finally:
        close_connection(connection)
    # print(keywords)
    return keywords



# 업데이트
def update_loc_info_apart_price(connection, data):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 업데이트 문
            update_query = """
                UPDATE loc_info
                SET 
                    APART_PRICE = %(apart_price)s
                WHERE 
                    CITY_ID = %(city_id)s AND 
                    DISTRICT_ID = %(district_id)s AND 
                    SUB_DISTRICT_ID = %(sub_district_id)s AND 
                    Y_M = "2024-10-01"
            """

            try:
                # Pydantic 모델로 데이터 검증
                validated_record = UpdateLocInfoApartPrice(**data)
                # 검증된 데이터를 dict로 변환하여 SQL 업데이트
                cursor.execute(update_query, validated_record.dict())
                # 커밋
                connection.commit()

            except ValueError as e:
                logger.error(f"Data Validation Error: {e}")

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()

    finally:
        # 리소스 해제
        cursor.close()
