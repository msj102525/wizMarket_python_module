from app.db.connect import *
from app.schemas.loc_info import InsertLocInfo
import logging

def get_all_region_id():
    """
    모든 city_id와 district_id, sub_district_id 쌍을 가져옴
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 모든 city_id와 district_id 쌍을 가져오는 쿼리
        query = """
            SELECT 
                   city.city_name AS city_name, 
                   city.city_id AS city_id, 
                   district.district_name AS district_name, 
                   district.district_id AS district_id, 
                   sub_district.sub_district_name AS sub_district_name,
                   sub_district.sub_district_id AS sub_district_id
            FROM sub_district
            JOIN city ON sub_district.city_id = city.city_id
            JOIN district ON sub_district.district_id = district.district_id
        """

        cursor.execute(query)
        result = cursor.fetchall()

        # city_id, district_id 쌍을 반환
        return result

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)



def fetch_keywords_from_db():
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
            region_name = f"{region['city_name']} {region['district_name']} {region['sub_district_name']}"
            keywords.append({
                'city_id': region['city_id'],
                'district_id': region['district_id'],
                'sub_district_id': region['sub_district_id'],
                'keyword': region_name
            })

    finally:
        close_connection(connection)
    # print(keywords)
    return keywords



def fetch_test_keywords_from_db():
    # 특정 테스트 데이터를 수동으로 설정
    test_keywords = [
        {
            'city_id': 1,
            'district_id': 1,
            'sub_district_id': 1,
            'keyword': '강원특별자치도 강릉시 강남동'
        },
        # {
        #     'city_id': 9,
        #     'district_id': 125,
        #     'sub_district_id': 1967,
        #     'keyword': '서울특별시 강남구 역삼1동'
        # },
        # {
        #     'city_id': 3,
        #     'district_id': 58,
        #     'sub_district_id': 907,
        #     'keyword': '경상남도 양산시 물금읍'
        # },
        # {
        #     'city_id': 9,
        #     'district_id': 136,
        #     'sub_district_id': 2141,
        #     'keyword': '서울특별시 동작구 상도1동'
        # },
    ]
    return test_keywords


# 12월 시/군/구 id 값 조회
def fetch_no_sub_district_id():
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = """
            SELECT 
                sub_district_id
            FROM loc_info
            where loc_info.y_m = '2024-12-01'
        """
        cursor.execute(query)
        loc_info_sub_district_id_list = cursor.fetchall()

    finally:
        close_connection(connection)
    # print(keywords)
    return loc_info_sub_district_id_list

# 기존 시/군/구 id 값 조회
def fetch_sub_district_id():
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = """
            SELECT 
                sub_district_id
            FROM sub_district
        """
        cursor.execute(query)
        all_sub_district_id_list = cursor.fetchall()

    finally:
        close_connection(connection)
    # print(keywords)
    return all_sub_district_id_list


# 빠진 id 값으로 지역 list 생성
def fetch_missing_list(sub_district_ids):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = f"""
            SELECT city.city_name, city.city_id,
                   district.district_name, district.district_id,
                   sub_district.sub_district_name, sub_district.sub_district_id
            FROM city
            JOIN district ON city.city_id = district.city_id
            JOIN sub_district ON district.district_id = sub_district.district_id
            WHERE sub_district.sub_district_id IN ({','.join(['%s'] * len(sub_district_ids))})
        """
        cursor.execute(query, sub_district_ids)
        all_region_list = cursor.fetchall()

        # 키워드 리스트 생성
        keywords = []
        for region in all_region_list:
            # 지역명을 합치기
            region_name = f"{region['city_name']} {region['district_name']} {region['sub_district_name']}"
            keywords.append({
                'city_id': region['city_id'],
                'district_id': region['district_id'],
                'sub_district_id': region['sub_district_id'],
                'keyword': region_name
            })

    finally:
        close_connection(connection)
    return keywords



# null 목록 조회
def fetch_null_keywords_from_db():
    """DB에서 지역 목록을 가져와 city_id, district_id, sub_district_id와 합쳐진 지역명을 포함한 리스트 생성"""
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = """
            SELECT 
                city.city_name, city.city_id,
                district.district_name, district.district_id,
                sub_district.sub_district_name, sub_district.sub_district_id
            FROM city
            JOIN district ON city.city_id = district.city_id
            JOIN sub_district ON district.district_id = sub_district.district_id
            JOIN loc_info ON sub_district.sub_district_id = loc_info.sub_district_id
            WHERE loc_info.shop IS NULL 
            AND loc_info.y_m = '2024-12-01'
        """
        cursor.execute(query)
        all_region_list = cursor.fetchall()

        # 키워드 리스트 생성
        keywords = []
        for region in all_region_list:
            # 지역명을 합치기
            region_name = f"{region['city_name']} {region['district_name']} {region['sub_district_name']}"
            keywords.append({
                'city_id': region['city_id'],
                'district_id': region['district_id'],
                'sub_district_id': region['sub_district_id'],
                'keyword': region_name
            })

    finally:
        close_connection(connection)
    # print(keywords)
    return keywords


def insert_loc_info_data(connection, data):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 인서트 문
            insert_query = """
                INSERT INTO loc_info (
                    CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, 
                    SHOP, MOVE_POP, SALES, WORK_POP, INCOME, 
                    SPEND, HOUSE, RESIDENT, CREATED_AT, UPDATED_AT, Y_M, REFERENCE_ID) 
                VALUES (
                    %(city_id)s, %(district_id)s, %(sub_district_id)s,
                    %(shop)s, %(move_pop)s, %(sales)s, %(work_pop)s, %(income)s,
                    %(spend)s, %(house)s, %(resident)s, %(created_at)s, %(updated_at)s, %(y_m)s, %(reference_id)s)
            """

            try:
                # Pydantic 모델로 데이터 검증
                validated_record = InsertLocInfo(**data)
                # 검증된 데이터를 dict로 변환하여 SQL 인서트
                cursor.execute(insert_query, validated_record.dict())
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


def update_null_loc_info_data(connection, data):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 업데이트 문
            update_query = """
                UPDATE loc_info
                SET 
                    SHOP = %(shop)s,
                    MOVE_POP = %(move_pop)s,
                    SALES = %(sales)s,
                    WORK_POP = %(work_pop)s,
                    INCOME = %(income)s,
                    SPEND = %(spend)s,
                    HOUSE = %(house)s,
                    RESIDENT = %(resident)s,
                    CREATED_AT = %(created_at)s,
                    UPDATED_AT = %(updated_at)s,
                    REFERENCE_ID = %(reference_id)s
                WHERE 
                    CITY_ID = %(city_id)s AND 
                    DISTRICT_ID = %(district_id)s AND 
                    SUB_DISTRICT_ID = %(sub_district_id)s AND 
                    Y_M = %(y_m)s
            """

            try:
                # Pydantic 모델로 데이터 검증
                validated_record = InsertLocInfo(**data)
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




