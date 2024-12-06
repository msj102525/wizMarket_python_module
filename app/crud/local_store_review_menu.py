from app.db.connect import *
from app.schemas.loc_store import UpdateLocalStoreReview
import logging

def test_store_info():
    connection = None
    cursor = None
    try:
        connection = get_re_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            SELECT 
                city_name, 
                district_name, 
                sub_district_name,
                STORE_NAME,
                STORE_BUSINESS_NUMBER
            FROM REPORT
            WHERE STORE_BUSINESS_NUMBER = 'JS0001'
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


def store_info():
    connection = None
    cursor = None
    try:
        connection = get_re_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            SELECT 
                city_name, 
                district_name, 
                sub_district_name,
                STORE_NAME,
                STORE_BUSINESS_NUMBER
            FROM REPORT
            WHERE city_name = '강원특별자치도'
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


def update_store_review(connection, data):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    # print(data)
    try:
        if connection.open:
            # SQL 인서트 문
            update_query = """
                UPDATE local_store 
                SET 
                    KAKAO_REVIEW_SCORE = %(kakao_review_score)s, 
                    KAKAO_REVIEW_COUNT = %(kakao_review_count)s,
                    MENU_1 = %(menu_1)s,
                    MENU_1_PRICE = %(menu_1_price)s,
                    MENU_2 = %(menu_2)s,
                    MENU_2_PRICE = %(menu_2_price)s,
                    MENU_3 = %(menu_3)s,
                    MENU_3_PRICE = %(menu_3_price)s
                WHERE STORE_BUSINESS_NUMBER = %(store_business_number)s
            """

            try:
                # Pydantic 모델로 데이터 검증
                validated_record = UpdateLocalStoreReview(**data)
                # 검증된 데이터를 dict로 변환하여 SQL 인서트
                cursor.execute(update_query, validated_record.dict())
                # 커밋
                connection.commit()
                print(f"성공: {data['store_business_number']}, {data['kakao_review_score']}, {data['menu_1']}")
            except ValueError as e:
                print(f"Data Validation Error: {e}")

    except pymysql.MySQLError as e:
        print(f"MySQL Error: {e}")
        connection.rollback()  # 롤백 처리
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

    finally:
        # 리소스 해제
        cursor.close()



