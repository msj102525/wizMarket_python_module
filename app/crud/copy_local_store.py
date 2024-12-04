from app.db.connect import *
import logging


def delete_local_store_temp():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            DELETE FROM local_store_temp;
        """
        cursor.execute(query)

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def select_local_store_data():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            SELECT 
                CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, STORE_BUSINESS_NUMBER, 
                STORE_NAME, BRANCH_NAME, LARGE_CATEGORY_CODE, LARGE_CATEGORY_NAME, 
                MEDIUM_CATEGORY_CODE, MEDIUM_CATEGORY_NAME, SMALL_CATEGORY_CODE, SMALL_CATEGORY_NAME, 
                INDUSTRY_CODE, INDUSTRY_NAME, PROVINCE_CODE, PROVINCE_NAME, DISTRICT_CODE, DISTRICT_NAME, 
                ADMINISTRATIVE_DONG_CODE, ADMINISTRATIVE_DONG_NAME, LEGAL_DONG_CODE, LEGAL_DONG_NAME, 
                LOT_NUMBER_CODE, LAND_CATEGORY_CODE, LAND_CATEGORY_NAME, LOT_MAIN_NUMBER, LOT_SUB_NUMBER, LOT_ADDRESS, 
                ROAD_NAME_CODE, ROAD_NAME, BUILDING_MAIN_NUMBER, BUILDING_SUB_NUMBER, BUILDING_MANAGEMENT_NUMBER, BUILDING_NAME, 
                ROAD_NAME_ADDRESS, OLD_POSTAL_CODE, NEW_POSTAL_CODE, DONG_INFO, FLOOR_INFO, UNIT_INFO, LONGITUDE, LATITUDE, 
                IS_EXIST, LOCAL_YEAR, LOCAL_QUARTER, CREATED_AT, UPDATED_AT, REFERENCE_ID
            FROM LOCAL_STORE
        """
        cursor.execute(query)
        result = cursor.fetchall()

        return result

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def insert_local_store_temp_data(data):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL INSERT 쿼리
            insert_query = """
                INSERT INTO LOCAL_STORE_TEMP (
                    CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, STORE_BUSINESS_NUMBER, 
                    STORE_NAME, BRANCH_NAME, LARGE_CATEGORY_CODE, LARGE_CATEGORY_NAME, 
                    MEDIUM_CATEGORY_CODE, MEDIUM_CATEGORY_NAME, SMALL_CATEGORY_CODE, SMALL_CATEGORY_NAME, 
                    INDUSTRY_CODE, INDUSTRY_NAME, PROVINCE_CODE, PROVINCE_NAME, DISTRICT_CODE, DISTRICT_NAME, 
                    ADMINISTRATIVE_DONG_CODE, ADMINISTRATIVE_DONG_NAME, LEGAL_DONG_CODE, LEGAL_DONG_NAME, 
                    LOT_NUMBER_CODE, LAND_CATEGORY_CODE, LAND_CATEGORY_NAME, LOT_MAIN_NUMBER, LOT_SUB_NUMBER, LOT_ADDRESS, 
                    ROAD_NAME_CODE, ROAD_NAME, BUILDING_MAIN_NUMBER, BUILDING_SUB_NUMBER, BUILDING_MANAGEMENT_NUMBER, BUILDING_NAME, 
                    ROAD_NAME_ADDRESS, OLD_POSTAL_CODE, NEW_POSTAL_CODE, DONG_INFO, FLOOR_INFO, UNIT_INFO, LONGITUDE, LATITUDE, 
                    IS_EXIST, LOCAL_YEAR, LOCAL_QUARTER, CREATED_AT, UPDATED_AT, REFERENCE_ID
                ) 
                VALUES (
                    %(CITY_ID)s, %(DISTRICT_ID)s, %(SUB_DISTRICT_ID)s, %(STORE_BUSINESS_NUMBER)s,
                    %(STORE_NAME)s, %(BRANCH_NAME)s, %(LARGE_CATEGORY_CODE)s, %(LARGE_CATEGORY_NAME)s,
                    %(MEDIUM_CATEGORY_CODE)s, %(MEDIUM_CATEGORY_NAME)s, %(SMALL_CATEGORY_CODE)s, %(SMALL_CATEGORY_NAME)s,
                    %(INDUSTRY_CODE)s, %(INDUSTRY_NAME)s, %(PROVINCE_CODE)s, %(PROVINCE_NAME)s, %(DISTRICT_CODE)s, %(DISTRICT_NAME)s,
                    %(ADMINISTRATIVE_DONG_CODE)s, %(ADMINISTRATIVE_DONG_NAME)s, %(LEGAL_DONG_CODE)s, %(LEGAL_DONG_NAME)s,
                    %(LOT_NUMBER_CODE)s, %(LAND_CATEGORY_CODE)s, %(LAND_CATEGORY_NAME)s, %(LOT_MAIN_NUMBER)s, %(LOT_SUB_NUMBER)s, %(LOT_ADDRESS)s,
                    %(ROAD_NAME_CODE)s, %(ROAD_NAME)s, %(BUILDING_MAIN_NUMBER)s, %(BUILDING_SUB_NUMBER)s, %(BUILDING_MANAGEMENT_NUMBER)s, %(BUILDING_NAME)s,
                    %(ROAD_NAME_ADDRESS)s, %(OLD_POSTAL_CODE)s, %(NEW_POSTAL_CODE)s, %(DONG_INFO)s, %(FLOOR_INFO)s, %(UNIT_INFO)s, %(LONGITUDE)s, %(LATITUDE)s,
                    %(IS_EXIST)s, %(LOCAL_YEAR)s, %(LOCAL_QUARTER)s, %(CREATED_AT)s, %(UPDATED_AT)s, %(REFERENCE_ID)s
                )
            """

            # 각 데이터 항목을 삽입
            for record in data:
                cursor.execute(insert_query, record)
            
            # 커밋
            connection.commit()
            logger.info("데이터 삽입 성공!")
    
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        # 리소스 해제
        cursor.close()


def drop_local_store():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            DROP TABLE local_store;
        """
        cursor.execute(query)

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def rename_local_store_temp_to_local_store():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            RENAME TABLE local_store_temp TO local_store;
        """
        cursor.execute(query)
        print("테이블 이름 변경 완료: local_store_temp → local_store")

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)