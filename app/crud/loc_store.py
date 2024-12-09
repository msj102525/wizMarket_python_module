import pymysql
from app.schemas.loc_store import LocalStoreBusinessNumber, LocalStore, LocalOldStore
from app.db.connect import get_db_connection, close_connection, close_cursor, commit, rollback

# 기존 매장 코드 조회
def get_store_business_number()-> LocalStoreBusinessNumber:
    results = []
    # 여기서 직접 DB 연결을 설정
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                STORE_BUSINESS_NUMBER
            FROM 
                local_store
            WHERE 
                (LOCAL_YEAR, LOCAL_QUARTER) = (
                    SELECT LOCAL_YEAR, LOCAL_QUARTER
                    FROM local_store
                    ORDER BY LOCAL_YEAR DESC, LOCAL_QUARTER DESC
                    LIMIT 1
                );
        """
        query_params = []

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, query_params)
        rows = cursor.fetchall()

        for row in rows:
            local_store_business_number = LocalStoreBusinessNumber(
                store_business_number= row.get("STORE_BUSINESS_NUMBER"),
            )
            results.append(local_store_business_number)

        return results

    finally:
        close_cursor(cursor)     
        close_connection(connection) 


# 기존 매장 업데이트
def update_data_to_new_local_store(cursor, data_dict):

    try:
        data = LocalStore(**data_dict)


        sql = """
            UPDATE local_store
            SET
                city_id = %s, district_id = %s, sub_district_id = %s,  
                store_business_number = %s, store_name = %s, branch_name = %s,
                large_category_code = %s, large_category_name = %s,
                medium_category_code = %s, medium_category_name = %s,
                small_category_code = %s, small_category_name = %s,
                industry_code = %s, industry_name = %s,
                province_code = %s, province_name = %s, district_code = %s,
                district_name = %s, administrative_dong_code = %s, administrative_dong_name = %s,
                legal_dong_code = %s, legal_dong_name = %s,
                lot_number_code = %s, land_category_code = %s, land_category_name = %s,
                lot_main_number = %s, lot_sub_number = %s, lot_address = %s,
                road_name_code = %s, road_name = %s, building_main_number = %s,
                building_sub_number = %s, building_management_number = %s, building_name = %s,
                road_name_address = %s, old_postal_code = %s, new_postal_code = %s,
                dong_info = %s, floor_info = %s, unit_info = %s,
                longitude = %s, latitude = %s, local_year = %s, local_quarter = %s,
                CREATED_AT = now(), UPDATED_AT = now(),
                IS_EXIST = %s
            WHERE store_business_number = %s
            """
            
        params = list(data.dict().values()) + [data.store_business_number]
        # print(params)
        cursor.execute(sql, params)
        # commit(connection)

    except Exception as e:
        print(f"Error updating data in local_store: {e}")
        raise





# 신규 매장 인서트
def insert_data_to_new_local_store(cursor, data_dict):
    try:
        # Pydantic 모델로 데이터 검증 및 변환
        data = LocalStore(**data_dict)

        # 인서트 SQL 쿼리
        sql = """
        INSERT INTO local_store (
            city_id, district_id, sub_district_id,  
            store_business_number, store_name, branch_name,
            large_category_code, large_category_name,
            medium_category_code, medium_category_name,
            small_category_code, small_category_name,
            industry_code, industry_name,
            province_code, province_name, district_code,
            district_name, administrative_dong_code, administrative_dong_name,
            legal_dong_code, legal_dong_name,
            lot_number_code, land_category_code, land_category_name,
            lot_main_number, lot_sub_number, lot_address,
            road_name_code, road_name, building_main_number,
            building_sub_number, building_management_number, building_name,
            road_name_address, old_postal_code, new_postal_code,
            dong_info, floor_info, unit_info,
            longitude, latitude, local_year, local_quarter,
            CREATED_AT, UPDATED_AT, IS_EXIST
        ) VALUES (
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, 
            %s, %s, 
            %s, %s, 
            %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s,
            %s, %s, %s, %s,
            NOW(), NOW(), %s
        )
        """

        # 매개변수 설정 및 SQL 실행
        params = list(data.dict().values())
        cursor.execute(sql, params)

    except Exception as e:
        print(f"Error inserting data in local_store: {e}")
        raise





# 없어진 매장 업데이트
def update_data_to_old_local_store(cursor, data_dict):
    try:
        # Pydantic 모델을 사용해 데이터 검증 및 변환
        data = LocalOldStore(**data_dict)

        # 업데이트 SQL 쿼리
        sql = """
        UPDATE local_store
        SET
            IS_EXIST = %s
        WHERE store_business_number = %s
        """
        
        # SQL 파라미터 설정
        params = [data.is_exist, data.store_business_number]
        cursor.execute(sql, params)

    except Exception as e:
        print(f"Error updating old data in local_store: {e}")
        raise






from app.db.connect import *


def add_local_store_column():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            ALTER TABLE local_store
            ADD COLUMN ktmyshop TINYINT(1) DEFAULT 0 COMMENT 'KT My Shop 여부',
            ADD COLUMN jsam TINYINT(1) DEFAULT 0 COMMENT 'JSAM 여부';
        """
        cursor.execute(query)

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def update_local_store_flag_column(store_business_number):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            UPDATE LOCAL_STORE
            SET JSAM = 1
            WHERE STORE_BUSINESS_NUMBER = %s
        """
        cursor.execute(query, store_business_number)
        connection.commit()

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)
