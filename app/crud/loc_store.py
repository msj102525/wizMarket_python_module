import pymysql

def check_previous_quarter_data_exists(connection, year, quarter):
    """저번 분기의 데이터가 DB에 있는지 확인하는 함수"""

    # SQL 쿼리 작성 (저번 분기의 데이터가 존재하는지 확인)
    query = "SELECT COUNT(*) AS count FROM local_store WHERE local_year = %s AND local_quarter = %s"

    with connection.cursor() as cursor:
        cursor.execute(query, (year, quarter))
        result = cursor.fetchone()

    # count 값이 0이면 데이터가 없는 것
    return result[0] > 0


def insert_data_to_loc_store(connection, data):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO local_store (
                CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID,  
                STORE_BUSINESS_NUMBER, store_name, branch_name,
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
                CREATED_AT, UPDATED_AT
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
                NOW(), NOW()
            )
            """

            # 디버깅을 위해 각 항목을 개별적으로 테스트
            try:
                cursor.execute(
                    sql,
                    (
                        data["CITY_ID"],
                        data["DISTRICT_ID"],
                        data["SUB_DISTRICT_ID"],
                        data["STORE_BUSINESS_NUMBER"],
                        data["store_name"],
                        data["branch_name"],
                        data["large_category_code"],
                        data["large_category_name"],
                        data["medium_category_code"],
                        data["medium_category_name"],
                        data["small_category_code"],
                        data["small_category_name"],
                        data["industry_code"],
                        data["industry_name"],
                        data["province_code"],
                        data["province_name"],
                        data["district_code"],
                        data["district_name"],
                        data["administrative_dong_code"],
                        data["administrative_dong_name"],
                        data["legal_dong_code"],
                        data["legal_dong_name"],
                        data["lot_number_code"],
                        data["land_category_code"],
                        data["land_category_name"],
                        data["lot_main_number"],
                        data["lot_sub_number"],
                        data["lot_address"],
                        data["road_name_code"],
                        data["road_name"],
                        data["building_main_number"],
                        data["building_sub_number"],
                        data["building_management_number"],
                        data["building_name"],
                        data["road_name_address"],
                        data["old_postal_code"],
                        data["new_postal_code"],
                        data["dong_info"],
                        data["floor_info"],
                        data["unit_info"],
                        data["longitude"],
                        data["latitude"],
                        data["local_year"],
                        data["local_quarter"],
                    ),
                )

                connection.commit()

            except Exception as e:
                print(f"Error inserting specific data: {e}")
                raise

    except pymysql.MySQLError as e:
        print(f"Error inserting data into local_store: {e}")
        connection.rollback()
        raise
