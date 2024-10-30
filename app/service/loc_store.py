import os
import pandas as pd
from dotenv import load_dotenv  # .env 파일 로드용 패키지
from app.schemas.loc_store import LocalStoreLatLng
from app.service.population import *
from app.db.connect import *
import re
from app.crud.loc_store import (
    insert_data_to_new_local_store,
    update_data_to_new_local_store,
    update_data_to_old_local_store,
    get_store_business_number
)
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed


# root_dir 경로 설정
root_dir = r"C:\Users\jyes_semin\Desktop\Data\locStoreData"

# 1. 필요한 데이터를 미리 로드합니다.
connection = get_db_connection()
cities = load_all_cities(connection)
districts = load_all_districts(connection)
sub_districts = load_all_sub_districts(connection)
store_business_number_list = get_store_business_number()
city_name_mappings = {
    "강원도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    # 필요한 다른 매핑도 추가 가능합니다.
}


# 2. 저번 분기를 'YYYY Q분기' 형식으로 변환 (폴더명 형식)
def convert_to_folder_quarter_format(db_quarter):
    """DB 분기 형식을 폴더명 형식으로 변환 (예: '2021.1/4' -> '2021 1분기')"""
    year, quarter = db_quarter.split(".")
    quarter = quarter.split("/")[0]  # '1/4'에서 '1' 추출
    return f"{year} {quarter}분기"



# 특정 분기 인서트 ###################################
def get_specific_quarter(year, quarter):
    """지정된 연도와 분기를 'YYYY.Q/4' 형식으로 반환하는 함수."""
    return f"{year}.{quarter}/4"


def find_specific_quarter_folder(root_dir, year, quarter):
    """지정된 연도와 분기에 해당하는 폴더명을 찾아 반환하는 함수."""
    specific_quarter = get_specific_quarter(year, quarter)
    specific_quarter_folder = convert_to_folder_quarter_format(specific_quarter)

    for folder_name in os.listdir(root_dir):
        if specific_quarter_folder in folder_name:
            return os.path.join(root_dir, folder_name)

    return None




def process_single_csv_file(file_path, year, quarter, store_business_number_set, cities, districts, sub_districts):
    """CSV 파일 하나를 처리하는 함수."""

    try:
        processed_store_business_numbers = set()
        update_data = []
        insert_data = []
        file_update_count = 0
        file_insert_count = 0

        # 파일을 읽어서 데이터프레임 생성
        df = pd.read_csv(file_path, dtype=str, encoding="utf-8")
        
        # 빈칸을 모두 None (즉, NULL)으로 처리
        df = df.where(pd.notnull(df), None)
        df = df.apply(lambda col: col.map(lambda x: None if pd.isna(x) or x.strip() == "" else x))

        # 데이터프레임의 각 행을 순회하며 처리
        for _, row in df.iterrows():
            try:
                city_name = city_name_mappings.get(row["시도명"], row["시도명"])
                district_name = row["시군구명"]
                sub_district_name = row["행정동명"]

                if sub_district_name is None or "출장소" in sub_district_name:
                    continue

                mappings = {
                    "숭의1.3동": "숭의1,3동",
                    "용현1.4동": "용현1,4동",
                    "도화2.3동": "도화2,3동",
                    "봉명2송정동": "봉명2.송정동",
                    "성화개신죽림동": "성화.개신.죽림동",
                    "용담명암산성동": "용담.명암.산성동",
                    "운천신봉동": "운천.신봉동",
                    "율량사천동": "율량.사천동",
                }
                if sub_district_name in mappings:
                    sub_district_name = mappings[sub_district_name]

                if " " in district_name:
                    district_name = district_name.split()[0]

                city = cities.get(city_name)
                district = districts.get((city.city_id, district_name)) if city else None
                sub_district = sub_districts.get((district.district_id, sub_district_name)) if district else None

                

                new_data = {
                    "city_id": city.city_id,
                    "district_id": (
                        district.district_id if district else None
                    ),
                    "sub_district_id": (
                        sub_district.sub_district_id
                        if sub_district
                        else None
                    ),
                    "store_business_number": row["상가업소번호"],
                    "store_name": row["상호명"],
                    "branch_name": row["지점명"],
                    "large_category_code": row["상권업종대분류코드"],
                    "large_category_name": row["상권업종대분류명"],
                    "medium_category_code": row["상권업종중분류코드"],
                    "medium_category_name": row["상권업종중분류명"],
                    "small_category_code": row["상권업종소분류코드"],
                    "small_category_name": row["상권업종소분류명"],
                    "industry_code": row["표준산업분류코드"],
                    "industry_name": row["표준산업분류명"],
                    "province_code": row["시도코드"],
                    "province_name": row["시도명"],
                    "district_code": row["시군구코드"],
                    "district_name": row["시군구명"],
                    "administrative_dong_code": row["행정동코드"],
                    "administrative_dong_name": row["행정동명"],
                    "legal_dong_code": row["법정동코드"],
                    "legal_dong_name": row["법정동명"],
                    "lot_number_code": row["지번코드"],
                    "land_category_code": row["대지구분코드"],
                    "land_category_name": row["대지구분명"],
                    "lot_main_number": row["지번본번지"],
                    "lot_sub_number": row["지번부번지"],
                    "lot_address": row["지번주소"],
                    "road_name_code": row["도로명코드"],
                    "road_name": row["도로명"],
                    "building_main_number": row["건물본번지"],
                    "building_sub_number": row["건물부번지"],
                    "building_management_number": row["건물관리번호"],
                    "building_name": row["건물명"],
                    "road_name_address": row["도로명주소"],
                    "old_postal_code": row["구우편번호"],
                    "new_postal_code": row["신우편번호"],
                    "dong_info": row["동정보"],
                    "floor_info": row["층정보"],
                    "unit_info": row["호정보"],
                    "longitude": row["경도"],
                    "latitude": row["위도"],
                    "local_year": year,
                    "local_quarter": quarter,
                    "is_exist" : 1
                    }

                current_store_business_number = row["상가업소번호"]
                processed_store_business_numbers.add(current_store_business_number)

                # Update or Insert
                if current_store_business_number in store_business_number_set:
                    update_data.append(new_data)
                    file_update_count  += 1
                else:
                    insert_data.append(new_data)
                    file_insert_count  += 1

                if sub_district is None:
                    print(f"No matching sub_district found for district_id: {district.district_id}, sub_district_name: {sub_district_name}")
                    break
                
            except Exception as e:
                print(f"Error processing row in file {file_path}: {e}")
                break  # 에러 발생 시 루프 중지

        return processed_store_business_numbers, update_data, insert_data, file_update_count, file_insert_count

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        rollback(connection)
        


def process_csv_files_parallel(year, quarter):
    store_business_number_set = {item.store_business_number for item in store_business_number_list}
    quarter_folder = find_specific_quarter_folder(root_dir, year, quarter)

    if not quarter_folder:
        print(f"No folder found for specified quarter {year}.{quarter}")
        return

    files = [os.path.join(subdir, file) for subdir, _, files in os.walk(quarter_folder) for file in files if file.endswith(".csv")]
    all_processed_store_numbers = set()
    update_data = []
    insert_data = []
    update_new_count = 0
    insert_new_count = 0

    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_single_csv_file, file_path, year, quarter, store_business_number_set, cities, districts, sub_districts): file_path for file_path in files}

        for future in as_completed(futures):
            try:
                processed_store_numbers, file_update_data, file_insert_data, file_update_count, file_insert_count = future.result()
                all_processed_store_numbers.update(processed_store_numbers)
                update_data.extend(file_update_data)  # 병렬 처리된 업데이트 데이터 모으기
                insert_data.extend(file_insert_data)  # 병렬 처리된 인서트 데이터 모으기
                update_new_count += file_update_count
                insert_new_count += file_insert_count
            except Exception as e:
                print(f"Exception processing file: {e}")

    # 데이터베이스 연결 후 한 번에 업데이트 및 인서트
    connection = get_db_connection()
    update_old_count = 0

    try:
        with connection.cursor() as cursor:
            # 인서트 데이터 일괄 처리
            for new_data in insert_data:
                insert_data_to_new_local_store(cursor, new_data)

            # 업데이트 데이터 일괄 처리
            for new_data in update_data:
                update_data_to_new_local_store(cursor, new_data)

            # 존재 여부 업데이트
            
            for store_number in store_business_number_set - all_processed_store_numbers:
                old_data = {"store_business_number": store_number, "is_exist": 0}
                update_data_to_old_local_store(cursor, old_data)
                update_old_count += 1

        commit(connection)

    except Exception as e:
        print(f"Error updating database: {e}")
        rollback(connection)

    finally:
        close_connection(connection)

    print('Updated existing stores:', update_new_count)
    print('New stores inserted:', insert_new_count)
    print('Stores marked as non-existent:', update_old_count)



if __name__ == "__main__":
    process_csv_files_parallel(2024, 3)
