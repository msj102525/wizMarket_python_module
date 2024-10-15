import os
import pandas as pd
from dotenv import load_dotenv  # .env 파일 로드용 패키지
from app.schemas.loc_store import LocalStoreLatLng
from app.service.population import *
from app.db.connect import *
import re
from app.crud.loc_store import (
    insert_data_to_loc_store,
    check_previous_quarter_data_exists
)
from datetime import datetime



# root_dir 경로 설정
root_dir = r"C:\Users\jyes_semin\Desktop\locStoreData"

# 1. 필요한 데이터를 미리 로드합니다.
connection = get_db_connection()
cities = load_all_cities(connection)
districts = load_all_districts(connection)
sub_districts = load_all_sub_districts(connection)


# 1. 저번 분기를 'YYYY.Q/4' 형식으로 반환하는 함수 (DB 형식)
def get_previous_quarter():
    """저번 분기를 'YYYY.Q/4' 형식으로 반환하는 함수."""
    now = datetime.now()
    current_quarter = (now.month - 1) // 3 + 1
    previous_year = now.year if current_quarter > 1 else now.year - 1
    previous_quarter = current_quarter - 1 if current_quarter > 1 else 4
    return f"{previous_year}.{previous_quarter}/4"


# 2. 저번 분기를 'YYYY Q분기' 형식으로 변환 (폴더명 형식)
def convert_to_folder_quarter_format(db_quarter):
    """DB 분기 형식을 폴더명 형식으로 변환 (예: '2021.1/4' -> '2021 1분기')"""
    year, quarter = db_quarter.split(".")
    quarter = quarter.split("/")[0]  # '1/4'에서 '1' 추출
    return f"{year} {quarter}분기"


# 3. 디렉토리에서 저번 분기 폴더가 있는지 확인하는 함수
def find_previous_quarter_folder(root_dir):
    """저번 분기에 해당하는 폴더명을 찾아 반환하는 함수."""
    previous_quarter = get_previous_quarter()  # DB 형식 'YYYY.Q/4'
    previous_quarter_folder = convert_to_folder_quarter_format(
        previous_quarter
    )  # 폴더명 형식 'YYYY Q분기'

    # 디렉토리 내에서 저번 분기 폴더명 찾기
    for folder_name in os.listdir(root_dir):
        if previous_quarter_folder in folder_name:
            return os.path.join(root_dir, folder_name)

    return None


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


#########################################################


# 2. 데이터 처리 및 로드
# def process_csv_files():
def process_csv_files(year, quarter):

    connection = get_db_connection()

    # # 1. 저번 분기 계산
    # previous_quarter = get_previous_quarter()

    # # 2. 데이터베이스에 저번 분기 데이터가 있는지 확인 (crud/loc_store.py 함수 사용)
    # exists = check_previous_quarter_data_exists(connection, previous_quarter)
    # if exists:
    #     print(f"저번 분기({previous_quarter}) 데이터가 이미 존재합니다. 인서트 생략.")
    #     return

    # # 3. 디렉토리에서 저번 분기 폴더를 찾음
    # quarter_folder = find_previous_quarter_folder(root_dir)
    # if not quarter_folder:
    #     print(f"저번 분기({previous_quarter})에 해당하는 폴더를 찾을 수 없습니다.")
    #     return

    ############################################## 특정 분기 인서트

    # 1. 지정된 분기 계산
    specific_quarter = get_specific_quarter(year, quarter)

    # 2. 데이터베이스에 지정된 분기 데이터가 있는지 확인
    exists = check_previous_quarter_data_exists(connection, year, quarter)
    if exists:
        print(f"지정된 분기({specific_quarter}) 데이터가 이미 존재합니다. 인서트 생략.")
        return

    # 3. 디렉토리에서 지정된 분기 폴더를 찾음
    quarter_folder = find_specific_quarter_folder(root_dir, year, quarter)
    if not quarter_folder:
        print(f"지정된 분기({specific_quarter})에 해당하는 폴더를 찾을 수 없습니다.")
        return

    ######################################################

    # 시도명 매핑 (CSV 파일의 시도명 -> 데이터베이스의 시도명)
    city_name_mappings = {
        "강원도": "강원특별자치도",
        "전라북도": "전북특별자치도",
        # 필요한 다른 매핑도 추가 가능합니다.
    }

    try:
        for subdir, dirs, files in os.walk(quarter_folder):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(subdir, file)

                    # 파일명에서 연도와 분기를 추출하여 year_quarter로 변환
                    file_name = os.path.basename(file_path)
                    year_quarter_raw = file_name.split("_")[-1].split(".")[
                        0
                    ]  # 예: '202103'
                    year = year_quarter_raw[:4]  # '2021'
                    quarter = (int(year_quarter_raw[4:6]) - 1) // 3 + 1  # 분기 계산

                    year = int(year)
                    quarter = int(quarter)

                    try:
                        # 파일을 읽어서 데이터프레임 생성
                        df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

                        # 빈칸을 모두 None (즉, NULL)으로 처리
                        df = df.where(pd.notnull(df), None)
                        # 빈칸, 공백 문자열을 모두 None (즉, NULL)으로 처리
                        df = df.apply(
                            lambda col: col.map(
                                lambda x: None if pd.isna(x) or x.strip() == "" else x
                            )
                        )

                        # 데이터프레임의 각 행을 순회하며 처리
                        for _, row in df.iterrows():
                            city_name = (
                                row["시도명"] if row["시도명"] is not None else None
                            )
                            district_name = (
                                row["시군구명"] if row["시군구명"] is not None else None
                            )
                            sub_district_name = (
                                row["행정동명"] if row["행정동명"] is not None else None
                            )

                            # 시도명을 매핑된 값으로 변환
                            city_name = city_name_mappings.get(city_name, city_name)

                            # sub_district_name이 None이면 다음으로 넘어가도록 처리
                            if sub_district_name is None:
                                continue

                            # sub_district_name에 '출장소'라는 단어가 포함된 경우, 건너뛰기
                            if "출장소" in sub_district_name:
                                continue

                            # sub_district_name에서 '제'를 제거
                            if isinstance(sub_district_name, str):
                                sub_district_name = re.sub(
                                    r"제(\d)", r"\1", sub_district_name
                                )

                            # 특정 패턴의 동명 매핑 작업
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

                            # district_name에 띄어쓰기가 있으면, 첫 번째 단어만 사용
                            if " " in district_name:
                                district_name = district_name.split()[0]

                            # 2. 로드된 데이터에서 참조
                            city = cities.get(city_name)
                            district = (
                                districts.get((city.city_id, district_name))
                                if city
                                else None
                            )
                            sub_district = (
                                sub_districts.get(
                                    (district.district_id, sub_district_name)
                                )
                                if district
                                else None
                            )

                            # 인서트할 데이터 준비
                            data = {
                                "CITY_ID": city.city_id,
                                "DISTRICT_ID": (
                                    district.district_id if district else None
                                ),
                                "SUB_DISTRICT_ID": (
                                    sub_district.sub_district_id
                                    if sub_district
                                    else None
                                ),
                                "STORE_BUSINESS_NUMBER": row["상가업소번호"],
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
                            }

                            # # 인서트할 데이터를 출력하고 자료형도 함께 출력
                            # print("인서트할 데이터:")
                            # print("인서트할 데이터 (총 항목 수: {}):".format(len(data)))
                            # for key, value in data.items():
                            #     print(f"{key}: {value} (자료형: {type(value)})")
                            # print("-" * 80)  # 구분선

                            # 데이터 삽입
                            try:
                                insert_data_to_loc_store(connection, data)
                            except Exception as insert_error:
                                print(f"데이터 삽입 오류: {insert_error}")
                                rollback(connection)
                                continue

                        # 파일 처리 후 커밋
                        commit(connection)

                    except Exception as e:
                        print(f"오류 발생 파일: {file_path}")
                        print(f"오류 메시지: {e}")
                        rollback(connection)
                        continue  # 다음 파일로 계속 진행

    except Exception as e:
        print(f"Unexpected error during processing: {e}")
        rollback(connection)
    finally:
        close_connection(connection)


# 윈도우 PC를 종료하는 명령어
def shutdown_windows():
    os.system("shutdown /s /t 1")


# if __name__ == "__main__":
#     process_csv_files(2024,2)
#     # shutdown_windows()