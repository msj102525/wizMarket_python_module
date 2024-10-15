import pandas as pd
import os
from dotenv import load_dotenv
from app.db.connect import *
from app.crud.crime import insert_crime_data


# csv 파일 인서트 

CITY_MAPPING = {
    "강원": 1,
    "경기": 2,
    "경남": 3,
    "경북": 4,
    "광주": 5,
    "대구": 6,
    "대전": 7,
    "부산": 8,
    "서울": 9,
    "세종": 10,
    "울산": 11,
    "인천": 12,
    "전남": 13,
    "전북": 14,
    "제주": 15,
    "충남": 16,
    "충북": 17,
}


def process_crime_data():
    # .env 파일에서 환경 변수 로드
    load_dotenv()

    # MySQL 연결 설정
    connection = get_db_connection()

    # ROOT_PATH 가져오기
    root_path = os.getenv('ROOT_PATH')

    # 엑셀 파일 경로 설정
    excel_file_path = os.path.join(root_path, 'app', 'data', 'crimeData')

    # 폴더 내 모든 .xlsx 파일 찾기
    files = [f for f in os.listdir(excel_file_path) if f.endswith(".xlsx")]

    combined_df = {}

    try:
        for file in files:
            file_path = os.path.join(excel_file_path, file)
            
            # 파일 이름에서 도시명 추출
            city_name_part = file.split('_')[4]  # "서울청", "경기남부청" 부분 추출
            city_name = city_name_part.replace('청', '')  # "서울", "경기남부", "경기북부" 등으로 변환

            city_id = CITY_MAPPING.get(city_name, 0)

            # 엑셀 파일을 DataFrame으로 로드 (첫 행을 무시)
            df = pd.read_excel(file_path)
            df = df.iloc[1:]  # 첫 번째 행 제거

            # 분기별 데이터프레임 생성
            quarters = ['2023.1/4', '2023.2/4', '2023.3/4', '2023.4/4', 
                        '2024.1/4', '2024.2/4']
            
            for quarter in quarters:
                cols = ['죄종별(1)', '죄종별(2)', quarter, f'{quarter}.1', f'{quarter}.2', f'{quarter}.3', f'{quarter}.4']
                if all(col in df.columns for col in cols):  # 분기에 해당하는 컬럼이 모두 존재할 경우
                    quarter_df = df[cols].copy()
                    quarter_df.columns = ['MajorCategory', 'MinorCategory', 'IncidentCount', 'ArrestCount', 'ArrestRatio', 'ArrestPersonnel', 'LegalEntity']

                    # 숫자형 컬럼들을 명시적으로 변환
                    for col in ['IncidentCount', 'ArrestCount', 'ArrestRatio', 'ArrestPersonnel', 'LegalEntity']:
                        quarter_df[col] = pd.to_numeric(quarter_df[col], errors='coerce')

                    # 경기남부와 경기북부를 합치기
                    if city_name in ['경기남부', '경기북부']:
                        if quarter not in combined_df:
                            combined_df[quarter] = quarter_df
                        else:
                            # 숫자형 컬럼만 합산 (ArrestRatio 제외)
                            combined_df[quarter][['IncidentCount', 'ArrestCount', 'ArrestPersonnel', 'LegalEntity']] += quarter_df[['IncidentCount', 'ArrestCount', 'ArrestPersonnel', 'LegalEntity']]
                            
                            # 발생건수와 검거건수 합산 후, ArrestRatio 재계산
                            combined_df[quarter]['ArrestRatio'] = (combined_df[quarter]['ArrestCount'] / combined_df[quarter]['IncidentCount']) * 100
                    else:
                        insert_crime_data(connection, quarter_df, city_id, quarter)

        # 합산된 경기 데이터 삽입
        for quarter, df in combined_df.items():
            insert_crime_data(connection, df, CITY_MAPPING['경기'], quarter)

    finally:
        close_connection(connection)

if __name__ == "__main__":
    process_crime_data()
