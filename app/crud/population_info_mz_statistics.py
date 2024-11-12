from app.schemas.sub_district import AllRegionIdOutPut
from app.schemas.population_info_mz_statistics import (
    NationMZPopOutPut,
    CityMZPopOutPut,
    DistrictMZPopOutPut,
    InsertMzPopStat
)
import logging
from pymysql import MySQLError
from typing import List, Optional
import pymysql
from datetime import date

from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)
import numpy as np


# 1. 전국 지역 별 mz 세대 인구 조회
def select_nation_mz_pop_by_region(
        city_id:int, district_id:int, sub_district_id:int, ref_date:date
) -> List[NationMZPopOutPut]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[NationMZPopOutPut] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    reference_date as REF_DATE,
                    (age_14 + age_15 + age_16 + age_17 + age_18 + age_19 + age_20 + age_21 + 
                     age_22 + age_23 + age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS MZ_POP
                FROM
                    population
                where city_id = %s and district_id = %s and sub_district_id = %s and reference_date = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            # 합산된 결과를 저장할 리스트
            merged_rows = []

            # 쌍으로 묶어서 처리
            for i in range(0, len(rows), 2):
                if i + 1 < len(rows):
                    # 두 항목의 mz_pop 값을 합산
                    combined_mz_pop = rows[i]["MZ_POP"] + rows[i + 1]["MZ_POP"]

                    # 첫 번째 항목을 복사하여 새로운 항목 생성
                    merged_result = rows[i].copy()  # 딕셔너리 복사

                    # mz_pop을 합산된 값으로 설정
                    merged_result["MZ_POP"] = combined_mz_pop

                    # 결과 리스트에 추가
                    merged_rows.append(merged_result)

            for row in merged_rows:
                mz_pop_by_region = NationMZPopOutPut(
                    city_id= row.get("CITY_ID"),
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    mz_pop= row.get("MZ_POP")
                )
                results.append(mz_pop_by_region)

            return results
        
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)

    return results


# 2. 시/도 지역 별 mz 세대 인구 조회
def select_city_mz_pop_by_region(
        city_id:int, sub_district_id:int, ref_date:date
) -> List[CityMZPopOutPut]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CityMZPopOutPut] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    CITY_ID,
                    SUB_DISTRICT_ID,
                    reference_date as REF_DATE,
                    (age_14 + age_15 + age_16 + age_17 + age_18 + age_19 + age_20 + age_21 + 
                     age_22 + age_23 + age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS MZ_POP
                FROM
                    population
                where city_id = %s and sub_district_id = %s and reference_date = %s
            """

            cursor.execute(select_query, (city_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            # 합산된 결과를 저장할 리스트
            merged_rows = []

            # 쌍으로 묶어서 처리
            for i in range(0, len(rows), 2):
                if i + 1 < len(rows):
                    # 두 항목의 mz_pop 값을 합산
                    combined_mz_pop = rows[i]["MZ_POP"] + rows[i + 1]["MZ_POP"]

                    # 첫 번째 항목을 복사하여 새로운 항목 생성
                    merged_result = rows[i].copy()  # 딕셔너리 복사

                    # mz_pop을 합산된 값으로 설정
                    merged_result["MZ_POP"] = combined_mz_pop

                    # 결과 리스트에 추가
                    merged_rows.append(merged_result)

            for row in merged_rows:
                mz_pop_by_region = CityMZPopOutPut(
                    city_id= row.get("CITY_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    mz_pop= row.get("MZ_POP")
                )
                results.append(mz_pop_by_region)

            return results
        
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)

    return results




# 3. 시/군/구 지역 별 mz 세대 인구 조회
def select_district_mz_pop_by_region(
        district_id:int, sub_district_id:int, ref_date:date
) -> List[DistrictMZPopOutPut]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[DistrictMZPopOutPut] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    reference_date as REF_DATE,
                    (age_14 + age_15 + age_16 + age_17 + age_18 + age_19 + age_20 + age_21 + 
                     age_22 + age_23 + age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS MZ_POP
                FROM
                    population
                where district_id = %s and sub_district_id = %s and reference_date = %s
            """

            cursor.execute(select_query, (district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            # 합산된 결과를 저장할 리스트
            merged_rows = []

            # 쌍으로 묶어서 처리
            for i in range(0, len(rows), 2):
                if i + 1 < len(rows):
                    # 두 항목의 mz_pop 값을 합산
                    combined_mz_pop = rows[i]["MZ_POP"] + rows[i + 1]["MZ_POP"]

                    # 첫 번째 항목을 복사하여 새로운 항목 생성
                    merged_result = rows[i].copy()  # 딕셔너리 복사

                    # mz_pop을 합산된 값으로 설정
                    merged_result["MZ_POP"] = combined_mz_pop

                    # 결과 리스트에 추가
                    merged_rows.append(merged_result)

            for row in merged_rows:
                mz_pop_by_region = DistrictMZPopOutPut(
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    mz_pop= row.get("MZ_POP")
                )
                results.append(mz_pop_by_region)

            return results
        
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)

    return results


# 4. 인서트용
def insert_mz_population_statistics(data):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 인서트 문
            insert_query = """
                INSERT INTO population_info_mz_statistics (
                    city_id, district_id, sub_district_id, ref_date, 
                    avg_val, med_val, std_val, max_val, min_val, 
                    j_score_rank, j_score_per, j_score, 
                    j_score_rank_non_outliers, j_score_per_non_outliers, j_score_non_outliers, 
                    stat_level, created_at
                ) VALUES (%(city_id)s, %(district_id)s, %(sub_district_id)s, %(ref_date)s, 
                        %(avg_val)s, %(med_val)s, %(std_val)s, %(max_val)s, %(min_val)s, 
                        %(j_score_rank)s, %(j_score_per)s, %(j_score)s, 
                        %(j_score_rank_non_outliers)s, %(j_score_per_non_outliers)s, %(j_score_non_outliers)s, 
                        %(stat_level)s, now())
            """

            # 데이터 리스트를 반복하여 인서트
            for record in data:
                try:
                    # Pydantic 모델로 검증
                    validated_record = InsertMzPopStat(**record)
                    # 딕셔너리로 변환하여 SQL 인서트
                    cursor.execute(insert_query, validated_record.dict())
                except ValueError as e:
                    logger.error(f"Data Validation Error: {e}")

            # 커밋
            connection.commit()

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        if connection:
            connection.rollback()

    finally:
        # 리소스 해제
        if cursor:
            cursor.close()
        if connection:
            connection.close()