from app.schemas.sub_district import AllRegionIdOutPut
from app.schemas.loc_info_statistics import (
    NationLocInfoOutPut,
    CityLocInfoOutPut,
    DistrictLocInfoOutPut,
    InsertLocInfoStat,
    LocInfoJScoreRank,
    MzPopJScoreRank,
    LocInfoJScorePer,
    MzPopJScorePer,
    LocInfoJScorePerNonOutLiers,
    MzPopJScorePerNonOutLiers
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


# 1. 전국 지역 별 타겟 입지 정보값 조회
def select_nation_loc_info_by_region(
    connection, city_id:int, district_id:int, sub_district_id:int, ref_date:date, target_item:str
) -> List[NationLocInfoOutPut]:
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[NationLocInfoOutPut] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    Y_M as REF_DATE,
                    REFERENCE_ID,
                    {target_item} as TARGET_ITEM  -- target 컬럼을 동적으로 추가
                FROM
                    loc_info
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND Y_M = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = NationLocInfoOutPut(
                    city_id= row.get("CITY_ID"),
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    reference_id= row.get("REFERENCE_ID"),
                    target_item= row.get("TARGET_ITEM")
                )
                results.append(loc_info_by_region)

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

    return results


# 2. 시/도 지역 별 타겟 값 입지 정보 조회
def select_city_loc_info_by_region(
    connection, city_id:int, sub_district_id:int, ref_date:date, target_item:str
) -> List[CityLocInfoOutPut]:

    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CityLocInfoOutPut] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    SUB_DISTRICT_ID,
                    Y_M as REF_DATE,
                    REFERENCE_ID,
                    {target_item} as TARGET_ITEM  -- target 컬럼을 동적으로 추가
                FROM
                    loc_info
                WHERE 
                    city_id = %s 
                    AND sub_district_id = %s 
                    AND Y_M = %s
            """

            cursor.execute(select_query, (city_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = CityLocInfoOutPut(
                    city_id= row.get("CITY_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    reference_id= row.get("REFERENCE_ID"),
                    target_item= row.get("TARGET_ITEM")
                )
                results.append(loc_info_by_region)

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

    return results




# 3. 시/군/구 지역 별 입지 정보 조회
def select_district_loc_info_by_region(
    connection, district_id:int, sub_district_id:int, ref_date:date, target_item:str
) -> List[DistrictLocInfoOutPut]:

    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[DistrictLocInfoOutPut] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    Y_M as REF_DATE,
                    REFERENCE_ID,
                    {target_item} as TARGET_ITEM  -- target 컬럼을 동적으로 추가
                FROM
                    loc_info
                WHERE 
                    district_id = %s 
                    AND sub_district_id = %s 
                    AND Y_M = %s
            """

            cursor.execute(select_query, (district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = DistrictLocInfoOutPut(
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    ref_date= row.get("REF_DATE"),
                    reference_id= row.get("REFERENCE_ID"),
                    target_item= row.get("TARGET_ITEM")
                )
                results.append(loc_info_by_region)

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

    return results


# 4. 인서트용
def insert_loc_info_statistics(connection, data):

    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 인서트 문
            insert_query = """
                INSERT INTO loc_info_statistics (
                    city_id, district_id, sub_district_id, reference_id, target_item,
                    avg_val, med_val, std_val, max_val, min_val, 
                    j_score_rank, j_score_per, j_score, 
                    j_score_per_non_outliers, j_score_non_outliers, 
                    ref_date, stat_level, created_at
                ) VALUES (%(city_id)s, %(district_id)s, %(sub_district_id)s, %(reference_id)s, %(target_item)s,
                        %(avg_val)s, %(med_val)s, %(std_val)s, %(max_val)s, %(min_val)s, 
                        %(j_score_rank)s, %(j_score_per)s, %(j_score)s,
                        %(j_score_per_non_outliers)s, %(j_score_non_outliers)s,
                        %(ref_date)s, %(stat_level)s, now())
            """

            # 데이터 리스트를 반복하여 인서트
            for record in data:
                try:
                    # Pydantic 모델로 검증
                    validated_record = InsertLocInfoStat(**record)
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


# 5. 가중치 계산 위한 입지 J-Score-rank 조회
def select_loc_info_j_score_rank(
        connection, city_id:int, district_id:int, sub_district_id:int, target_item :str, ref_date: date
) -> List[LocInfoJScoreRank]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[LocInfoJScoreRank] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    TARGET_ITEM,  
                    J_SCORE_RANK
                FROM
                    loc_info_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND target_item = %s
                    AND stat_level = '전국'
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, target_item, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = LocInfoJScoreRank(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    target_item=row.get("TARGET_ITEM"),
                    j_score_rank=row.get("J_SCORE_RANK")  
                )
                results.append(loc_info_by_region)

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


    return results


# 6. 가중치 계산 위한 mz 인구 j_score_rank 값 조회
def select_mz_j_score_rank(
    connection, city_id:int, district_id:int, sub_district_id:int, ref_date: date
) -> List[MzPopJScoreRank]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[MzPopJScoreRank] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    J_SCORE_RANK
                FROM
                    population_info_mz_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = MzPopJScoreRank(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    j_score_rank=row.get("J_SCORE_RANK") 
                )
                results.append(loc_info_by_region)

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



# 7. 가중치 계산 위한 입지 J-Score-Per 조회
def select_loc_info_j_score_per(
        connection, city_id:int, district_id:int, sub_district_id:int, target_item :str, ref_date: date
) -> List[LocInfoJScorePer]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[LocInfoJScorePer] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    TARGET_ITEM,  
                    J_SCORE_PER
                FROM
                    loc_info_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND target_item = %s
                    AND stat_level = '전국'
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, target_item, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = LocInfoJScorePer(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    target_item=row.get("TARGET_ITEM"),
                    j_score_per=row.get("J_SCORE_PER")  
                )
                results.append(loc_info_by_region)

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


    return results


# 8. 가중치 계산 위한 mz 인구 j_score_per 값 조회
def select_mz_j_score_per(
    connection, city_id:int, district_id:int, sub_district_id:int, ref_date: date
) -> List[MzPopJScorePer]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[MzPopJScorePer] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    J_SCORE_PER
                FROM
                    population_info_mz_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = MzPopJScorePer(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    j_score_per=row.get("J_SCORE_PER") 
                )
                results.append(loc_info_by_region)

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


    return results


########### 이상치 제거





# 7. 가중치 계산 위한 입지 J-Score-Per 조회
def select_loc_info_j_score_per_non_outliers(
        connection, city_id:int, district_id:int, sub_district_id:int, target_item :str, ref_date: date
) -> List[LocInfoJScorePerNonOutLiers]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[LocInfoJScorePerNonOutLiers] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    TARGET_ITEM,  
                    J_SCORE_PER_NON_OUTLIERS
                FROM
                    loc_info_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND target_item = %s
                    AND stat_level = '전국'
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, target_item, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = LocInfoJScorePerNonOutLiers(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    target_item=row.get("TARGET_ITEM"),
                    j_score_per_non_outliers=row.get("J_SCORE_PER_NON_OUTLIERS")  
                )
                results.append(loc_info_by_region)

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


    return results


# 8. 가중치 계산 위한 mz 인구 j_score_per 값 조회
def select_mz_j_score_per_non_outliers(
    connection, city_id:int, district_id:int, sub_district_id:int, ref_date: date
) -> List[MzPopJScorePerNonOutLiers]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[MzPopJScorePerNonOutLiers] = []

    try:
        if connection.open:
            select_query = f"""
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    J_SCORE_PER_NON_OUTLIERS
                FROM
                    population_info_mz_statistics
                WHERE 
                    city_id = %s 
                    AND district_id = %s 
                    AND sub_district_id = %s 
                    AND REF_DATE = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            rows = cursor.fetchall()

            for row in rows:
                loc_info_by_region = MzPopJScorePerNonOutLiers(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    j_score_per_non_outliers=row.get("J_SCORE_PER_NON_OUTLIERS") 
                )
                results.append(loc_info_by_region)

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


    return results







# 15. 가중치 적용 평균 j_score_rank 인서트용
def insert_loc_info_statistics_avg_j_score(data):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            # SQL 인서트 문
            insert_query = """
                INSERT INTO loc_info_statistics (
                    city_id, district_id, sub_district_id, reference_id, target_item,
                    AVG_VAL, MED_VAL, STD_VAL, MAX_VAL, MIN_VAL,
                    J_SCORE_RANK, J_SCORE_PER, J_SCORE, 
                    J_SCORE_PER_NON_OUTLIERS, J_SCORE_NON_OUTLIERS,
                    REF_DATE, STAT_LEVEL, CREATED_AT
                ) VALUES (
                    %(city_id)s, %(district_id)s, %(sub_district_id)s, 3, 'j_score_avg',
                    NULL, NULL, NULL, NULL, NULL, 
                    %(j_score_rank)s, %(j_score_per)s, %(j_score)s, 
                    %(j_score_per_non_outliers)s, %(j_score_non_outliers)s,
                    %(ref_date)s, '전국', NOW()
                )
            """
            # 데이터 리스트를 반복하여 인서트
            for record in data:
                try:
                    cursor.execute(insert_query, record)  # 딕셔너리 형태로 인서트 
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
