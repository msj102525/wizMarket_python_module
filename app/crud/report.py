import logging
from typing import List
import pymysql
from tqdm import tqdm

from app.db.connect import (
    close_connection,
    close_cursor,
    get_db_connection,
    get_service_report_db_connection,
)
from app.schemas.report import (
    LocalStoreBasicInfo,
    LocalStoreMappingRepId,
    LocalStorePopulationData,
    LocalStoreSubdistrictId,
    LocalStoreTop5Menu,
    Report,
)


##################### SELECT ##############################
##################### 기본 매장 정보 넣기 ##############################
def select_local_store_info(batch_size: int = 5000) -> List[LocalStoreBasicInfo]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            select_query = """
                SELECT 
                    ls.STORE_BUSINESS_NUMBER,
                    c.CITY_NAME,
                    d.DISTRICT_NAME,
                    sd.SUB_DISTRICT_NAME,
                    ls.SMALL_CATEGORY_NAME,
                    ls.STORE_NAME,
                    ls.ROAD_NAME,
                    ls.BUILDING_NAME,
                    ls.FLOOR_INFO,
                    ls.LATITUDE,
                    ls.LONGITUDE
                FROM
                    LOCAL_STORE ls
                JOIN CITY c ON c.CITY_ID = ls.CITY_ID
                JOIN DISTRICT d ON d.DISTRICT_ID = ls.DISTRICT_ID
                JOIN SUB_DISTRICT sd ON sd.SUB_DISTRICT_ID = ls.SUB_DISTRICT_ID
                -- LIMIT 50000
            """

            logger.info(f"Executing query: {select_query}")
            cursor.execute(select_query)

            results = []
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    results.append(
                        LocalStoreBasicInfo(
                            store_business_number=row["STORE_BUSINESS_NUMBER"],
                            city_name=row["CITY_NAME"],
                            district_name=row["DISTRICT_NAME"],
                            sub_district_name=row["SUB_DISTRICT_NAME"],
                            detail_category_name=row["SMALL_CATEGORY_NAME"],
                            store_name=row["STORE_NAME"],
                            road_name=row["ROAD_NAME"],
                            building_name=row["BUILDING_NAME"],
                            floor_info=row["FLOOR_INFO"],
                            latitude=row["LATITUDE"],
                            longitude=row["LONGITUDE"],
                        )
                    )

            return results

    except Exception as e:
        logger.error(f"Error fetching statistics data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


##################### 매장마다 소분류별 뜨는 메뉴 TOP5 넣기 ##############################
# def select_local_store_rep_id() -> List[LocalStoreTop5Menu]:
#     connection = get_db_connection()
#     cursor = connection.cursor(pymysql.cursors.DictCursor)
#     logger = logging.getLogger(__name__)

#     try:
#         if connection.open:
#             select_query = """
#                 SELECT DISTINCT
#                     LS.STORE_BUSINESS_NUMBER,
#                     DCM.REP_ID
#                 FROM LOCAL_STORE LS
#                 JOIN BUSINESS_AREA_CATEGORY BAC ON BAC.DETAIL_CATEGORY_NAME = LS.SMALL_CATEGORY_NAME
#                 JOIN DETAIL_CATEGORY_MAPPING DCM ON DCM.BUSINESS_AREA_CATEGORY_ID = BAC.BUSINESS_AREA_CATEGORY_ID
#             ;
#             """

#             select_query_top5 = """
#                 SELECT
#                     TOP_MENU_1,
#                     TOP_MENU_2,
#                     TOP_MENU_3,
#                     TOP_MENU_4,
#                     TOP_MENU_5,
#                     Y_M
#                 FROM COMMERCIAL_DISTRICT
#                 WHERE BIZ_DETAIL_CATEGORY_ID = %s
#                 GROUP BY BIZ_DETAIL_CATEGORY_ID, TOP_MENU_1, TOP_MENU_2, TOP_MENU_3, TOP_MENU_4, TOP_MENU_5, Y_M
#                ;
#             """

#             cursor.execute(select_query)

#             results = []
#             rows = cursor.fetchall()
#             if not rows:
#                 return results

#             for row in tqdm(rows, desc="Processing rows", unit="top5_row"):
#                 store_business_number = row["STORE_BUSINESS_NUMBER"]
#                 rep_id = row["REP_ID"]

#                 # Fetch top 5 menu for each rep_id
#                 cursor.execute(select_query_top5, (rep_id,))
#                 result_top5 = cursor.fetchone()

#                 if result_top5:
#                     results.append(
#                         LocalStoreTop5Menu(
#                             store_business_number=store_business_number,
#                             detail_category_top1_ordered_menu=result_top5["TOP_MENU_1"],
#                             detail_category_top2_ordered_menu=result_top5["TOP_MENU_2"],
#                             detail_category_top3_ordered_menu=result_top5["TOP_MENU_3"],
#                             detail_category_top4_ordered_menu=result_top5["TOP_MENU_4"],
#                             detail_category_top5_ordered_menu=result_top5["TOP_MENU_5"],
#                             nice_biz_map_data_ref_date=result_top5["Y_M"],
#                         )
#                     )

#             return results

#     except Exception as e:
#         logger.error(f"Error fetching statistics data: {e}")
#         raise
#     finally:
#         cursor.close()
#         connection.close()


## ############################################################
def select_local_store_rep_id() -> List[LocalStoreMappingRepId]:
    logger = logging.getLogger(__name__)

    try:
        # with 문을 사용하여 데이터베이스 연결을 안전하게 관리
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            select_query = """
                SELECT DISTINCT
                    DCM.REP_ID,
                    LS.STORE_BUSINESS_NUMBER
                FROM LOCAL_STORE LS
                JOIN BUSINESS_AREA_CATEGORY BAC ON BAC.DETAIL_CATEGORY_NAME = LS.SMALL_CATEGORY_NAME
                JOIN DETAIL_CATEGORY_MAPPING DCM ON DCM.BUSINESS_AREA_CATEGORY_ID = BAC.BUSINESS_AREA_CATEGORY_ID
            ;"""

            cursor.execute(select_query)
            rows = cursor.fetchall()

            result = []
            for row in rows:
                if row["REP_ID"] is not None:
                    result.append(
                        LocalStoreMappingRepId(
                            store_business_number=row["STORE_BUSINESS_NUMBER"],
                            rep_id=row["REP_ID"],
                        )
                    )

            return result

    except Exception as e:
        logger.error(f"대표 ID를 가져오는 중 오류 발생: {e}")
        raise


def select_local_store_top5_menus(
    batch: List[LocalStoreMappingRepId],
) -> List[LocalStoreTop5Menu]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            # rep_id 리스트 생성
            rep_ids = [store_info.rep_id for store_info in batch]

            # IN 절을 사용하여 한 번에 조회
            select_query_top5 = """
                SELECT
                    BIZ_DETAIL_CATEGORY_ID,
                    TOP_MENU_1,
                    TOP_MENU_2,
                    TOP_MENU_3,
                    TOP_MENU_4,
                    TOP_MENU_5,
                    Y_M
                FROM COMMERCIAL_DISTRICT
                WHERE BIZ_DETAIL_CATEGORY_ID IN (%s)
            """
            # IN 절 파라미터 생성
            in_params = ",".join(["%s"] * len(rep_ids))
            query = select_query_top5 % in_params

            cursor.execute(query, rep_ids)
            rows = cursor.fetchall()

            # rep_id를 키로 하는 딕셔너리 생성
            top5_dict = {row["BIZ_DETAIL_CATEGORY_ID"]: row for row in rows}

            # batch의 순서를 유지하면서 결과 생성
            for store_info in batch:
                if store_info.rep_id in top5_dict:
                    result_top5 = top5_dict[store_info.rep_id]
                    results.append(
                        LocalStoreTop5Menu(
                            store_business_number=store_info.store_business_number,
                            detail_category_top1_ordered_menu=result_top5["TOP_MENU_1"],
                            detail_category_top2_ordered_menu=result_top5["TOP_MENU_2"],
                            detail_category_top3_ordered_menu=result_top5["TOP_MENU_3"],
                            detail_category_top4_ordered_menu=result_top5["TOP_MENU_4"],
                            detail_category_top5_ordered_menu=result_top5["TOP_MENU_5"],
                            nice_biz_map_data_ref_date=result_top5["Y_M"],
                        )
                    )

            return results

    except Exception as e:
        logger.error(f"Top 5 메뉴를 가져오는 중 오류 발생: {e}")
        raise


##################### 동별 인구 데이터 ##############################
### 매장별 읍/면/동 아이디 조회
def select_local_store_sub_district_id(
    batch_size: int = 5000,
) -> List[LocalStoreSubdistrictId]:  # Updated type hint
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            select_query = """
                SELECT
                    STORE_BUSINESS_NUMBER,
                    SUB_DISTRICT_ID
                FROM LOCAL_STORE
                ;
            """

            cursor.execute(select_query)

            results = []
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    if row["SUB_DISTRICT_ID"] is not None:
                        results.append(
                            LocalStoreSubdistrictId(
                                store_business_number=row["STORE_BUSINESS_NUMBER"],
                                sub_district_id=row["SUB_DISTRICT_ID"],
                            )
                        )

            return results

    except Exception as e:
        logger.error(f"Error fetching statistics data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


### 매장 읍/면/동 인구 데이터 가져오기
# def get_population_data_for_multiple_ids(sub_district_ids: List[int]):
#     connection = get_db_connection()
#     cursor = connection.cursor(pymysql.cursors.DictCursor)
#     logger = logging.getLogger(__name__)

#     try:
#         # 여러 SUB_DISTRICT_ID에 대해 인구 데이터를 조회하는 쿼리
#         format_strings = ",".join(
#             ["%s"] * len(sub_district_ids)
#         )  # %s를 sub_district_ids 개수만큼 생성
#         select_query = f"""
#             SELECT
#                 SUB_DISTRICT_ID,
#                 GENDER_ID,
#                 AGE_UNDER_10s,
#                 AGE_10s,
#                 AGE_20s,
#                 AGE_30s,
#                 AGE_40s,
#                 AGE_50s,
#                 AGE_PLUS_60s,
#                 TOTAL_POPULATION_BY_GENDER,
#                 TOTAL_POPULATION,
#                 REF_DATE
#             FROM POPULATION_AGE
#             WHERE SUB_DISTRICT_ID IN ({format_strings})
#             ORDER BY REF_DATE DESC
#             ;
#         """
#         cursor.execute(select_query, sub_district_ids)
#         return cursor.fetchall()


#     except Exception as e:
#         logger.error(
#             f"Error fetching population data for sub_district_ids {sub_district_ids}: {e}"
#         )
#         raise
#     finally:
#         cursor.close()
#         connection.close()
def get_population_data_for_multiple_ids(sub_district_ids: List[int]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        # 여러 SUB_DISTRICT_ID에 대해 인구 데이터를 조회하는 쿼리
        format_strings = ",".join(
            ["%s"] * len(sub_district_ids)
        )  # %s를 sub_district_ids 개수만큼 생성
        select_query = f"""
            SELECT
                pa.SUB_DISTRICT_ID,
                pa.GENDER_ID,
                pa.AGE_UNDER_10s,
                pa.AGE_10s,
                pa.AGE_20s,
                pa.AGE_30s,
                pa.AGE_40s,
                pa.AGE_50s,
                pa.AGE_PLUS_60s,
                pa.TOTAL_POPULATION_BY_GENDER,
                pa.TOTAL_POPULATION,
                pa.REF_DATE
            FROM POPULATION_AGE pa
            JOIN (
                SELECT SUB_DISTRICT_ID, MAX(REF_DATE) AS max_ref_date
                FROM POPULATION_AGE
                WHERE SUB_DISTRICT_ID IN ({format_strings})
                GROUP BY SUB_DISTRICT_ID
            ) AS recent
            ON pa.SUB_DISTRICT_ID = recent.SUB_DISTRICT_ID AND pa.REF_DATE = recent.max_ref_date
            ORDER BY pa.SUB_DISTRICT_ID;
        """
        cursor.execute(select_query, sub_district_ids)
        return cursor.fetchall()

    except Exception as e:
        logger.error(
            f"Error fetching population data for sub_district_ids {sub_district_ids}: {e}"
        )
        raise
    finally:
        cursor.close()
        connection.close()


def select_local_store_population_data(batch: List[LocalStoreSubdistrictId]):
    local_store_population_data = []

    # 배치로 처리하기 위해 서브리스트를 만들어서 조회
    sub_district_ids = [local_store.sub_district_id for local_store in batch]

    # 여러 개의 SUB_DISTRICT_ID에 대해 한번에 인구 데이터를 조회
    population_data_rows = get_population_data_for_multiple_ids(sub_district_ids)
    print(population_data_rows[0])
    print(population_data_rows[1])

    # 인구 데이터를 정리하기 위한 딕셔너리 초기화
    population_data_dict = {}

    # 가져온 인구 데이터를 딕셔너리에 저장
    for row in population_data_rows:
        key = row["SUB_DISTRICT_ID"]

        if key not in population_data_dict:
            population_data_dict[key] = {
                "total_population": row["TOTAL_POPULATION"],  # 총 인구
                "population_male": 0,
                "population_female": 0,
                "age_under_10": 0,
                "age_10s": 0,
                "age_20s": 0,
                "age_30s": 0,
                "age_40s": 0,
                "age_50s": 0,
                "age_60_over": 0,
                "ref_date": row["REF_DATE"],
            }

        # 성별 및 연령대별 인구 수 계산
        if row["GENDER_ID"] == 1:  # 남자
            population_data_dict[key]["population_male"] = row[
                "TOTAL_POPULATION_BY_GENDER"
            ]
            population_data_dict[key]["age_under_10"] += row["AGE_UNDER_10s"]
            population_data_dict[key]["age_10s"] += row["AGE_10s"]
            population_data_dict[key]["age_20s"] += row["AGE_20s"]
            population_data_dict[key]["age_30s"] += row["AGE_30s"]
            population_data_dict[key]["age_40s"] += row["AGE_40s"]
            population_data_dict[key]["age_50s"] += row["AGE_50s"]
            population_data_dict[key]["age_60_over"] += row["AGE_PLUS_60s"]
        elif row["GENDER_ID"] == 2:  # 여자
            population_data_dict[key]["population_female"] = row[
                "TOTAL_POPULATION_BY_GENDER"
            ]
            population_data_dict[key]["age_under_10"] += row["AGE_UNDER_10s"]
            population_data_dict[key]["age_10s"] += row["AGE_10s"]
            population_data_dict[key]["age_20s"] += row["AGE_20s"]
            population_data_dict[key]["age_30s"] += row["AGE_30s"]
            population_data_dict[key]["age_40s"] += row["AGE_40s"]
            population_data_dict[key]["age_50s"] += row["AGE_50s"]
            population_data_dict[key]["age_60_over"] += row["AGE_PLUS_60s"]

    # 각 local store에 대해 인구 데이터 생성
    for local_store in batch:
        sub_district_id = local_store.sub_district_id
        if sub_district_id in population_data_dict:
            total_population = population_data_dict[sub_district_id]["total_population"]
            population_male = population_data_dict[sub_district_id]["population_male"]
            population_female = population_data_dict[sub_district_id][
                "population_female"
            ]
            age_under_10 = population_data_dict[sub_district_id]["age_under_10"]
            age_10s = population_data_dict[sub_district_id]["age_10s"]
            age_20s = population_data_dict[sub_district_id]["age_20s"]
            age_30s = population_data_dict[sub_district_id]["age_30s"]
            age_40s = population_data_dict[sub_district_id]["age_40s"]
            age_50s = population_data_dict[sub_district_id]["age_50s"]
            age_60_over = population_data_dict[sub_district_id]["age_60_over"]
            ref_date = population_data_dict[sub_district_id]["ref_date"]

            # 성비 계산
            population_male_percent = (
                (population_male / total_population * 100)
                if total_population > 0
                else 0
            )
            population_female_percent = (
                (population_female / total_population * 100)
                if total_population > 0
                else 0
            )

            # LocalStorePopulationData 인스턴스 생성 및 추가
            local_store_population_data.append(
                LocalStorePopulationData(
                    store_business_number=local_store.store_business_number,
                    population_total=total_population,
                    population_male_percent=population_male_percent,
                    population_female_percent=population_female_percent,
                    population_age_10_under=age_under_10,
                    population_age_10s=age_10s,
                    population_age_20s=age_20s,
                    population_age_30s=age_30s,
                    population_age_40s=age_40s,
                    population_age_50s=age_50s,
                    population_age_60_over=age_60_over,
                    population_date_ref_date=ref_date,
                )
            )

    return local_store_population_data


##################### 입지분석 J_SCORE 가중치 평균 ##############################


######################## INSERT ######################################


# 매장 기본 정보 넣기
def insert_or_update_store_info_batch(batch: List[LocalStoreBasicInfo]) -> None:
    connection = get_service_report_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        if connection.open:
            insert_query = """
                INSERT INTO REPORT (
                    STORE_BUSINESS_NUMBER, CITY_NAME, DISTRICT_NAME, SUB_DISTRICT_NAME,
                    DETAIL_CATEGORY_NAME, STORE_NAME, ROAD_NAME, BUILDING_NAME,
                    FLOOR_INFO, LATITUDE, LONGITUDE
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    CITY_NAME = VALUES(CITY_NAME),
                    DISTRICT_NAME = VALUES(DISTRICT_NAME),
                    SUB_DISTRICT_NAME = VALUES(SUB_DISTRICT_NAME),
                    DETAIL_CATEGORY_NAME = VALUES(DETAIL_CATEGORY_NAME),
                    STORE_NAME = VALUES(STORE_NAME),
                    ROAD_NAME = VALUES(ROAD_NAME),
                    BUILDING_NAME = VALUES(BUILDING_NAME),
                    FLOOR_INFO = VALUES(FLOOR_INFO),
                    LATITUDE = VALUES(LATITUDE),
                    LONGITUDE = VALUES(LONGITUDE)
            """

            values = [
                (
                    store_info.store_business_number,
                    store_info.city_name,
                    store_info.district_name,
                    store_info.sub_district_name,
                    store_info.detail_category_name or "소분류 없음",
                    store_info.store_name or "매장명 없음",
                    store_info.road_name,
                    store_info.building_name,
                    store_info.floor_info,
                    store_info.latitude,
                    store_info.longitude,
                )
                for store_info in batch
            ]

            cursor.executemany(insert_query, values)
            connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating store info: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


# 매장 top5 정보 넣기
def insert_or_update_top5_batch(batch: List[LocalStoreTop5Menu]) -> None:
    connection = get_service_report_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        if connection.open:
            insert_query = """
                INSERT INTO REPORT (
                    STORE_BUSINESS_NUMBER,
                    DETAIL_CATEGORY_TOP1_ORDERED_MENU, 
                    DETAIL_CATEGORY_TOP2_ORDERED_MENU,
                    DETAIL_CATEGORY_TOP3_ORDERED_MENU, 
                    DETAIL_CATEGORY_TOP4_ORDERED_MENU,
                    DETAIL_CATEGORY_TOP5_ORDERED_MENU,
                    NICE_BIZ_MAP_DATA_REF_DATE
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    DETAIL_CATEGORY_TOP1_ORDERED_MENU = VALUES(DETAIL_CATEGORY_TOP1_ORDERED_MENU),
                    DETAIL_CATEGORY_TOP2_ORDERED_MENU = VALUES(DETAIL_CATEGORY_TOP2_ORDERED_MENU),
                    DETAIL_CATEGORY_TOP3_ORDERED_MENU = VALUES(DETAIL_CATEGORY_TOP3_ORDERED_MENU),
                    DETAIL_CATEGORY_TOP4_ORDERED_MENU = VALUES(DETAIL_CATEGORY_TOP4_ORDERED_MENU),
                    DETAIL_CATEGORY_TOP5_ORDERED_MENU = VALUES(DETAIL_CATEGORY_TOP5_ORDERED_MENU),
                    NICE_BIZ_MAP_DATA_REF_DATE = VALUES(NICE_BIZ_MAP_DATA_REF_DATE)
            """

            values = [
                (
                    store_info.store_business_number,
                    store_info.detail_category_top1_ordered_menu,
                    store_info.detail_category_top2_ordered_menu,
                    store_info.detail_category_top3_ordered_menu,
                    store_info.detail_category_top4_ordered_menu,
                    store_info.detail_category_top5_ordered_menu,
                    store_info.nice_biz_map_data_ref_date,
                )
                for store_info in batch
            ]

            cursor.executemany(insert_query, values)
            connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating top5 menu data: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


# 매장 읍/면/동 인구 데이터 넣기
def insert_or_update_population_data_batch(batch: List[LocalStoreTop5Menu]) -> None:
    connection = get_service_report_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        if connection.open:
            insert_query = """
                INSERT INTO REPORT (
                    STORE_BUSINESS_NUMBER,
                    POPULATION_TOTAL, 
                    POPULATION_MALE_PERCENT,
                    POPULATION_FEMALE_PERCENT, 
                    POPULATION_AGE_10_UNDER,
                    POPULATION_AGE_10S,
                    POPULATION_AGE_20S,
                    POPULATION_AGE_30S,
                    POPULATION_AGE_40S,
                    POPULATION_AGE_50S,
                    POPULATION_AGE_60_OVER,
                    POPULATION_DATA_REF_DATE
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    POPULATION_TOTAL = VALUES(POPULATION_TOTAL),
                    POPULATION_MALE_PERCENT = VALUES(POPULATION_MALE_PERCENT),
                    POPULATION_FEMALE_PERCENT = VALUES(POPULATION_FEMALE_PERCENT),
                    POPULATION_AGE_10_UNDER = VALUES(POPULATION_AGE_10_UNDER),
                    POPULATION_AGE_10S = VALUES(POPULATION_AGE_10S),
                    POPULATION_AGE_20S = VALUES(POPULATION_AGE_20S),
                    POPULATION_AGE_30S = VALUES(POPULATION_AGE_30S),
                    POPULATION_AGE_40S = VALUES(POPULATION_AGE_40S),
                    POPULATION_AGE_50S = VALUES(POPULATION_AGE_50S),
                    POPULATION_AGE_60_OVER = VALUES(POPULATION_AGE_60_OVER),
                    POPULATION_DATA_REF_DATE = VALUES(POPULATION_DATA_REF_DATE)
            """

            values = [
                (
                    store_info.store_business_number,
                    store_info.population_total,
                    store_info.population_male_percent,
                    store_info.population_female_percent,
                    store_info.population_age_10_under,
                    store_info.population_age_10s,
                    store_info.population_age_20s,
                    store_info.population_age_30s,
                    store_info.population_age_40s,
                    store_info.population_age_50s,
                    store_info.population_age_60_over,
                    store_info.population_date_ref_date,
                )
                for store_info in batch
            ]

            cursor.executemany(insert_query, values)
            connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating top5 menu data: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
