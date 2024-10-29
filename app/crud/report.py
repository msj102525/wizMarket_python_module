from collections import defaultdict
import logging
from statistics import mean
from typing import Dict, List
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
    LocalStoreCDDistrictAverageSalesTop5,
    LocalStoreCDJSWeightedAverage,
    LocalStoreCDWeekdayTiemAveragePercent,
    LocalStoreCommercialDistrictJscoreAverage,
    LocalStoreLIJSWeightedAverage,
    LocalStoreLocInfoData,
    LocalStoreLocInfoJscoreData,
    LocalStoreMainCategoryCount,
    LocalStoreMappingSubDistrictDetailCategoryId,
    LocalStoreMappingRepId,
    LocalStoreMovePopData,
    LocalStorePopulationData,
    LocalStoreResidentWorkPopData,
    LocalStoreRisingBusinessNTop5SDTop3,
    LocalStoreSubdistrictId,
    LocalStoreTop5Menu,
    Report,
)


##################### SELECT ##############################
##################### 기본 매장 정보 넣기 ##############################
def select_local_store_info(batch_size: int = 5000) -> List[LocalStoreBasicInfo]:
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
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
                    AND LOCAL_YEAR = (SELECT MAX(LOCAL_YEAR) FROM LOCAL_STORE)
                    AND LOCAL_QUARTER = (SELECT MAX(LOCAL_QUARTER) FROM LOCAL_STORE)
                    ;
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
        logger.error(f"Error LocalStoreBasicInfo data: {e}")
        raise


##################### 매장마다 소분류별 뜨는 메뉴 TOP5 넣기 ##############################


def select_local_store_sub_district_rep_id() -> List[LocalStoreMappingRepId]:
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                select_query = """
                    SELECT DISTINCT
                        LS.STORE_BUSINESS_NUMBER,
                        LS.SUB_DISTRICT_ID,
                        DCM.REP_ID
                    FROM LOCAL_STORE LS
                    JOIN BUSINESS_AREA_CATEGORY BAC ON BAC.DETAIL_CATEGORY_NAME = LS.SMALL_CATEGORY_NAME
                    JOIN DETAIL_CATEGORY_MAPPING DCM ON DCM.BUSINESS_AREA_CATEGORY_ID = BAC.BUSINESS_AREA_CATEGORY_ID
                    WHERE LS.SUB_DISTRICT_ID IS NOT NULL
                ;
                """

                cursor.execute(select_query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    if row["REP_ID"] is not None:
                        result.append(
                            LocalStoreMappingRepId(
                                store_business_number=row["STORE_BUSINESS_NUMBER"],
                                sub_district_id=row["SUB_DISTRICT_ID"],
                                rep_id=row["REP_ID"],
                            )
                        )

                return result

    except Exception as e:
        logger.error(f"LocalStoreMappingRepId 가져오는 중 오류 발생: {e}")
        raise


def select_local_store_top5_menus(
    batch: List[LocalStoreMappingRepId],
) -> List[LocalStoreTop5Menu]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:

                # rep_id 리스트 생성
                rep_ids = [store_info.rep_id for store_info in batch]

                # IN 절을 사용하여 한 번에 조회
                select_query_top5 = """
                    SELECT DISTINCT
                        BIZ_DETAIL_CATEGORY_ID,
                        TOP_MENU_1,
                        TOP_MENU_2,
                        TOP_MENU_3,
                        TOP_MENU_4,
                        TOP_MENU_5,
                        Y_M
                    FROM COMMERCIAL_DISTRICT
                    WHERE BIZ_DETAIL_CATEGORY_ID IN (%s)
                    AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
                    ;
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
                                detail_category_top1_ordered_menu=result_top5[
                                    "TOP_MENU_1"
                                ],
                                detail_category_top2_ordered_menu=result_top5[
                                    "TOP_MENU_2"
                                ],
                                detail_category_top3_ordered_menu=result_top5[
                                    "TOP_MENU_3"
                                ],
                                detail_category_top4_ordered_menu=result_top5[
                                    "TOP_MENU_4"
                                ],
                                detail_category_top5_ordered_menu=result_top5[
                                    "TOP_MENU_5"
                                ],
                                nice_biz_map_data_ref_date=result_top5["Y_M"],
                            )
                        )

            return results

    except Exception as e:
        logger.error(f"LocalStoreTop5Menu 가져오는 중 오류 발생: {e}")
        raise


##################### 동별 인구 데이터 ##############################
### 매장별 읍/면/동 아이디 조회
def select_local_store_sub_district_id(
    batch_size: int = 5000,
) -> List[LocalStoreSubdistrictId]:
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                select_query = """
                    SELECT
                        STORE_BUSINESS_NUMBER,
                        SUB_DISTRICT_ID
                    FROM LOCAL_STORE
                    WHERE LOCAL_YEAR = (SELECT MAX(LOCAL_YEAR) FROM LOCAL_STORE)
                    AND LOCAL_QUARTER = (SELECT MAX(LOCAL_QUARTER) FROM LOCAL_STORE)
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
        logger.error(f"Error LocalStoreSubdistrictId data: {e}")
        raise


def get_population_data_for_multiple_ids(sub_district_ids: List[int]):
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
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
        logger.error(f"population data for sub_district_ids {sub_district_ids}: {e}")
        raise


def select_local_store_population_data(batch: List[LocalStoreSubdistrictId]):
    logger = logging.getLogger(__name__)
    local_store_population_data = []

    # 배치로 처리하기 위해 서브리스트를 만들어서 조회
    sub_district_ids = [local_store.sub_district_id for local_store in batch]

    # 여러 개의 SUB_DISTRICT_ID에 대해 한번에 인구 데이터를 조회
    population_data_rows = get_population_data_for_multiple_ids(sub_district_ids)

    # 인구 데이터를 정리하기 위한 딕셔너리 초기화
    population_data_dict = {}
    try:

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
                total_population = population_data_dict[sub_district_id][
                    "total_population"
                ]
                population_male = population_data_dict[sub_district_id][
                    "population_male"
                ]
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
    except Exception as e:
        logger.error(f"LocalStoreSubdistrictId 생성 중 오류 발생: {e}")
        raise


##################### 입지분석 J_SCORE 가중치 평균 ##############################
def select_local_store_loc_info_j_score_average_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreLIJSWeightedAverage]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # IN 절을 사용하여 한 번에 조회
            select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    J_SCORE_PER
                FROM
                    LOC_INFO_STATISTICS
                WHERE TARGET_ITEM = 'j_score_avg'
                AND STAT_LEVEL = '전국'
                AND SUB_DISTRICT_ID IN ({})
                AND REF_DATE = (SELECT MAX(REF_DATE) FROM LOC_INFO_STATISTICS)
                ;
            """
            in_params = ",".join(["%s"] * len(sub_district_ids))
            query = select_query.format(in_params)

            cursor.execute(query, sub_district_ids)

            rows = cursor.fetchall()

            loc_info_dict = {row["SUB_DISTRICT_ID"]: row for row in rows}

            # batch의 순서를 유지하면서 결과 생성
            for store_info in batch:
                if store_info.sub_district_id in loc_info_dict:
                    loc_info_data = loc_info_dict[store_info.sub_district_id]

                    results.append(
                        LocalStoreLIJSWeightedAverage(
                            store_business_number=store_info.store_business_number,
                            loc_info_j_score_average=(
                                loc_info_data["J_SCORE_PER"] or 0
                            ),
                        ),
                    )

            return results

    except Exception as e:
        logger.error(f"LocalStoreLIJSWeightedAverage 가져오는 중 오류 발생: {e}")
        raise


##################### 입지분석 데이터 ##############################
def select_local_store_loc_info_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreLocInfoData]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # IN 절을 사용하여 한 번에 조회
            select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    RESIDENT,
                    WORK_POP,
                    MOVE_POP,
                    SHOP,
                    INCOME,
                    Y_M
                FROM LOC_INFO
                WHERE SUB_DISTRICT_ID IN (%s)
                AND Y_M = (SELECT MAX(Y_M) FROM LOC_INFO)
            ;
            """
            # IN 절 파라미터 생성
            in_params = ",".join(["%s"] * len(sub_district_ids))
            query = select_query % (in_params)

            cursor.execute(query, sub_district_ids)  # sub_district_ids를 한 번만 사용

            rows = cursor.fetchall()

            # sub_district_id를 키로 하는 딕셔너리 생성
            loc_info_dict = {
                row["SUB_DISTRICT_ID"]: row for row in rows
            }  # 딕셔너리에 SUB_DISTRICT_ID 포함

            # batch의 순서를 유지하면서 결과 생성
            for store_info in batch:
                if store_info.sub_district_id in loc_info_dict:

                    loc_info_data = loc_info_dict[store_info.sub_district_id]

                    results.append(
                        LocalStoreLocInfoData(
                            store_business_number=store_info.store_business_number,
                            loc_info_resident_k=round(
                                (loc_info_data["RESIDENT"] or 0) / 1000, 1
                            ),
                            loc_info_work_pop_k=round(
                                (loc_info_data["WORK_POP"] or 0) / 1000, 1
                            ),
                            loc_info_move_pop_k=round(
                                (loc_info_data["MOVE_POP"] or 0) / 1000, 1
                            ),
                            loc_info_shop_k=round(
                                (loc_info_data["SHOP"] or 0) / 1000, 1
                            ),
                            loc_info_income_won=round((loc_info_data["INCOME"] or 0) / 10000),
                            loc_info_data_ref_date=loc_info_data["Y_M"],
                        )
                    )

            return results

    except Exception as e:
        logger.error(f"LocalStoreLocInfoData 가져오는 중 오류 발생: {e}")
        raise


##################### 입지분석 J_SCORE 데이터 ##############################
def select_local_store_loc_info_j_score_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreLocInfoJscoreData]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            cursor2 = connection.cursor(pymysql.cursors.DictCursor)

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # loc_info 통계 조회
            loc_select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    TARGET_ITEM,
                    J_SCORE
                FROM LOC_INFO_STATISTICS
                WHERE SUB_DISTRICT_ID IN (%s)
                AND STAT_LEVEL = '전국'
                AND REF_DATE = (SELECT MAX(REF_DATE) FROM LOC_INFO_STATISTICS)
            ;
            """
            in_params = ",".join(["%s"] * len(sub_district_ids))
            loc_query = loc_select_query % (in_params)

            cursor.execute(loc_query, sub_district_ids)
            loc_rows = cursor.fetchall()

            # loc_info 점수 딕셔너리 생성
            loc_info_j_score_dict = {
                (row["SUB_DISTRICT_ID"], row["TARGET_ITEM"]): row["J_SCORE"]
                for row in loc_rows
            }

            # population mz 통계 조회
            pop_select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    J_SCORE
                FROM POPULATION_INFO_MZ_STATISTICS
                WHERE SUB_DISTRICT_ID IN (%s)
                AND STAT_LEVEL = '전국'
                AND REF_DATE = (SELECT MAX(REF_DATE) FROM POPULATION_INFO_MZ_STATISTICS)
            ;
            """
            pop_query = pop_select_query % (in_params)
            cursor2.execute(pop_query, sub_district_ids)
            pop_rows = cursor2.fetchall()

            # pop_info 점수 딕셔너리 생성
            pop_info_j_score_dict = {
                row["SUB_DISTRICT_ID"]: row["J_SCORE"] for row in pop_rows
            }

            # 결과 생성
            for store_info in batch:
                sub_district_id = store_info.sub_district_id
                results.append(
                    LocalStoreLocInfoJscoreData(
                        store_business_number=store_info.store_business_number,
                        loc_info_resident_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "resident"), 0.0
                        ),
                        loc_info_work_pop_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "work_pop"), 0.0
                        ),
                        loc_info_move_pop_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "move_pop"), 0.0
                        ),
                        loc_info_shop_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "shop"), 0.0
                        ),
                        loc_info_income_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "income"), 0.0
                        ),
                        loc_info_average_spend_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "spend"), 0.0
                        ),
                        loc_info_average_sales_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "sales"), 0.0
                        ),
                        loc_info_house_j_score=loc_info_j_score_dict.get(
                            (sub_district_id, "house"), 0.0
                        ),
                        population_mz_population_j_score=pop_info_j_score_dict.get(
                            sub_district_id, 0.0
                        ),
                    )
                )

            return results

    except Exception as e:
        logger.error(f"LocalStoreLocInfoJscoreData 가져오는 중 오류 발생: {e}")
        raise


##################### 입지분석 주거인구 직장인구 ##############################
def select_local_store_loc_info_resident_work_pop_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreLocInfoData]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # IN 절을 사용하여 한 번에 조회
            select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    RESIDENT,
                    WORK_POP
                FROM
                    LOC_INFO
                WHERE SUB_DISTRICT_ID IN (%s)
                AND Y_M = (SELECT MAX(Y_M) FROM LOC_INFO)
                ;
            """
            # IN 절 파라미터 생성
            in_params = ",".join(["%s"] * len(sub_district_ids))
            query = select_query % (in_params)

            cursor.execute(query, sub_district_ids)  # sub_district_ids를 한 번만 사용

            rows = cursor.fetchall()

            # sub_district_id를 키로 하는 딕셔너리 생성
            loc_info_dict = {
                row["SUB_DISTRICT_ID"]: row for row in rows
            }  # 딕셔너리에 SUB_DISTRICT_ID 포함

            # batch의 순서를 유지하면서 결과 생성
            for store_info in batch:
                if store_info.sub_district_id in loc_info_dict:

                    loc_info_data = loc_info_dict[store_info.sub_district_id]
                    resident = loc_info_data["RESIDENT"] or 0
                    work_pop = loc_info_data["WORK_POP"] or 0

                    # 두 값을 더하여 전체 인구 계산
                    total_pop = resident + work_pop

                    # 퍼센트 계산 (전체 인구가 0일 경우 0으로 처리)
                    loc_info_resident_percent = (
                        (resident / total_pop * 100) if total_pop > 0 else 0
                    )
                    loc_info_work_pop_percent = (
                        (work_pop / total_pop * 100) if total_pop > 0 else 0
                    )

                    results.append(
                        LocalStoreResidentWorkPopData(
                            store_business_number=store_info.store_business_number,
                            loc_info_resident=resident,
                            loc_info_work_pop=work_pop,
                            loc_info_resident_percent=loc_info_resident_percent,
                            loc_info_work_pop_percent=loc_info_work_pop_percent,
                        )
                    )

            return results

    except Exception as e:
        logger.error(f"LocalStoreResidentWorkPopData 가져오는 중 오류 발생: {e}")
        raise


##################### 입지분석 유동인구, 시/도 평균 유동인구 ##############################
def select_local_store_loc_info_move_pop_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreMovePopData]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            cursor2 = connection.cursor(pymysql.cursors.DictCursor)

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # loc_info 통계 조회
            loc_select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    MOVE_POP
                FROM
                    LOC_INFO
                WHERE
                    SUB_DISTRICT_ID IN (%s)
                AND Y_M = (SELECT MAX(Y_M) FROM LOC_INFO)
                ;
            """
            in_params = ",".join(["%s"] * len(sub_district_ids))
            loc_query = loc_select_query % (in_params)

            cursor.execute(loc_query, sub_district_ids)
            loc_rows = cursor.fetchall()

            # sub_district_id를 키로 하여 MOVE_POP 값을 저장하는 딕셔너리 생성
            loc_info_move_pop_dict = {
                row["SUB_DISTRICT_ID"]: row["MOVE_POP"] or 0 for row in loc_rows
            }

            # population mz 통계 조회 (시/도 평균 유동인구)
            pop_select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    AVG_VAL AS CITY_MOVE_POP
                FROM
                    LOC_INFO_STATISTICS
                WHERE SUB_DISTRICT_ID IN (%s)
                AND TARGET_ITEM = 'move_pop'
                AND STAT_LEVEL = '시/도'
                AND REF_DATE = (SELECT MAX(REF_DATE) FROM LOC_INFO_STATISTICS)
                ;
            """
            pop_query = pop_select_query % (in_params)
            cursor2.execute(pop_query, sub_district_ids)
            pop_rows = cursor2.fetchall()

            # sub_district_id를 키로 하여 CITY_MOVE_POP 값을 저장하는 딕셔너리 생성
            pop_info_city_move_pop_dict = {
                row["SUB_DISTRICT_ID"]: row["CITY_MOVE_POP"] or 0.0 for row in pop_rows
            }

            # 결과 생성
            for store_info in batch:
                sub_district_id = store_info.sub_district_id
                results.append(
                    LocalStoreMovePopData(
                        store_business_number=store_info.store_business_number,
                        loc_info_move_pop=loc_info_move_pop_dict.get(
                            sub_district_id, 0
                        ),
                        loc_info_city_move_pop=round(
                            pop_info_city_move_pop_dict.get(sub_district_id, 0.0)
                        ),
                    )
                )

            return results

    except Exception as e:
        logger.error(f"LocalStoreMovePopData 가져오는 중 오류 발생: {e}")
        raise


##################### 상권분석 J_Score 가중치 평균 ##############################


def select_commercial_district_j_score_weighted_average_data(
    mappings: List[LocalStoreMappingSubDistrictDetailCategoryId],
) -> List[LocalStoreCDJSWeightedAverage]:

    logger = logging.getLogger(__name__)

    store_mappings: Dict[str, Dict] = defaultdict(
        lambda: {"sub_district_id": None, "detail_categories": set()}
    )

    for mapping in mappings:
        store_data = store_mappings[mapping.store_business_number]
        store_data["sub_district_id"] = mapping.sub_district_id
        store_data["detail_categories"].add(mapping.detail_category_id)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                store_j_scores: Dict[str, Dict[str, List[float]]] = defaultdict(
                    lambda: {
                        "j_score": [],
                    }
                )

                for store_number, store_data in store_mappings.items():
                    detail_categories_str = ", ".join(
                        map(str, store_data["detail_categories"])
                    )

                    query = """
                    SELECT
                        BIZ_DETAIL_CATEGORY_ID,
                        J_SCORE_PER_AVG
                    FROM
                        COMMERCIAL_DISTRICT_WEIGHTED_AVERAGE
                    WHERE SUB_DISTRICT_ID = %s
                    AND BIZ_DETAIL_CATEGORY_ID IN ({});
                    """.format(
                        detail_categories_str
                    )

                    cursor.execute(query, (store_data["sub_district_id"],))
                    scores = cursor.fetchall()

                    store_scores = store_j_scores[store_number]
                    for score in scores:
                        if score["BIZ_DETAIL_CATEGORY_ID"] is not None:
                            if score["J_SCORE_PER_AVG"] is not None:
                                store_scores["j_score"].append(
                                    float(score["J_SCORE_PER_AVG"])
                                )

                results = []
                for store_number, scores in store_j_scores.items():
                    try:
                        result = LocalStoreCDJSWeightedAverage(
                            store_business_number=store_number,
                            commercial_district_j_score_average=(
                                mean(scores["j_score"]) if scores["j_score"] else 0.0
                            ),
                        )
                        results.append(result)
                    except Exception as e:
                        logger.error(
                            f"Error calculating averages for store {store_number}: {e}"
                        )
                        continue

                return results

    except Exception as e:
        logger.error(f"Error processing batch J scores weighted: {e}")
        raise


##################### 상권분석 읍/면/동 대분류 갯수 ##############################
def select_commercial_district_main_detail_category_count_data(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreMainCategoryCount]:
    logger = logging.getLogger(__name__)
    results = []
    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]
            # IN 절을 사용하여 한 번에 조회
            select_query = """
                SELECT
                    SUB_DISTRICT_ID,
                    BIZ_MAIN_CATEGORY_ID,
                    COUNT(BIZ_MAIN_CATEGORY_ID) as category_count
                FROM
                    COMMERCIAL_DISTRICT
                WHERE SUB_DISTRICT_ID IN (%s)
                AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
                GROUP BY SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID
                ;
            """
            # IN 절 파라미터 생성
            in_params = ",".join(["%s"] * len(sub_district_ids))
            query = select_query % (in_params)
            cursor.execute(query, sub_district_ids)
            rows = cursor.fetchall()

            # sub_district_id별로 카테고리 카운트를 정리하는 딕셔너리
            category_counts = {}

            # 결과를 정리
            for row in rows:
                sub_district_id = row["SUB_DISTRICT_ID"]
                if sub_district_id not in category_counts:
                    category_counts[sub_district_id] = {
                        1: 0,  # food
                        3: 0,  # retail
                        4: 0,  # lifestyle
                        5: 0,  # entertainment
                        6: 0,  # education
                        7: 0,  # healthcare
                    }
                category_counts[sub_district_id][row["BIZ_MAIN_CATEGORY_ID"]] = row[
                    "category_count"
                ]

            # batch의 순서를 유지하면서 결과 생성
            for store_info in batch:
                counts = category_counts.get(store_info.sub_district_id, {})
                results.append(
                    LocalStoreMainCategoryCount(
                        store_business_number=store_info.store_business_number,
                        commercial_district_food_business_count=counts.get(1, 0),
                        commercial_district_retail_business_count=counts.get(3, 0),
                        commercial_district_lifestyle_business_count=counts.get(4, 0),
                        commercial_district_entertainment_business_count=counts.get(
                            5, 0
                        ),
                        commercial_district_education_business_count=counts.get(6, 0),
                        commercial_district_healthcare_business_count=counts.get(7, 0),
                    )
                )
            return results

    except Exception as e:
        logger.error(f"LocalStoreMainCategoryCount 가져오는 중 오류 발생: {e}")
        raise


##################### 상권분석 동별 매핑 소분류별 평균 jscore ##############################
def select_local_store_mp_detail_cateogry_id() -> (
    List[LocalStoreMappingSubDistrictDetailCategoryId]
):
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                select_query = """
                    SELECT
                        LS.STORE_BUSINESS_NUMBER,
                        LS.SUB_DISTRICT_ID,
                        DCM.DETAIL_CATEGORY_ID
                    FROM LOCAL_STORE LS
                    JOIN BUSINESS_AREA_CATEGORY BAC ON BAC.DETAIL_CATEGORY_NAME = LS.SMALL_CATEGORY_NAME
                    JOIN DETAIL_CATEGORY_MAPPING DCM ON DCM.BUSINESS_AREA_CATEGORY_ID = BAC.BUSINESS_AREA_CATEGORY_ID
                    ;
                """

                cursor.execute(select_query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    # None 값이 있는지 확인하고 제외
                    if None not in (
                        row["STORE_BUSINESS_NUMBER"],
                        row["SUB_DISTRICT_ID"],
                        row["DETAIL_CATEGORY_ID"],
                    ):
                        result.append(
                            LocalStoreMappingSubDistrictDetailCategoryId(
                                store_business_number=row["STORE_BUSINESS_NUMBER"],
                                sub_district_id=row["SUB_DISTRICT_ID"],
                                detail_category_id=row["DETAIL_CATEGORY_ID"],
                            )
                        )

                return result

    except Exception as e:
        logger.error(
            f"LocalStoreMappingSubDistrictDetailCategoryId를 가져오는 중 오류 발생: {e}"
        )
        raise


def select_commercial_district_j_score_average_data(
    mappings: List[LocalStoreMappingSubDistrictDetailCategoryId],
) -> List[LocalStoreCommercialDistrictJscoreAverage]:

    logger = logging.getLogger(__name__)

    store_mappings: Dict[str, Dict] = defaultdict(
        lambda: {"sub_district_id": None, "detail_categories": set()}
    )

    for mapping in mappings:
        store_data = store_mappings[mapping.store_business_number]
        store_data["sub_district_id"] = mapping.sub_district_id
        store_data["detail_categories"].add(mapping.detail_category_id)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                store_j_scores: Dict[str, Dict[str, List[float]]] = defaultdict(
                    lambda: {
                        "market_size": [],
                        "average_sales": [],
                        "usage_count": [],
                        "sub_district_density": [],
                        "average_payment": [],
                    }
                )

                for store_number, store_data in store_mappings.items():
                    detail_categories_str = ", ".join(
                        map(str, store_data["detail_categories"])
                    )

                    query = """
                    SELECT 
                        SD.SUB_DISTRICT_ID,
                        COALESCE(CDMSS.BIZ_DETAIL_CATEGORY_ID, CDUCS.BIZ_DETAIL_CATEGORY_ID, 
                                CDASS.BIZ_DETAIL_CATEGORY_ID, CDSDDS.BIZ_DETAIL_CATEGORY_ID, 
                                CDAPS.BIZ_DETAIL_CATEGORY_ID) AS BIZ_DETAIL_CATEGORY_ID,
                        CDMSS.J_SCORE_PER AS MARKET_SIZE_J,
                        CDUCS.J_SCORE_PER AS USAGE_COUNT_J,
                        CDASS.J_SCORE_PER AS AVERAGE_SALES_J,
                        CDSDDS.J_SCORE_PER AS SUB_DISTRICT_DENSITY_J,
                        CDAPS.J_SCORE_PER AS AVERAGE_PAYMENT_J
                    FROM
                        SUB_DISTRICT SD
                    LEFT JOIN COMMERCIAL_DISTRICT_MARKET_SIZE_STATISTICS CDMSS 
                        ON CDMSS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                        AND CDMSS.BIZ_DETAIL_CATEGORY_ID IN ({})
                    LEFT JOIN COMMERCIAL_DISTRICT_USEAGE_COUNT_STATISTICS CDUCS 
                        ON CDUCS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                        AND CDUCS.BIZ_DETAIL_CATEGORY_ID = CDMSS.BIZ_DETAIL_CATEGORY_ID
                    LEFT JOIN COMMERCIAL_DISTRICT_AVERAGE_SALES_STATISTICS CDASS 
                        ON CDASS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                        AND CDASS.BIZ_DETAIL_CATEGORY_ID = CDMSS.BIZ_DETAIL_CATEGORY_ID
                    LEFT JOIN COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_STATISTICS CDSDDS 
                        ON CDSDDS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                        AND CDSDDS.BIZ_DETAIL_CATEGORY_ID = CDMSS.BIZ_DETAIL_CATEGORY_ID
                    LEFT JOIN COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_STATISTICS CDAPS 
                        ON CDAPS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                        AND CDAPS.BIZ_DETAIL_CATEGORY_ID = CDMSS.BIZ_DETAIL_CATEGORY_ID
                    WHERE
                        SD.SUB_DISTRICT_ID = %s
                    AND CDMSS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_MARKET_SIZE_STATISTICS)
                    AND CDUCS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_USEAGE_COUNT_STATISTICS)
                    AND CDASS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_AVERAGE_SALES_STATISTICS)
                    AND CDSDDS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_STATISTICS)
                    AND CDAPS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_STATISTICS)
                    ;
                    """.format(
                        detail_categories_str
                    )

                    cursor.execute(query, (store_data["sub_district_id"],))
                    scores = cursor.fetchall()

                    store_scores = store_j_scores[store_number]
                    for score in scores:
                        if score["BIZ_DETAIL_CATEGORY_ID"] is not None:
                            if score["MARKET_SIZE_J"] is not None:
                                store_scores["market_size"].append(
                                    float(score["MARKET_SIZE_J"])
                                )
                            if score["AVERAGE_SALES_J"] is not None:
                                store_scores["average_sales"].append(
                                    float(score["AVERAGE_SALES_J"])
                                )
                            if score["USAGE_COUNT_J"] is not None:
                                store_scores["usage_count"].append(
                                    float(score["USAGE_COUNT_J"])
                                )
                            if score["SUB_DISTRICT_DENSITY_J"] is not None:
                                store_scores["sub_district_density"].append(
                                    float(score["SUB_DISTRICT_DENSITY_J"])
                                )
                            if score["AVERAGE_PAYMENT_J"] is not None:
                                store_scores["average_payment"].append(
                                    float(score["AVERAGE_PAYMENT_J"])
                                )

                # Calculate averages for each store
                results = []
                for store_number, scores in store_j_scores.items():
                    try:
                        result = LocalStoreCommercialDistrictJscoreAverage(
                            store_business_number=store_number,
                            commercial_district_market_size_j_socre=(
                                mean(scores["market_size"])
                                if scores["market_size"]
                                else 0.0
                            ),
                            commercial_district_average_sales_j_socre=(
                                mean(scores["average_sales"])
                                if scores["average_sales"]
                                else 0.0
                            ),
                            commercial_district_usage_count_j_socre=(
                                mean(scores["usage_count"])
                                if scores["usage_count"]
                                else 0.0
                            ),
                            commercial_district_sub_district_density_j_socre=(
                                mean(scores["sub_district_density"])
                                if scores["sub_district_density"]
                                else 0.0
                            ),
                            commercial_district_sub_average_payment_j_socre=(
                                mean(scores["average_payment"])
                                if scores["average_payment"]
                                else 0.0
                            ),
                        )
                        results.append(result)
                    except Exception as e:
                        logger.error(
                            f"Error calculating averages for store {store_number}: {e}"
                        )
                        continue

                return results

    except Exception as e:
        logger.error(f"Error processing batch J scores: {e}")
        raise


######################## 상권분석 동별 소분류별 요일,시간대 매출 비중 ######################################
def select_local_store_weekday_time_average_sales_data(
    batch: List[LocalStoreMappingRepId],
) -> List[LocalStoreCDWeekdayTiemAveragePercent]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:

                # sub_district_id와 rep_id의 조합 리스트 생성
                query_params = [
                    (store_info.sub_district_id, store_info.rep_id)
                    for store_info in batch
                ]

                # IN 절에 사용할 플레이스홀더 생성
                placeholders = ", ".join(["(%s, %s)"] * len(query_params))

                # 쿼리 작성
                select_query = f"""
                    SELECT
                        SUB_DISTRICT_ID,
                        BIZ_DETAIL_CATEGORY_ID,
                        AVG_PROFIT_PER_MON,
                        AVG_PROFIT_PER_TUE,
                        AVG_PROFIT_PER_WED,
                        AVG_PROFIT_PER_THU,
                        AVG_PROFIT_PER_FRI,
                        AVG_PROFIT_PER_SAT,
                        AVG_PROFIT_PER_SUN,
                        AVG_PROFIT_PER_06_09,
                        AVG_PROFIT_PER_09_12,
                        AVG_PROFIT_PER_12_15,
                        AVG_PROFIT_PER_15_18,
                        AVG_PROFIT_PER_18_21,
                        AVG_PROFIT_PER_21_24
                    FROM COMMERCIAL_DISTRICT
                    WHERE (SUB_DISTRICT_ID, BIZ_DETAIL_CATEGORY_ID) IN ({placeholders})
                    AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
                    ;
                """

                # 쿼리 실행
                flat_query_params = [
                    item for sublist in query_params for item in sublist
                ]  # flatten list
                cursor.execute(select_query, flat_query_params)
                rows = cursor.fetchall()

                # 결과를 딕셔너리로 변환
                weekday_tiem_average_dict = {
                    (row["SUB_DISTRICT_ID"], row["BIZ_DETAIL_CATEGORY_ID"]): row
                    for row in rows
                }

                # batch의 순서를 유지하면서 결과 생성
                for store_info in batch:
                    key = (store_info.sub_district_id, store_info.rep_id)
                    result_row = weekday_tiem_average_dict.get(key)
                    if result_row:
                        results.append(
                            LocalStoreCDWeekdayTiemAveragePercent(
                                store_business_number=store_info.store_business_number,
                                commercial_district_average_sales_percent_mon=result_row.get(
                                    "AVG_PROFIT_PER_MON", 0.0
                                ),
                                commercial_district_average_sales_percent_tue=result_row.get(
                                    "AVG_PROFIT_PER_TUE", 0.0
                                ),
                                commercial_district_average_sales_percent_wed=result_row.get(
                                    "AVG_PROFIT_PER_WED", 0.0
                                ),
                                commercial_district_average_sales_percent_thu=result_row.get(
                                    "AVG_PROFIT_PER_THU", 0.0
                                ),
                                commercial_district_average_sales_percent_fri=result_row.get(
                                    "AVG_PROFIT_PER_FRI", 0.0
                                ),
                                commercial_district_average_sales_percent_sat=result_row.get(
                                    "AVG_PROFIT_PER_SAT", 0.0
                                ),
                                commercial_district_average_sales_percent_sun=result_row.get(
                                    "AVG_PROFIT_PER_SUN", 0.0
                                ),
                                commercial_district_average_sales_percent_06_09=result_row.get(
                                    "AVG_PROFIT_PER_06_09", 0.0
                                ),
                                commercial_district_average_sales_percent_09_12=result_row.get(
                                    "AVG_PROFIT_PER_09_12", 0.0
                                ),
                                commercial_district_average_sales_percent_12_15=result_row.get(
                                    "AVG_PROFIT_PER_12_15", 0.0
                                ),
                                commercial_district_average_sales_percent_15_18=result_row.get(
                                    "AVG_PROFIT_PER_15_18", 0.0
                                ),
                                commercial_district_average_sales_percent_18_21=result_row.get(
                                    "AVG_PROFIT_PER_18_21", 0.0
                                ),
                                commercial_district_average_sales_percent_21_24=result_row.get(
                                    "AVG_PROFIT_PER_21_24", 0.0
                                ),
                            )
                        )

            return results

    except Exception as e:
        logger.error(f"쿼리 실행 중 오류 발생: {select_query}, Error: {e}")
        logger.error(f"Error fetching LocalStoreCDWeekdayTiemAveragePercent data: {e}")
        raise


######################## 상권 분석 시/군/구에서 매핑된 소분류들 매출합 TOP5 ######################################


def select_commercial_district_district_average_sales_data_batch(
    mappings: List[LocalStoreMappingSubDistrictDetailCategoryId],
) -> List[LocalStoreCDDistrictAverageSalesTop5]:

    logger = logging.getLogger(__name__)
    results = []

    store_mappings: Dict[str, Dict] = defaultdict(
        lambda: {"sub_district_id": None, "detail_categories": set()}
    )

    for mapping in mappings:
        store_data = store_mappings[mapping.store_business_number]
        store_data["sub_district_id"] = mapping.sub_district_id
        store_data["detail_categories"].add(mapping.detail_category_id)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:

                for store_number, store_data in store_mappings.items():
                    detail_categories_str = ", ".join(
                        map(str, store_data["detail_categories"])
                    )

                    query = """
                            WITH AggregatedSales AS (
                                SELECT
                                    SD.SUB_DISTRICT_NAME,
                                    SUM(CD.AVERAGE_SALES) AS TOTAL_SALES
                                FROM
                                    COMMERCIAL_DISTRICT CD
                                JOIN SUB_DISTRICT SD ON SD.SUB_DISTRICT_ID = CD.SUB_DISTRICT_ID
                                WHERE
                                    CD.DISTRICT_ID IN (SELECT DISTRICT_ID FROM SUB_DISTRICT WHERE SUB_DISTRICT_ID = %s)
                                    AND CD.BIZ_DETAIL_CATEGORY_ID IN ({})
                                    AND CD.Y_M = (SELECT MAX(Y_M ) FROM COMMERCIAL_DISTRICT)
                                GROUP BY
                                    SD.SUB_DISTRICT_NAME
                            ),

                            RankedSales AS (
                                SELECT
                                    SUB_DISTRICT_NAME,
                                    TOTAL_SALES,
                                    ROW_NUMBER() OVER (ORDER BY TOTAL_SALES DESC) AS `rank`
                                FROM
                                    AggregatedSales
                            )

                            SELECT
                                SUB_DISTRICT_NAME,
                                TOTAL_SALES
                            FROM
                                RankedSales
                            WHERE
                                `rank` <= 5
                            ORDER BY
                                TOTAL_SALES DESC
                        ;
                    """.format(
                        detail_categories_str
                    )

                    cursor.execute(query, (store_data["sub_district_id"],))
                    scores = cursor.fetchall()

                    # Prepare result formatted as "SUB_DISTRICT_NAME,TOTAL_SALES"
                    top_sales = [
                        f"{row['SUB_DISTRICT_NAME']},{row['TOTAL_SALES']}"
                        for row in scores
                    ]
                    result = LocalStoreCDDistrictAverageSalesTop5(
                        store_business_number=store_number,
                        commercial_districdt_detail_category_average_sales_top1_info=(
                            top_sales[0] if len(top_sales) > 0 else ","
                        ),
                        commercial_districdt_detail_category_average_sales_top2_info=(
                            top_sales[1] if len(top_sales) > 1 else ","
                        ),
                        commercial_districdt_detail_category_average_sales_top3_info=(
                            top_sales[2] if len(top_sales) > 2 else ","
                        ),
                        commercial_districdt_detail_category_average_sales_top4_info=(
                            top_sales[3] if len(top_sales) > 3 else ","
                        ),
                        commercial_districdt_detail_category_average_sales_top5_info=(
                            top_sales[4] if len(top_sales) > 4 else ","
                        ),
                    )
                    results.append(result)

                return results

    except Exception as e:
        logger.error(f"Error processing batch J scores: {e}")
        raise


######################## 뜨는 업종 전국 TOP5, 읍/면/동 TOP3 (시/군/구,읍/면/동,소분류명,증가율) ######################################
def select_commercial_district_top5_top3_data_batch(
    batch: List[LocalStoreSubdistrictId],
) -> List[LocalStoreRisingBusinessNTop5SDTop3]:
    logger = logging.getLogger(__name__)
    results = []

    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            # TOP5 Query - ORDER BY 제거
            select_top5_query = """
                WITH RankedBusiness AS (
                    SELECT 
                        D.DISTRICT_NAME,
                        SD.SUB_DISTRICT_NAME,
                        RB.GROWTH_RATE,
                        BDC.BIZ_DETAIL_CATEGORY_NAME,
                        ROW_NUMBER() OVER (PARTITION BY Y_M ORDER BY GROWTH_RATE DESC) AS NATIONAL_TOP5
                    FROM RISING_BUSINESS RB
                    JOIN DISTRICT D ON D.DISTRICT_ID= RB.DISTRICT_ID
                    JOIN SUB_DISTRICT SD ON SD.SUB_DISTRICT_ID = RB.SUB_DISTRICT_ID
                    JOIN BIZ_DETAIL_CATEGORY BDC ON BDC.BIZ_DETAIL_CATEGORY_ID = RB.BIZ_DETAIL_CATEGORY_ID
                    WHERE GROWTH_RATE < 1000
                    AND Y_M = (SELECT MAX(Y_M) FROM RISING_BUSINESS)
                )
                SELECT
                    DISTRICT_NAME,
                    SUB_DISTRICT_NAME,
                    BIZ_DETAIL_CATEGORY_NAME,
                    GROWTH_RATE    
                FROM RankedBusiness
                WHERE NATIONAL_TOP5 <= 5
            """

            # sub_district_id 리스트 생성
            sub_district_ids = [store_info.sub_district_id for store_info in batch]

            # TOP3 Query - ORDER BY 제거
            select_top3_query = f"""
                WITH RankedBusiness AS (
                    SELECT 
                        D.DISTRICT_NAME,
                        SD.SUB_DISTRICT_NAME,
                        BDC.BIZ_DETAIL_CATEGORY_NAME,
                        RB.GROWTH_RATE,
                        RB.SUB_DISTRICT_ID,
                        ROW_NUMBER() OVER (PARTITION BY RB.SUB_DISTRICT_ID ORDER BY RB.GROWTH_RATE DESC) AS NATIONAL_TOP3
                    FROM RISING_BUSINESS RB
                    JOIN DISTRICT D ON D.DISTRICT_ID = RB.DISTRICT_ID
                    JOIN SUB_DISTRICT SD ON SD.SUB_DISTRICT_ID = RB.SUB_DISTRICT_ID
                    JOIN BIZ_DETAIL_CATEGORY BDC ON BDC.BIZ_DETAIL_CATEGORY_ID = RB.BIZ_DETAIL_CATEGORY_ID
                    WHERE GROWTH_RATE < 1000
                    AND RB.SUB_DISTRICT_ID IN ({', '.join(['%s'] * len(sub_district_ids))})
                    AND Y_M = (SELECT MAX(Y_M) FROM RISING_BUSINESS)
                )
                SELECT
                    DISTRICT_NAME,
                    SUB_DISTRICT_NAME,
                    BIZ_DETAIL_CATEGORY_NAME,
                    GROWTH_RATE,
                    SUB_DISTRICT_ID
                FROM RankedBusiness
                WHERE NATIONAL_TOP3 <= 3
            """

            # Execute TOP5 query
            cursor.execute(select_top5_query)
            top5_rows = cursor.fetchall()

            # Execute TOP3 query
            cursor.execute(select_top3_query, sub_district_ids)
            top3_rows = cursor.fetchall()

            # Process results for each store
            for store_info in batch:
                # Format TOP5 data
                top5_info = []
                for row in top5_rows:
                    info = f"{row['DISTRICT_NAME']},{row['SUB_DISTRICT_NAME']},{row['BIZ_DETAIL_CATEGORY_NAME']},{row['GROWTH_RATE']}"
                    top5_info.append(info)

                # Fill remaining TOP5 slots with empty strings if necessary
                while len(top5_info) < 5:
                    top5_info.append(",,,")

                # Filter and format TOP3 data for current sub_district
                current_store_top3 = []
                for row in top3_rows:
                    if row["SUB_DISTRICT_ID"] == store_info.sub_district_id:
                        info = f"{row['DISTRICT_NAME']},{row['SUB_DISTRICT_NAME']},{row['BIZ_DETAIL_CATEGORY_NAME']},{row['GROWTH_RATE']}"
                        current_store_top3.append(info)

                # Fill remaining TOP3 slots with empty strings if necessary
                while len(current_store_top3) < 3:
                    current_store_top3.append(",,,")

                # Create result object
                result = LocalStoreRisingBusinessNTop5SDTop3(
                    store_business_number=store_info.store_business_number,
                    rising_business_national_rising_sales_top1_info=(
                        top5_info[0] if len(top5_info) > 0 else ",,,"
                    ),
                    rising_business_national_rising_sales_top2_info=(
                        top5_info[1] if len(top5_info) > 1 else ",,,"
                    ),
                    rising_business_national_rising_sales_top3_info=(
                        top5_info[2] if len(top5_info) > 2 else ",,,"
                    ),
                    rising_business_national_rising_sales_top4_info=(
                        top5_info[3] if len(top5_info) > 3 else ",,,"
                    ),
                    rising_business_national_rising_sales_top5_info=(
                        top5_info[4] if len(top5_info) > 4 else ",,,"
                    ),
                    rising_business_sub_district_rising_sales_top1_info=(
                        current_store_top3[0] if len(current_store_top3) > 0 else ",,,"
                    ),
                    rising_business_sub_district_rising_sales_top2_info=(
                        current_store_top3[1] if len(current_store_top3) > 1 else ",,,"
                    ),
                    rising_business_sub_district_rising_sales_top3_info=(
                        current_store_top3[2] if len(current_store_top3) > 2 else ",,,"
                    ),
                )
                # print(result)
                results.append(result)

            return results

    except Exception as e:
        logger.error(f"LocalStoreLocInfoData 가져오는 중 오류 발생: {e}")
        raise


######################## INSERT ######################################
######################## INSERT ######################################
######################## INSERT ######################################
######################## INSERT ######################################
# 매장 기본 정보 넣기
def insert_or_update_store_info_batch(batch: List[LocalStoreBasicInfo]) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
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
                    ;
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
        raise


# 매장 top5 정보 넣기
def insert_or_update_top5_batch(batch: List[LocalStoreTop5Menu]) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
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
                    ;
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
        raise


# 매장 읍/면/동 인구 데이터 넣기
def insert_or_update_population_data_batch(
    batch: List[LocalStorePopulationData],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
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
                    ;
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
        logging.error(f"Error inserting/updating population data: {e}")
        raise


# 매장 읍/면/동 입지 정보 넣기
def insert_or_update_loc_info_data_batch(batch: List[LocalStoreLocInfoData]) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        LOC_INFO_RESIDENT_K, 
                        LOC_INFO_WORK_POP_K,
                        LOC_INFO_MOVE_POP_K, 
                        LOC_INFO_SHOP_K,
                        LOC_INFO_INCOME_WON,
                        LOC_INFO_DATA_REF_DATE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        LOC_INFO_RESIDENT_K = VALUES(LOC_INFO_RESIDENT_K),
                        LOC_INFO_WORK_POP_K = VALUES(LOC_INFO_WORK_POP_K),
                        LOC_INFO_MOVE_POP_K = VALUES(LOC_INFO_MOVE_POP_K),
                        LOC_INFO_SHOP_K = VALUES(LOC_INFO_SHOP_K),
                        LOC_INFO_INCOME_WON = VALUES(LOC_INFO_INCOME_WON),
                        LOC_INFO_DATA_REF_DATE = VALUES(LOC_INFO_DATA_REF_DATE)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.loc_info_resident_k,
                        store_info.loc_info_work_pop_k,
                        store_info.loc_info_move_pop_k,
                        store_info.loc_info_shop_k,
                        store_info.loc_info_income_won,
                        store_info.loc_info_data_ref_date,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating loc info data: {e}")
        raise


# 매장 읍/면/동 입지 정보 J_SCORE 넣기
def insert_or_update_loc_info_j_score_data_batch(
    batch: List[LocalStoreLocInfoData],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        LOC_INFO_RESIDENT_J_SCORE, 
                        LOC_INFO_WORK_POP_J_SCORE,
                        LOC_INFO_MOVE_POP_J_SCORE, 
                        LOC_INFO_SHOP_J_SCORE,
                        LOC_INFO_INCOME_J_SCORE,
                        LOC_INFO_AVERAGE_SPEND_J_SCORE,
                        LOC_INFO_AVERAGE_SALES_J_SCORE,
                        LOC_INFO_HOUSE_J_SCORE,
                        LOC_INFO_MZ_POPULATION_J_SCORE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        LOC_INFO_RESIDENT_J_SCORE = VALUES(LOC_INFO_RESIDENT_J_SCORE),
                        LOC_INFO_WORK_POP_J_SCORE = VALUES(LOC_INFO_WORK_POP_J_SCORE),
                        LOC_INFO_MOVE_POP_J_SCORE = VALUES(LOC_INFO_MOVE_POP_J_SCORE),
                        LOC_INFO_SHOP_J_SCORE = VALUES(LOC_INFO_SHOP_J_SCORE),
                        LOC_INFO_INCOME_J_SCORE = VALUES(LOC_INFO_INCOME_J_SCORE),
                        LOC_INFO_AVERAGE_SPEND_J_SCORE = VALUES(LOC_INFO_AVERAGE_SPEND_J_SCORE),
                        LOC_INFO_AVERAGE_SALES_J_SCORE = VALUES(LOC_INFO_AVERAGE_SALES_J_SCORE),
                        LOC_INFO_HOUSE_J_SCORE = VALUES(LOC_INFO_HOUSE_J_SCORE),
                        LOC_INFO_MZ_POPULATION_J_SCORE = VALUES(LOC_INFO_MZ_POPULATION_J_SCORE)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.loc_info_resident_j_score,
                        store_info.loc_info_work_pop_j_score,
                        store_info.loc_info_move_pop_j_score,
                        store_info.loc_info_shop_j_score,
                        store_info.loc_info_income_j_score,
                        store_info.loc_info_average_spend_j_score,
                        store_info.loc_info_average_sales_j_score,
                        store_info.loc_info_house_j_score,
                        store_info.loc_info_mz_population_j_score,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating loc info j_score data: {e}")
        raise


# 매장 읍/면/동 입지 정보 주거인구, 직장인구
def insert_or_update_loc_info_resident_work_pop_data_batch(
    batch: List[LocalStoreLocInfoData],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        LOC_INFO_RESIDENT, 
                        LOC_INFO_WORK_POP,
                        LOC_INFO_RESIDENT_PERCENT, 
                        LOC_INFO_WORK_POP_PERCENT
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        LOC_INFO_RESIDENT = VALUES(LOC_INFO_RESIDENT),
                        LOC_INFO_WORK_POP = VALUES(LOC_INFO_WORK_POP),
                        LOC_INFO_RESIDENT_PERCENT = VALUES(LOC_INFO_RESIDENT_PERCENT),
                        LOC_INFO_WORK_POP_PERCENT = VALUES(LOC_INFO_WORK_POP_PERCENT)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.loc_info_resident,
                        store_info.loc_info_work_pop,
                        store_info.loc_info_resident_percent,
                        store_info.loc_info_work_pop_percent,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating loc info resident work_pop data: {e}")
        raise


# 매장 읍/면/동 입지 정보 유동인구, 시/도 평균 유동인구
def insert_or_update_loc_info_move_pop_data_batch(
    batch: List[LocalStoreMovePopData],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        LOC_INFO_MOVE_POP, 
                        LOC_INFO_CITY_MOVE_POP
                    ) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        LOC_INFO_MOVE_POP = VALUES(LOC_INFO_MOVE_POP),
                        LOC_INFO_CITY_MOVE_POP = VALUES(LOC_INFO_CITY_MOVE_POP)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.loc_info_move_pop,
                        store_info.loc_info_city_move_pop,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating loc info move_pop data: {e}")
        raise


# 입지분석 읍/면/동 J_Score 가중치 평균 합
def insert_or_update_loc_info_j_score_average_data_batch(
    batch: List[LocalStoreLIJSWeightedAverage],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        LOC_INFO_J_SCORE_AVERAGE
                    ) VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        LOC_INFO_J_SCORE_AVERAGE = VALUES(LOC_INFO_J_SCORE_AVERAGE)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.loc_info_j_score_average,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating LocalStoreLIJSWeightedAverage data: {e}"
        )
        raise


# 상권분석 읍/면/동 소분류 J_Score 가중치 평균 합
def insert_or_update_commercial_district_j_score_weighted_average_data_batch(
    batch: List[LocalStoreCDJSWeightedAverage],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        COMMERCIAL_DISTRICT_J_SCORE_AVERAGE
                    ) VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        COMMERCIAL_DISTRICT_J_SCORE_AVERAGE = VALUES(COMMERCIAL_DISTRICT_J_SCORE_AVERAGE)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.commercial_district_j_score_average,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district top5 top3 data: {e}"
        )
        raise


# 매장 읍/면/동 상권분석 대분류 갯수 넣기
def insert_or_update_commercial_district_main_category_count_data_batch(
    batch: List[LocalStoreLocInfoData],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        COMMERCIAL_DISTRICT_FOOD_BUSINESS_COUNT, 
                        COMMERCIAL_DISTRICT_HEALTHCARE_BUSINESS_COUNT,
                        COMMERCIAL_DISTRICT_EDUCATION_BUSINESS_COUNT, 
                        COMMERCIAL_DISTRICT_ENTERTAINMENT_BUSINESS_COUNT,
                        COMMERCIAL_DISTRICT_LIFESTYLE_BUSINESS_COUNT,
                        COMMERCIAL_DISTRICT_RETAIL_BUSINESS_COUNT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        COMMERCIAL_DISTRICT_FOOD_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_FOOD_BUSINESS_COUNT),
                        COMMERCIAL_DISTRICT_HEALTHCARE_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_HEALTHCARE_BUSINESS_COUNT),
                        COMMERCIAL_DISTRICT_EDUCATION_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_EDUCATION_BUSINESS_COUNT),
                        COMMERCIAL_DISTRICT_ENTERTAINMENT_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_ENTERTAINMENT_BUSINESS_COUNT),
                        COMMERCIAL_DISTRICT_LIFESTYLE_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_LIFESTYLE_BUSINESS_COUNT),
                        COMMERCIAL_DISTRICT_RETAIL_BUSINESS_COUNT = VALUES(COMMERCIAL_DISTRICT_RETAIL_BUSINESS_COUNT)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.commercial_district_food_business_count,
                        store_info.commercial_district_healthcare_business_count,
                        store_info.commercial_district_education_business_count,
                        store_info.commercial_district_entertainment_business_count,
                        store_info.commercial_district_lifestyle_business_count,
                        store_info.commercial_district_retail_business_count,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(f"Error inserting/updating loc info j_score data: {e}")
        raise


# 상권분석 읍/면/동 소분류 J_Score 평균
def insert_or_update_commercial_district_j_score_average_data_batch(
    batch: List[LocalStoreCommercialDistrictJscoreAverage],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        COMMERCIAL_DISTRICT_MARKET_SIZE_J_SCORE, 
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_J_SCORE,
                        COMMERCIAL_DISTRICT_USAGE_COUNT_J_SCORE, 
                        COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_J_SCORE,
                        COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_J_SCORE
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        COMMERCIAL_DISTRICT_MARKET_SIZE_J_SCORE = VALUES(COMMERCIAL_DISTRICT_MARKET_SIZE_J_SCORE),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_J_SCORE = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_J_SCORE),
                        COMMERCIAL_DISTRICT_USAGE_COUNT_J_SCORE = VALUES(COMMERCIAL_DISTRICT_USAGE_COUNT_J_SCORE),
                        COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_J_SCORE = VALUES(COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_J_SCORE),
                        COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_J_SCORE = VALUES(COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_J_SCORE)
                    ;
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.commercial_district_market_size_j_socre,
                        store_info.commercial_district_average_sales_j_socre,
                        store_info.commercial_district_usage_count_j_socre,
                        store_info.commercial_district_sub_district_density_j_socre,
                        store_info.commercial_district_sub_average_payment_j_socre,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district j_score average data: {e}"
        )
        raise


# 매장 상권분석 동별 소분류별 요일,시간대 매출 비중
def insert_or_update_commercial_district_weekday_time_average_sales_data_batch(
    batch: List[LocalStoreCommercialDistrictJscoreAverage],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_MON, 
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_TUE,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_WED, 
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_THU,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_FRI,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SAT,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SUN,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_06_09,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_09_12,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_12_15,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_15_18,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_18_21,
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_21_24
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_MON = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_MON),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_TUE = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_TUE),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_WED = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_WED),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_THU = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_THU),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_FRI = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_FRI),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SAT = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SAT),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SUN = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_SUN),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_06_09 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_06_09),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_09_12 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_09_12),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_12_15 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_12_15),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_15_18 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_15_18),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_18_21 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_18_21),
                        COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_21_24 = VALUES(COMMERCIAL_DISTRICT_AVERAGE_SALES_PERCENT_21_24)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.commercial_district_average_sales_percent_mon,
                        store_info.commercial_district_average_sales_percent_tue,
                        store_info.commercial_district_average_sales_percent_wed,
                        store_info.commercial_district_average_sales_percent_thu,
                        store_info.commercial_district_average_sales_percent_fri,
                        store_info.commercial_district_average_sales_percent_sat,
                        store_info.commercial_district_average_sales_percent_sun,
                        store_info.commercial_district_average_sales_percent_06_09,
                        store_info.commercial_district_average_sales_percent_09_12,
                        store_info.commercial_district_average_sales_percent_12_15,
                        store_info.commercial_district_average_sales_percent_15_18,
                        store_info.commercial_district_average_sales_percent_18_21,
                        store_info.commercial_district_average_sales_percent_21_24,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district weekday time average sales data: {e}"
        )
        raise


# 상권 분석 시/군/구에서 매핑된 소분류들 매출합 TOP5
def insert_or_update_commercial_district_district_average_sales_data_batch(
    batch: List[LocalStoreCDDistrictAverageSalesTop5],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP1_INFO, 
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP2_INFO,
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP3_INFO, 
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP4_INFO,
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP5_INFO
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP1_INFO = VALUES(COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP1_INFO),
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP2_INFO = VALUES(COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP2_INFO),
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP3_INFO = VALUES(COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP3_INFO),
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP4_INFO = VALUES(COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP4_INFO),
                        COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP5_INFO = VALUES(COMMERCIAL_DISTRICT_DETAIL_CATEGORY_AVERAGE_SALES_TOP5_INFO)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.commercial_districdt_detail_category_average_sales_top1_info,
                        store_info.commercial_districdt_detail_category_average_sales_top2_info,
                        store_info.commercial_districdt_detail_category_average_sales_top3_info,
                        store_info.commercial_districdt_detail_category_average_sales_top4_info,
                        store_info.commercial_districdt_detail_category_average_sales_top5_info,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district district average sales data: {e}"
        )
        raise


# 뜨는 업종 전국 TOP5, 읍/면/동 TOP3 (시/군/구,읍/면/동,소분류명,증가율)
def insert_or_update_commercial_district_top5_top3_data_batch(
    batch: List[LocalStoreCDDistrictAverageSalesTop5],
) -> None:
    try:
        with get_service_report_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO REPORT (
                        STORE_BUSINESS_NUMBER,
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP1_INFO, 
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP2_INFO,
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP3_INFO, 
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP4_INFO,
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP5_INFO,
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP1_INFO,
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP2_INFO,
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP3_INFO
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP1_INFO = VALUES(RISING_BUSINESS_NATIONAL_RISING_SALES_TOP1_INFO),
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP2_INFO = VALUES(RISING_BUSINESS_NATIONAL_RISING_SALES_TOP2_INFO),
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP3_INFO = VALUES(RISING_BUSINESS_NATIONAL_RISING_SALES_TOP3_INFO),
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP4_INFO = VALUES(RISING_BUSINESS_NATIONAL_RISING_SALES_TOP4_INFO),
                        RISING_BUSINESS_NATIONAL_RISING_SALES_TOP5_INFO = VALUES(RISING_BUSINESS_NATIONAL_RISING_SALES_TOP5_INFO),
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP1_INFO = VALUES(RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP1_INFO),
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP2_INFO = VALUES(RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP2_INFO),
                        RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP3_INFO = VALUES(RISING_BUSINESS_SUB_DISTRICT_RISING_SALES_TOP3_INFO)
                    ;
                """

                values = [
                    (
                        store_info.store_business_number,
                        store_info.rising_business_national_rising_sales_top1_info,
                        store_info.rising_business_national_rising_sales_top2_info,
                        store_info.rising_business_national_rising_sales_top3_info,
                        store_info.rising_business_national_rising_sales_top4_info,
                        store_info.rising_business_national_rising_sales_top5_info,
                        store_info.rising_business_sub_district_rising_sales_top1_info,
                        store_info.rising_business_sub_district_rising_sales_top2_info,
                        store_info.rising_business_sub_district_rising_sales_top3_info,
                    )
                    for store_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district top5 top3 data: {e}"
        )
        raise
