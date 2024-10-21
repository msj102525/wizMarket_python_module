import logging
from typing import List
import pymysql
from app.db.connect import (
    close_connection,
    close_cursor,
    get_db_connection,
    get_service_report_db_connection,
)
from app.schemas.report import (
    LocalStoreBasicInfo,
    LocalStoreMappingRepId,
    LocalStoreTop5Menu,
    Report,
)


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


##################### 매장마다 소분류별 뜨는 메뉴 TOP5 넣기 ##############################
def select_local_store_rep_id(batch_size: int = 5000) -> List[LocalStoreTop5Menu]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            select_query = """
                SELECT DISTINCT
                    LS.STORE_BUSINESS_NUMBER,
                    DCM.REP_ID
                FROM LOCAL_STORE LS
                JOIN BUSINESS_AREA_CATEGORY BAC ON BAC.DETAIL_CATEGORY_NAME = LS.SMALL_CATEGORY_NAME
                JOIN DETAIL_CATEGORY_MAPPING DCM ON DCM.BUSINESS_AREA_CATEGORY_ID = BAC.BUSINESS_AREA_CATEGORY_ID
                ;
            """

            select_query_top5 = """
                SELECT
                    TOP_MENU_1,
                    TOP_MENU_2,
                    TOP_MENU_3,
                    TOP_MENU_4,
                    TOP_MENU_5,
                    Y_M
                FROM COMMERCIAL_DISTRICT
                WHERE BIZ_DETAIL_CATEGORY_ID = %s
                GROUP BY BIZ_DETAIL_CATEGORY_ID, TOP_MENU_1, TOP_MENU_2, TOP_MENU_3, TOP_MENU_4, TOP_MENU_5, Y_M
                ;
            """

            cursor.execute(select_query)

            results = []
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    store_business_number = row["STORE_BUSINESS_NUMBER"]
                    rep_id = row["REP_ID"]

                    cursor.execute(select_query_top5, (rep_id,))
                    result_top5 = cursor.fetchone()

                    if result_top5:
                        results.append(
                            LocalStoreTop5Menu(
                                store_business_number=store_business_number,
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
        logger.error(f"Error fetching statistics data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


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
