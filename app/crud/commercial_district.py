from collections import defaultdict
import logging
from typing import Dict, List, Optional, Set
import pymysql
from app.schemas.commercial_district import (
    CommercialDistrictInsert,
    CommercialDistrictOutput,
    CommercialDistrictStatistics,
    CommercialDistrictStatisticsBase,
    CommercialDistrictSubDistrictDetailCategoryId,
    CommercialDistrictWeightedAvgStatistics,
)
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def insert_commercial_district(data: CommercialDistrictInsert):
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        insert_query = """
        INSERT INTO commercial_district (
            city_id, district_id, sub_district_id, 
            biz_main_category_id, biz_sub_category_id, biz_detail_category_id,
            national_density, city_density, district_density, sub_district_density,
            market_size, average_payment, usage_count,
            average_sales, operating_cost, food_cost, employee_cost, rental_cost, tax_cost, 
            family_employee_cost, ceo_cost, etc_cost, average_profit,
            avg_profit_per_mon, avg_profit_per_tue, avg_profit_per_wed, avg_profit_per_thu, avg_profit_per_fri, avg_profit_per_sat, avg_profit_per_sun,
            avg_profit_per_06_09, avg_profit_per_09_12, avg_profit_per_12_15, avg_profit_per_15_18, avg_profit_per_18_21, avg_profit_per_21_24, avg_profit_per_24_06,
            avg_client_per_m_20, avg_client_per_m_30, avg_client_per_m_40, avg_client_per_m_50, avg_client_per_m_60,
            avg_client_per_f_20, avg_client_per_f_30, avg_client_per_f_40, avg_client_per_f_50, avg_client_per_f_60,
            top_menu_1, top_menu_2, top_menu_3, top_menu_4, top_menu_5, y_m
        ) VALUES (
            %(city_id)s, %(district_id)s, %(sub_district_id)s, 
            %(biz_main_category_id)s, %(biz_sub_category_id)s, %(biz_detail_category_id)s,
            %(national_density)s, %(city_density)s, %(district_density)s, %(sub_district_density)s,
            %(market_size)s, %(average_payment)s, %(usage_count)s,
            %(average_sales)s, %(operating_cost)s, %(food_cost)s, %(employee_cost)s, %(rental_cost)s, %(tax_cost)s, 
            %(family_employee_cost)s, %(ceo_cost)s, %(etc_cost)s, %(average_profit)s,
            %(avg_profit_per_mon)s, %(avg_profit_per_tue)s, %(avg_profit_per_wed)s, %(avg_profit_per_thu)s, %(avg_profit_per_fri)s, %(avg_profit_per_sat)s, %(avg_profit_per_sun)s,
            %(avg_profit_per_06_09)s, %(avg_profit_per_09_12)s, %(avg_profit_per_12_15)s, %(avg_profit_per_15_18)s, %(avg_profit_per_18_21)s, %(avg_profit_per_21_24)s, %(avg_profit_per_24_06)s,
            %(avg_client_per_m_20)s, %(avg_client_per_m_30)s, %(avg_client_per_m_40)s, %(avg_client_per_m_50)s, %(avg_client_per_m_60)s,
            %(avg_client_per_f_20)s, %(avg_client_per_f_30)s, %(avg_client_per_f_40)s, %(avg_client_per_f_50)s, %(avg_client_per_f_60)s,
            %(top_menu_1)s, %(top_menu_2)s, %(top_menu_3)s, %(top_menu_4)s, %(top_menu_5)s, %(y_m)s
        );
        """

        cursor.execute(insert_query, data)
        connection.commit()
        # logger.info("Executing query: %s with data: %s", insert_query, data)

    except pymysql.MySQLError as e:
        connection.rollback()
        logger.error(f"Error inserting data: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


# 시장 규모
def select_market_size_has_value() -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                MARKET_SIZE
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                MARKET_SIZE > 0
            AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row["MARKET_SIZE"],
                )
                for row in rows
            ]

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


# 결제 건수
def select_usage_count_has_value() -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                USAGE_COUNT
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                USAGE_COUNT > 0
            AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row["USAGE_COUNT"],
                )
                for row in rows
            ]

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


# 평균 매출
def select_average_sales_has_value() -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                AVERAGE_SALES
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                AVERAGE_SALES > 0
            AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row["AVERAGE_SALES"],
                )
                for row in rows
            ]

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


# 상권 분석 읍/면/동 밀집도
def select_sub_district_density_has_value() -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                SUB_DISTRICT_DENSITY
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                SUB_DISTRICT_DENSITY > 0
            AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row["SUB_DISTRICT_DENSITY"],
                )
                for row in rows
            ]

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


# 상권 분석 평균 결제
def select_average_payment_has_value() -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                AVERAGE_PAYMENT
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                AVERAGE_PAYMENT > 0
            AND Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row["AVERAGE_PAYMENT"],
                )
                for row in rows
            ]

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


def select_column_name_has_value(
    column_name: str,
) -> List[CommercialDistrictStatisticsBase]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictStatisticsBase] = []

    try:
        if connection.open:
            select_query = """
            SELECT
                CITY_ID,
                DISTRICT_ID,
                SUB_DISTRICT_ID,
                BIZ_DETAIL_CATEGORY_ID,
                {column_name}
            FROM
                COMMERCIAL_DISTRICT
            WHERE
                {column_name} > 0
            ORDER BY BIZ_DETAIL_CATEGORY_ID
            ;
            """

            logger.info(f"Executing query: {select_query}")

            cursor.execute(select_query)
            rows = cursor.fetchall()

            # 대문자 키를 소문자로 변경하여 Pydantic 모델과 일치시킴
            results = [
                CommercialDistrictStatisticsBase(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    column_name=row[column_name],
                )
                for row in rows
            ]

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


################################################


# 상권정보 시장규모 통계
def insert_market_size_statistics(stats: List[CommercialDistrictStatistics]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
            INSERT INTO COMMERCIAL_DISTRICT_MARKET_SIZE_STATISTICS
            (CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID, 
            BIZ_SUB_CATEGORY_ID, BIZ_DETAIL_CATEGORY_ID, AVG_VAL, MED_VAL, 
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, J_SCORE, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 데이터를 튜플 리스트로 변환
            values = [
                (
                    stat.city_id,
                    stat.district_id,
                    stat.sub_district_id,
                    stat.biz_main_category_id,
                    stat.biz_sub_category_id,
                    stat.biz_detail_category_id,
                    stat.avg_val,
                    stat.med_val,
                    stat.std_val,
                    stat.max_val,
                    stat.min_val,
                    stat.j_score_rank,
                    stat.j_score_per,
                    stat.j_score,
                    stat.stat_level,
                    stat.ref_date,
                )
                for stat in stats
            ]

            # 여러 레코드 한 번에 삽입
            cursor.executemany(insert_query, values)

            # 변경사항 커밋
            connection.commit()  # commit 함수 바로 호출

            logger.info(f"Successfully inserted {len(stats)} records.")
            return len(stats)

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return 0  # 삽입 실패 시 0 반환


# 상권정보 결제건수 통계
def insert_usage_count_statistics(stats: List[CommercialDistrictStatistics]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
            INSERT INTO COMMERCIAL_DISTRICT_USEAGE_COUNT_STATISTICS
            (CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID, 
            BIZ_SUB_CATEGORY_ID, BIZ_DETAIL_CATEGORY_ID, AVG_VAL, MED_VAL, 
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, J_SCORE, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 데이터를 튜플 리스트로 변환
            values = [
                (
                    stat.city_id,
                    stat.district_id,
                    stat.sub_district_id,
                    stat.biz_main_category_id,
                    stat.biz_sub_category_id,
                    stat.biz_detail_category_id,
                    stat.avg_val,
                    stat.med_val,
                    stat.std_val,
                    stat.max_val,
                    stat.min_val,
                    stat.j_score_rank,
                    stat.j_score_per,
                    stat.j_score,
                    stat.stat_level,
                    stat.ref_date,
                )
                for stat in stats
            ]

            # 여러 레코드 한 번에 삽입
            cursor.executemany(insert_query, values)

            # 변경사항 커밋
            connection.commit()  # commit 함수 바로 호출

            logger.info(f"Successfully inserted {len(stats)} records.")
            return len(stats)

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return 0  # 삽입 실패 시 0 반환


# 상권정보 평균매출 통계
def insert_average_sales_statistics(stats: List[CommercialDistrictStatistics]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
            INSERT INTO COMMERCIAL_DISTRICT_AVERAGE_SALES_STATISTICS
            (CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID, 
            BIZ_SUB_CATEGORY_ID, BIZ_DETAIL_CATEGORY_ID, AVG_VAL, MED_VAL, 
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, J_SCORE, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 데이터를 튜플 리스트로 변환
            values = [
                (
                    stat.city_id,
                    stat.district_id,
                    stat.sub_district_id,
                    stat.biz_main_category_id,
                    stat.biz_sub_category_id,
                    stat.biz_detail_category_id,
                    stat.avg_val,
                    stat.med_val,
                    stat.std_val,
                    stat.max_val,
                    stat.min_val,
                    stat.j_score_rank,
                    stat.j_score_per,
                    stat.j_score,
                    stat.stat_level,
                    stat.ref_date,
                )
                for stat in stats
            ]

            # 여러 레코드 한 번에 삽입
            cursor.executemany(insert_query, values)

            # 변경사항 커밋
            connection.commit()  # commit 함수 바로 호출

            logger.info(f"Successfully inserted {len(stats)} records.")
            return len(stats)

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return 0  # 삽입 실패 시 0 반환


# 상권정보 밀집도 통계
def insert_sub_district_density_statistics(stats: List[CommercialDistrictStatistics]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
            INSERT INTO COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_STATISTICS
            (CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID, 
            BIZ_SUB_CATEGORY_ID, BIZ_DETAIL_CATEGORY_ID, AVG_VAL, MED_VAL, 
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, J_SCORE, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 데이터를 튜플 리스트로 변환
            values = [
                (
                    stat.city_id,
                    stat.district_id,
                    stat.sub_district_id,
                    stat.biz_main_category_id,
                    stat.biz_sub_category_id,
                    stat.biz_detail_category_id,
                    stat.avg_val,
                    stat.med_val,
                    stat.std_val,
                    stat.max_val,
                    stat.min_val,
                    stat.j_score_rank,
                    stat.j_score_per,
                    stat.j_score,
                    stat.stat_level,
                    stat.ref_date,
                )
                for stat in stats
            ]

            # 여러 레코드 한 번에 삽입
            cursor.executemany(insert_query, values)

            # 변경사항 커밋
            connection.commit()  # commit 함수 바로 호출

            logger.info(f"Successfully inserted {len(stats)} records.")
            return len(stats)

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return 0  # 삽입 실패 시 0 반환


# 상권정보 평균결제 통계
def insert_average_payment_statistics(stats: List[CommercialDistrictStatistics]):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
            INSERT INTO COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_STATISTICS
            (CITY_ID, DISTRICT_ID, SUB_DISTRICT_ID, BIZ_MAIN_CATEGORY_ID, 
            BIZ_SUB_CATEGORY_ID, BIZ_DETAIL_CATEGORY_ID, AVG_VAL, MED_VAL, 
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, J_SCORE, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 데이터를 튜플 리스트로 변환
            values = [
                (
                    stat.city_id,
                    stat.district_id,
                    stat.sub_district_id,
                    stat.biz_main_category_id,
                    stat.biz_sub_category_id,
                    stat.biz_detail_category_id,
                    stat.avg_val,
                    stat.med_val,
                    stat.std_val,
                    stat.max_val,
                    stat.min_val,
                    stat.j_score_rank,
                    stat.j_score_per,
                    stat.j_score,
                    stat.stat_level,
                    stat.ref_date,
                )
                for stat in stats
            ]

            # 여러 레코드 한 번에 삽입
            cursor.executemany(insert_query, values)

            # 변경사항 커밋
            connection.commit()  # commit 함수 바로 호출

            logger.info(f"Successfully inserted {len(stats)} records.")
            return len(stats)

    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return 0  # 삽입 실패 시 0 반환


################################################################################################


# 상권분석 j_score 가중치 평균
def select_commercial_district_sub_district_detail_category_ids() -> (
    List[CommercialDistrictSubDistrictDetailCategoryId]
):
    logger = logging.getLogger(__name__)

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                select_query = """
                    SELECT
                        SUB_DISTRICT_ID,
                        BIZ_DETAIL_CATEGORY_ID
                    FROM
                        COMMERCIAL_DISTRICT
                    WHERE Y_M = (SELECT MAX(Y_M) FROM COMMERCIAL_DISTRICT)
                    ;
                """

                cursor.execute(select_query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    if row["BIZ_DETAIL_CATEGORY_ID"] is not None:
                        result.append(
                            CommercialDistrictSubDistrictDetailCategoryId(
                                sub_district_id=row["SUB_DISTRICT_ID"],
                                detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                            )
                        )

                return result

    except Exception as e:
        logger.error(
            f"CommercialDistrictSubDistrictDetailCategoryId 가져오는 중 오류 발생: {e}"
        )
        raise


def select_commercial_district_j_score_weight_average_data(
    mappings: List[CommercialDistrictSubDistrictDetailCategoryId],
) -> List[CommercialDistrictWeightedAvgStatistics]:
    logger = logging.getLogger(__name__)
    results = []

    # sub_district_id와 detail_category_id를 각각 추출하여 중복 제거 후 오름차순 정렬
    sub_district_ids = sorted({mapping.sub_district_id for mapping in mappings})
    detail_category_ids = sorted({mapping.detail_category_id for mapping in mappings})

    # 중복 제거된 ID들을 문자열로 변환
    sub_district_ids_str = ", ".join(map(str, sub_district_ids))
    detail_category_ids_str = ", ".join(map(str, detail_category_ids))

    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:

                query = f"""
                SELECT DISTINCT
                    COALESCE(CDMSS.CITY_ID, CDUCS.CITY_ID, 
                            CDASS.CITY_ID, CDSDDS.CITY_ID, 
                            CDAPS.CITY_ID) AS CITY_ID,
                    COALESCE(CDMSS.DISTRICT_ID, CDUCS.DISTRICT_ID, 
                            CDASS.DISTRICT_ID, CDSDDS.DISTRICT_ID, 
                            CDAPS.DISTRICT_ID) AS DISTRICT_ID,
                    COALESCE(CDMSS.SUB_DISTRICT_ID, CDUCS.SUB_DISTRICT_ID, 
                            CDASS.SUB_DISTRICT_ID, CDSDDS.SUB_DISTRICT_ID, 
                            CDAPS.SUB_DISTRICT_ID) AS SUB_DISTRICT_ID,
                    COALESCE(CDMSS.BIZ_MAIN_CATEGORY_ID, CDUCS.BIZ_MAIN_CATEGORY_ID, 
                            CDASS.BIZ_MAIN_CATEGORY_ID, CDSDDS.BIZ_MAIN_CATEGORY_ID, 
                            CDAPS.BIZ_MAIN_CATEGORY_ID) AS BIZ_MAIN_CATEGORY_ID,
                    COALESCE(CDMSS.BIZ_SUB_CATEGORY_ID, CDUCS.BIZ_SUB_CATEGORY_ID, 
                            CDASS.BIZ_SUB_CATEGORY_ID, CDSDDS.BIZ_SUB_CATEGORY_ID, 
                            CDAPS.BIZ_SUB_CATEGORY_ID) AS BIZ_SUB_CATEGORY_ID,           
                    COALESCE(CDMSS.BIZ_DETAIL_CATEGORY_ID, CDUCS.BIZ_DETAIL_CATEGORY_ID, 
                            CDASS.BIZ_DETAIL_CATEGORY_ID, CDSDDS.BIZ_DETAIL_CATEGORY_ID, 
                            CDAPS.BIZ_DETAIL_CATEGORY_ID) AS BIZ_DETAIL_CATEGORY_ID,
                    CDMSS.J_SCORE_RANK AS MARKET_SIZE_J_RANK,
                    CDUCS.J_SCORE_RANK AS USAGE_COUNT_J_RANK,
                    CDASS.J_SCORE_RANK AS AVERAGE_SALES_J_RANK,
                    CDSDDS.J_SCORE_RANK AS SUB_DISTRICT_DENSITY_J_RANK,
                    CDAPS.J_SCORE_RANK AS AVERAGE_PAYMENT_J_RANK,
                    CDMSS.J_SCORE_PER AS MARKET_SIZE_J_PER,
                    CDUCS.J_SCORE_PER AS USAGE_COUNT_J_PER,
                    CDASS.J_SCORE_PER AS AVERAGE_SALES_J_PER,
                    CDSDDS.J_SCORE_PER AS SUB_DISTRICT_DENSITY_J_PER,
                    CDAPS.J_SCORE_PER AS AVERAGE_PAYMENT_J_PER,
                    COALESCE(CDMSS.REF_DATE, CDUCS.REF_DATE, 
                            CDASS.REF_DATE, CDSDDS.REF_DATE, 
                            CDAPS.REF_DATE) AS REF_DATE
                    FROM
                    SUB_DISTRICT SD
                    LEFT JOIN COMMERCIAL_DISTRICT_MARKET_SIZE_STATISTICS CDMSS 
                    ON CDMSS.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID 
                    AND CDMSS.BIZ_DETAIL_CATEGORY_ID IN ({detail_category_ids_str})
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
                    SD.SUB_DISTRICT_ID IN ({sub_district_ids_str})
                    AND CDMSS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_MARKET_SIZE_STATISTICS)
                    AND CDUCS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_USEAGE_COUNT_STATISTICS)
                    AND CDASS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_AVERAGE_SALES_STATISTICS)
                    AND CDSDDS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_SUB_DISTRICT_DENSITY_STATISTICS)
                    AND CDAPS.REF_DATE = (SELECT MAX(REF_DATE) FROM COMMERCIAL_DISTRICT_AVERAGE_PAYMENT_STATISTICS)
                    ;
                """

                # logger.warning(f"Generated SQL Query: {query}")

                cursor.execute(query)
                rows = cursor.fetchall()

                # print(rows[0])
                # print(rows[1])
                # print(rows[2])
                # print(rows[3])
                # print(rows[46])

            # 서브디스트릭트별로 데이터를 그룹화
            sub_district_data: Dict[int, List[Dict]] = defaultdict(list)
            for row in rows:
                if row["SUB_DISTRICT_ID"] is not None:
                    sub_district_data[row["SUB_DISTRICT_ID"]].append(row)

            # 모든 ROW에 대한 중간 결과를 저장할 리스트
            intermediate_results = []

            # 각 행에 대해 가중치를 적용하고 평균 계산
            for row in rows:
                row_ranks = []
                row_pers = []

                # AVERAGE_SALES에 1.5 가중치 적용
                if row["AVERAGE_SALES_J_RANK"] is not None:
                    row_ranks.append(float(row["AVERAGE_SALES_J_RANK"]) * 1.5)
                if row["AVERAGE_SALES_J_PER"] is not None:
                    row_pers.append(float(row["AVERAGE_SALES_J_PER"]) * 1.5)

                # 나머지 항목들 추가
                rank_keys = [
                    "MARKET_SIZE_J_RANK",
                    "USAGE_COUNT_J_RANK",
                    "SUB_DISTRICT_DENSITY_J_RANK",
                    "AVERAGE_PAYMENT_J_RANK",
                ]
                per_keys = [
                    "MARKET_SIZE_J_PER",
                    "USAGE_COUNT_J_PER",
                    "SUB_DISTRICT_DENSITY_J_PER",
                    "AVERAGE_PAYMENT_J_PER",
                ]

                for key in rank_keys:
                    if row[key] is not None:
                        row_ranks.append(float(row[key]))

                for key in per_keys:
                    if row[key] is not None:
                        row_pers.append(float(row[key]))

                # 평균 계산
                avg_rank = sum(row_ranks) / len(row_ranks) if row_ranks else 0
                avg_per = sum(row_pers) / len(row_pers) if row_pers else 0

                intermediate_results.append(
                    {"row_data": row, "avg_rank": avg_rank, "avg_per": avg_per}
                )

            # 최대값 계산
            max_rank = (
                max(result["avg_rank"] for result in intermediate_results)
                if intermediate_results
                else 1
            )
            max_per = (
                max(result["avg_per"] for result in intermediate_results)
                if intermediate_results
                else 1
            )

            # 최종 결과 생성
            results = []
            for item in intermediate_results:
                row = item["row_data"]
                # 10점 만점으로 정규화
                normalized_rank = (
                    10 * (item["avg_rank"] / max_rank) if max_rank != 0 else 0
                )
                normalized_per = 10 * (item["avg_per"] / max_per) if max_per != 0 else 0

                result = CommercialDistrictWeightedAvgStatistics(
                    city_id=row["CITY_ID"],
                    district_id=row["DISTRICT_ID"],
                    sub_district_id=row["SUB_DISTRICT_ID"],
                    biz_main_category_id=row["BIZ_MAIN_CATEGORY_ID"],
                    biz_sub_category_id=row["BIZ_SUB_CATEGORY_ID"],
                    biz_detail_category_id=row["BIZ_DETAIL_CATEGORY_ID"],
                    j_score_rank=normalized_rank,
                    j_score_per=normalized_per,
                    j_score=(normalized_rank + normalized_per) / 2,
                    stat_level="전국",
                    ref_date=row["REF_DATE"],
                )
                results.append(result)

            return results

    except Exception as e:
        logger.error(
            f"Error processing batch CommercialDistrictWeightedAvgStatistics: {e}"
        )
        raise


def insert_or_update_commercial_district_j_score_weight_average_data_batch(
    batch: List[CommercialDistrictWeightedAvgStatistics],
) -> None:
    try:
        with get_db_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                insert_query = """
                    INSERT INTO COMMERCIAL_DISTRICT_WEIGHTED_AVERAGE (
                        CITY_ID, 
                        DISTRICT_ID,
                        SUB_DISTRICT_ID, 
                        BIZ_MAIN_CATEGORY_ID,
                        BIZ_SUB_CATEGORY_ID,
                        BIZ_DETAIL_CATEGORY_ID,
                        J_SCORE_RANK_AVG,
                        J_SCORE_PER_AVG,
                        J_SCORE_AVG,
                        STAT_LEVEL,
                        REF_DATE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        CITY_ID = VALUES(CITY_ID),
                        DISTRICT_ID = VALUES(DISTRICT_ID),
                        SUB_DISTRICT_ID = VALUES(SUB_DISTRICT_ID),
                        BIZ_MAIN_CATEGORY_ID = VALUES(BIZ_MAIN_CATEGORY_ID),
                        BIZ_SUB_CATEGORY_ID = VALUES(BIZ_SUB_CATEGORY_ID),
                        BIZ_DETAIL_CATEGORY_ID = VALUES(BIZ_DETAIL_CATEGORY_ID),
                        J_SCORE_RANK_AVG = VALUES(J_SCORE_RANK_AVG),
                        J_SCORE_PER_AVG = VALUES(J_SCORE_PER_AVG),
                        J_SCORE_AVG = VALUES(J_SCORE_AVG),
                        STAT_LEVEL = VALUES(STAT_LEVEL),
                        REF_DATE = VALUES(REF_DATE)
                    ;
                """

                values = [
                    (
                        cd_info.city_id,
                        cd_info.district_id,
                        cd_info.sub_district_id,
                        cd_info.biz_main_category_id,
                        cd_info.biz_sub_category_id,
                        cd_info.biz_detail_category_id,
                        cd_info.j_score_rank,
                        cd_info.j_score_per,
                        cd_info.j_score,
                        cd_info.stat_level,
                        cd_info.ref_date,
                    )
                    for cd_info in batch
                ]

                cursor.executemany(insert_query, values)
                connection.commit()

    except Exception as e:
        logging.error(
            f"Error inserting/updating commercial_district CommercialDistrictWeightedAvgStatistics data: {e}"
        )
        raise
