import logging
from typing import List, Optional
import pymysql
from app.schemas.commercial_district import (
    CommercialDistrictInsert,
    CommercialDistrictOutput,
    CommercialDistrictStatistics,
    CommercialDistrictStatisticsBase,
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
            top_menu_1, top_menu_2, top_menu_3, top_menu_4, top_menu_5
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
            %(top_menu_1)s, %(top_menu_2)s, %(top_menu_3)s, %(top_menu_4)s, %(top_menu_5)s
        );
        """

        cursor.execute(insert_query, data)
        commit(connection)
        logger.info("Executing query: %s with data: %s", insert_query, data)

    except pymysql.MySQLError as e:
        rollback(connection)
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
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            STD_VAL, MAX_VAL, MIN_VAL, J_SCORE_RANK, J_SCORE_PER, STAT_LEVEL, REF_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
