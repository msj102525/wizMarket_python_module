import logging
from pymysql import MySQLError
from typing import List, Optional

import pymysql
from app.schemas.rising_business import (
    RisingBusinessInsert,
    RisingBusinessOutput,
)
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def insert_rising_business(data_list: List[RisingBusinessInsert]):
    connection = get_db_connection()
    cursor = None
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO RISING_BUSINESS (city_id, district_id, sub_district_id, biz_main_category_id, biz_sub_category_id, biz_detail_category_id, growth_rate, sub_district_rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """

            for data in data_list:
                values = (
                    data.city_id,
                    data.district_id,
                    data.sub_district_id,
                    data.biz_main_category_id,
                    data.biz_sub_category_id,
                    data.biz_detail_category_id,
                    data.growth_rate if data.growth_rate is not None else 0.0,
                    data.sub_district_rank if data.sub_district_rank is not None else 0,
                )
                logger.info(f"Executing query: {insert_query % values}")
                cursor.execute(insert_query, values)
                print(f"Data inserted for {values}")

            commit(connection)

    except MySQLError as e:
        print(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        print(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def select_all_rising_business_by_region_id(
    city_id: int, district_id: int, sub_district_id: int
) -> List[RisingBusinessOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[RisingBusinessOutput] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    rb.RISING_BUSINESS_ID,
                    CI.CITY_NAME,
                    DI.DISTRICT_NAME,
                    SD.SUB_DISTRICT_NAME,
                    BMC.BIZ_MAIN_CATEGORY_NAME,
                    BSC.BIZ_SUB_CATEGORY_NAME,
                    BDC.BIZ_DETAIL_CATEGORY_NAME,
                    rb.growth_rate,
                    rb.sub_district_rank,
                    rb.Y_M,
                    rb.CREATED_AT,
                    rb.UPDATED_AT
                FROM
                    rising_business rb
                JOIN
                    CITY CI ON rb.CITY_ID = CI.CITY_ID
                JOIN
                    DISTRICT DI ON rb.DISTRICT_ID = DI.DISTRICT_ID
                JOIN
                    SUB_DISTRICT SD ON rb.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
                JOIN
                    BIZ_MAIN_CATEGORY BMC ON rb.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
                JOIN
                    BIZ_SUB_CATEGORY BSC ON rb.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
                JOIN
                    BIZ_DETAIL_CATEGORY BDC ON rb.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
                WHERE rb.city_id = %s AND rb.DISTRICT_ID = %s AND rb.SUB_DISTRICT_ID = %s
                ORDER BY rb.RISING_BUSINESS_ID DESC;
            """
            # logger.info(
            #     f"Executing query: {select_query % (city_id, district_id, sub_district_id)}"
            # )
            cursor.execute(select_query, (city_id, district_id, sub_district_id))

            rows = cursor.fetchall()

            for row in rows:
                rising_business_ouput = RisingBusinessOutput(
                    rising_business_id=row.get("RISING_BUSINESS_ID"),
                    city_name=row.get("CITY_NAME"),
                    district_name=row.get("DISTRICT_NAME"),
                    sub_district_name=row.get("SUB_DISTRICT_NAME"),
                    biz_main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
                    biz_sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
                    biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
                    growth_rate=(
                        row.get("growth_rate")
                        if row.get("growth_rate") is not None
                        else 0.0
                    ),
                    sub_district_rank=(
                        row.get("sub_district_rank")
                        if row.get("sub_district_rank") is not None
                        else 0
                    ),
                    y_m=row.get("Y_M"),
                    created_at=row.get("CREATED_AT"),
                    updated_at=row.get("UPDATED_AT"),
                )
                results.append(rising_business_ouput)
            # print(f"뜨는업종 결과: {results}")
            return results
    except MySQLError as e:
        print(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        print(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)

    return results


def select_all_rising_business_by_dynamic_query(
    city_id: Optional[int] = None,
    district_id: Optional[int] = None,
    sub_district_id: Optional[int] = None,
    biz_main_category_id: Optional[int] = None,
    biz_sub_category_id: Optional[int] = None,
    biz_detail_category_id: Optional[int] = None,
    growth_rate_min: Optional[float] = None,
    growth_rate_max: Optional[float] = None,
    rank_min: Optional[int] = None,
    rank_max: Optional[int] = None,
) -> List[RisingBusinessOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[RisingBusinessOutput] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    rb.RISING_BUSINESS_ID,
                    CI.CITY_NAME,
                    DI.DISTRICT_NAME,
                    SD.SUB_DISTRICT_NAME,
                    BMC.BIZ_MAIN_CATEGORY_NAME,
                    BSC.BIZ_SUB_CATEGORY_NAME,
                    BDC.BIZ_DETAIL_CATEGORY_NAME,
                    rb.growth_rate,
                    rb.sub_district_rank,
                    rb.y_m,
                    rb.CREATED_AT,
                    rb.UPDATED_AT
                FROM
                    rising_business rb
                JOIN
                    CITY CI ON rb.CITY_ID = CI.CITY_ID
                JOIN
                    DISTRICT DI ON rb.DISTRICT_ID = DI.DISTRICT_ID
                JOIN
                    SUB_DISTRICT SD ON rb.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
                JOIN
                    BIZ_MAIN_CATEGORY BMC ON rb.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
                JOIN
                    BIZ_SUB_CATEGORY BSC ON rb.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
                JOIN
                    BIZ_DETAIL_CATEGORY BDC ON rb.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
                WHERE 1=1 AND BDC.BIZ_DETAIL_CATEGORY_NAME != '소분류없음'
            """

            # 조건을 동적으로 추가
            params = []
            if city_id is not None:
                select_query += " AND rb.CITY_ID = %s"
                params.append(city_id)
            if district_id is not None:
                select_query += " AND rb.DISTRICT_ID = %s"
                params.append(district_id)
            if sub_district_id is not None:
                select_query += " AND rb.SUB_DISTRICT_ID = %s"
                params.append(sub_district_id)
            if biz_main_category_id is not None:
                select_query += " AND rb.BIZ_MAIN_CATEGORY_ID = %s"
                params.append(biz_main_category_id)
            if biz_sub_category_id is not None:
                select_query += " AND rb.BIZ_SUB_CATEGORY_ID = %s"
                params.append(biz_sub_category_id)
            if biz_detail_category_id is not None:
                select_query += " AND rb.BIZ_DETAIL_CATEGORY_ID = %s"
                params.append(biz_detail_category_id)
            if growth_rate_min is not None:
                select_query += " AND rb.growth_rate >= %s"
                params.append(growth_rate_min)
            if growth_rate_max is not None:
                select_query += " AND rb.growth_rate <= %s"
                params.append(growth_rate_max)
            if rank_min is not None:
                select_query += " AND rb.sub_district_rank >= %s"
                params.append(rank_min)
            if rank_max is not None:
                select_query += " AND rb.sub_district_rank <= %s"
                params.append(rank_max)

            select_query += " ORDER BY rb.RISING_BUSINESS_ID DESC"

            # logger.info(f"Executing query: {select_query % tuple(params)}")
            cursor.execute(select_query, tuple(params))

            rows = cursor.fetchall()

            for row in rows:
                rising_business_ouput = RisingBusinessOutput(
                    rising_business_id=row.get("RISING_BUSINESS_ID"),
                    city_name=row.get("CITY_NAME"),
                    district_name=row.get("DISTRICT_NAME"),
                    sub_district_name=row.get("SUB_DISTRICT_NAME"),
                    biz_main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
                    biz_sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
                    biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
                    growth_rate=(
                        row.get("growth_rate")
                        if row.get("growth_rate") is not None
                        else 0.0
                    ),
                    sub_district_rank=(
                        row.get("sub_district_rank")
                        if row.get("sub_district_rank") is not None
                        else 0
                    ),
                    y_m=row.get("y_m"),
                    created_at=row.get("CREATED_AT"),
                    updated_at=row.get("UPDATED_AT"),
                )
                results.append(rising_business_ouput)

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


# 전국 top5
def select_top5_rising_business() -> List[RisingBusinessOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[RisingBusinessOutput] = []

    try:
        if connection.open:
            select_query = """
                SELECT 
                    rb.RISING_BUSINESS_ID,
                    CI.CITY_NAME,
                    DI.DISTRICT_NAME,
                    SD.SUB_DISTRICT_NAME,
                    BMC.BIZ_MAIN_CATEGORY_NAME,
                    BSC.BIZ_SUB_CATEGORY_NAME,
                    BDC.BIZ_DETAIL_CATEGORY_NAME,
                    rb.growth_rate,
                    rb.sub_district_rank,
                    rb.Y_M,
                    rb.CREATED_AT,
                    rb.UPDATED_AT
                FROM
                    rising_business rb
                        JOIN
                    CITY CI ON rb.CITY_ID = CI.CITY_ID
                        JOIN
                    DISTRICT DI ON rb.DISTRICT_ID = DI.DISTRICT_ID
                        JOIN
                    SUB_DISTRICT SD ON rb.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
                        JOIN
                    BIZ_MAIN_CATEGORY BMC ON rb.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
                        JOIN
                    BIZ_SUB_CATEGORY BSC ON rb.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
                        JOIN
                    BIZ_DETAIL_CATEGORY BDC ON rb.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
                WHERE
                    rb.growth_rate <= 1000
                -- AND 
                --    YEAR(rb.Y_M) = YEAR(CURDATE() - INTERVAL 1 MONTH)
                -- AND 
                --    MONTH(rb.Y_M) = MONTH(CURDATE() - INTERVAL 1 MONTH) 
                ORDER BY 
                    rb.GROWTH_RATE DESC
                LIMIT 5;
                ;
            """

            # logger.info(f"Executing query: {select_query % tuple(params)}")
            cursor.execute(select_query)

            rows = cursor.fetchall()

            for row in rows:
                rising_business_ouput = RisingBusinessOutput(
                    rising_business_id=row.get("RISING_BUSINESS_ID"),
                    city_name=row.get("CITY_NAME"),
                    district_name=row.get("DISTRICT_NAME"),
                    sub_district_name=row.get("SUB_DISTRICT_NAME"),
                    biz_main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
                    biz_sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
                    biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
                    growth_rate=(
                        row.get("growth_rate")
                        if row.get("growth_rate") is not None
                        else 0.0
                    ),
                    sub_district_rank=(
                        row.get("sub_district_rank")
                        if row.get("sub_district_rank") is not None
                        else 0
                    ),
                    y_m=row.get("Y_M"),
                    created_at=row.get("CREATED_AT"),
                    updated_at=row.get("UPDATED_AT"),
                )
                results.append(rising_business_ouput)

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


# 동기준 top3
def select_top3_rising_business_by_store_business_number(
    sub_district_id: int,
) -> List[RisingBusinessOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[RisingBusinessOutput] = []

    # print(f"top3: sub_district_id: {sub_district_id}")

    try:
        if connection.open:

            select_query = """
                SELECT 
                    rb.RISING_BUSINESS_ID,
                    CI.CITY_NAME,
                    DI.DISTRICT_NAME,
                    SD.SUB_DISTRICT_NAME,
                    BMC.BIZ_MAIN_CATEGORY_NAME,
                    BSC.BIZ_SUB_CATEGORY_NAME,
                    BDC.BIZ_DETAIL_CATEGORY_NAME,
                    rb.growth_rate,
                    rb.sub_district_rank,
                    rb.Y_M,
                    rb.CREATED_AT,
                    rb.UPDATED_AT
                FROM
                    rising_business rb
                        JOIN
                    CITY CI ON rb.CITY_ID = CI.CITY_ID
                        JOIN
                    DISTRICT DI ON rb.DISTRICT_ID = DI.DISTRICT_ID
                        JOIN
                    SUB_DISTRICT SD ON rb.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
                        JOIN
                    BIZ_MAIN_CATEGORY BMC ON rb.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
                        JOIN
                    BIZ_SUB_CATEGORY BSC ON rb.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
                        JOIN
                    BIZ_DETAIL_CATEGORY BDC ON rb.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
                WHERE
                    rb.sub_district_id = %s
                -- AND 
                    -- YEAR(rb.Y_M) = YEAR(CURDATE() - INTERVAL 1 MONTH)
                -- AND 
                    -- MONTH(rb.Y_M) = MONTH(CURDATE() - INTERVAL 1 MONTH) 
                ORDER BY 
                    rb.GROWTH_RATE DESC
                LIMIT 3;
                ;
            """

            # logger.info(f"Executing query: {select_query % tuple(sub_district_id,)}")
            cursor.execute(select_query, (sub_district_id,))

            rows = cursor.fetchall()

            for row in rows:
                rising_business_ouput = RisingBusinessOutput(
                    rising_business_id=row.get("RISING_BUSINESS_ID"),
                    city_name=row.get("CITY_NAME"),
                    district_name=row.get("DISTRICT_NAME"),
                    sub_district_name=row.get("SUB_DISTRICT_NAME"),
                    biz_main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
                    biz_sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
                    biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
                    growth_rate=(
                        row.get("growth_rate")
                        if row.get("growth_rate") is not None
                        else 0.0
                    ),
                    sub_district_rank=(
                        row.get("sub_district_rank")
                        if row.get("sub_district_rank") is not None
                        else 0
                    ),
                    y_m=row.get("Y_M"),
                    created_at=row.get("CREATED_AT"),
                    updated_at=row.get("UPDATED_AT"),
                )
                results.append(rising_business_ouput)

            return results
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        logger.error(
            f"Unexpected Error select_top3_rising_business_by_store_business_number: {e}"
        )
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)

    return results
