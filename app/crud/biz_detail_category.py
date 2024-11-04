import logging
from typing import List, Optional

import pymysql
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)
from app.schemas.biz_detail_category import (
    BizDetailCategoryId,
    BizDetailCategoryOutput,
)
from app.schemas.category import CategoryListOutput


def get_or_create_biz_detail_category_id(
    biz_sub_category_id: int, biz_detail_category_name: str
) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = """
        SELECT biz_detail_category_id
        FROM biz_detail_category
        WHERE biz_sub_category_id = %s
        AND biz_detail_category_name = %s;
        """
        cursor.execute(
            select_query,
            (biz_sub_category_id, biz_detail_category_name),
        )
        result = cursor.fetchone()

        # logger.info(
        #     f"Executing query: {select_query % (biz_sub_category_id,biz_detail_category_name )}"
        # )

        if result:
            return result[0]
        else:
            insert_query = """
            INSERT INTO biz_detail_category
            (biz_sub_category_id, biz_detail_category_name)
            VALUES (%s, %s);
            """
            cursor.execute(
                insert_query,
                (biz_sub_category_id, biz_detail_category_name),
            )
            commit(connection)

            return cursor.lastrowid
    except Exception as e:
        rollback(connection)
        logger.error(
            "Error in get_or_create_biz_detail_category_id: %s", e, exc_info=True
        )
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_detail_category_name_by_detial_category_id(detail_category_id: int) -> str:
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        select_query = "SELECT biz_detail_category_name FROM biz_detail_category WHERE biz_detail_category_id = %s;"
        cursor.execute(select_query, (detail_category_id,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return ""
    except Exception as e:
        rollback(connection)
        print(f"get_detail_category_name:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_biz_categories_id_by_biz_detail_category_name(biz_detail_category_name: str):
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = """
        SELECT
            bmc.biz_main_category_id,
            bsc.biz_sub_category_id,
            bdc.biz_detail_category_id
        FROM
            biz_detail_category bdc
        JOIN
            biz_sub_category bsc ON bdc.biz_sub_category_id = bsc.biz_sub_category_id
        JOIN
            biz_main_category bmc ON bsc.biz_main_category_id = bmc.biz_main_category_id
        WHERE biz_detail_category_name = %s
        ;

        """
        cursor.execute(
            select_query,
            (biz_detail_category_name,),
        )
        result = cursor.fetchone()

        logger.info(f"Executing query: {select_query % (biz_detail_category_name, )}")

        if result:
            print(f"result: {result}")
            return result
        else:
            raise Exception(f"detail_category 없음: {biz_detail_category_name}")

    except Exception as e:
        rollback(connection)
        logger.error(
            "Error in get_or_create_biz_detail_category_id: %s", e, exc_info=True
        )
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_all_biz_detail_category_by_biz_sub_category_id(
    biz_sub_category_id: int,
) -> List[BizDetailCategoryOutput]:
    connection = get_db_connection()
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            results: List[BizDetailCategoryOutput] = []
            select_query = """
            SELECT BIZ_DETAIL_CATEGORY_ID, BIZ_DETAIL_CATEGORY_NAME
            FROM BIZ_DETAIL_CATEGORY
            WHERE BIZ_SUB_CATEGORY_ID = %s
            ;
            """

            cursor.execute(select_query, (biz_sub_category_id,))
            rows = cursor.fetchall()

            for row in rows:
                if row.get("BIZ_SUB_CATEGORY_ID") != 2:
                    biz_main_category = BizDetailCategoryOutput(
                        biz_detail_category_id=row.get("BIZ_DETAIL_CATEGORY_ID"),
                        biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
                    )
                    results.append(biz_main_category)

            return results
    except Exception as e:
        print(f"get_all_main_category Error: {e}")
        # raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        close_connection(connection)


def get__all_biz_categories_id_like_biz_detail_category_name(
    search_cate: str,
):
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        select_query = """
        SELECT
            bmc.biz_main_category_id,
            bsc.biz_sub_category_id,
            bdc.biz_detail_category_id
        FROM
            biz_detail_category bdc
        JOIN
            biz_sub_category bsc ON bdc.biz_sub_category_id = bsc.biz_sub_category_id
        JOIN
            biz_main_category bmc ON bsc.biz_main_category_id = bmc.biz_main_category_id
        WHERE bdc.biz_detail_category_name LIKE %s
        ;
        """

        like_pattern = f"%{search_cate}%"

        # logger.info(f"Executing query: {select_query} with pattern: {like_pattern}")

        cursor.execute(select_query, (like_pattern,))
        result = cursor.fetchall()

        return result
    except pymysql.MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        # Handle exception as needed
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def get_all_detail_category_count() -> int:
    connection = get_db_connection()
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            select_query = """
            SELECT COUNT(BIZ_DETAIL_CATEGORY_ID) COUNT
            FROM BIZ_DETAIL_CATEGORY
            """
            cursor.execute(select_query)
            result = cursor.fetchone()

            # print(result)
            # print(result["COUNT"])
            return result["COUNT"]
    except Exception as e:
        print(f"get_all_detail_category_count Error: {e}")
        # raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        close_connection(connection)


def select_all_biz_category_by_dynamic_query(
    main_category_id: Optional[str] = None,
    sub_category_id: Optional[int] = None,
    detail_category_id: Optional[int] = None,
) -> List[CategoryListOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CategoryListOutput] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    BDC.BIZ_DETAIL_CATEGORY_ID,
                    BMC.BIZ_MAIN_CATEGORY_NAME,
                    BSC.BIZ_SUB_CATEGORY_NAME,
                    BDC.BIZ_DETAIL_CATEGORY_NAME
                FROM
                    BIZ_DETAIL_CATEGORY BDC
                JOIN
                    BIZ_SUB_CATEGORY BSC ON BDC.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
                JOIN
                    BIZ_MAIN_CATEGORY BMC ON BSC.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
            """

            params = []

            if main_category_id is not None:
                select_query += " AND BSC.BIZ_MAIN_CATEGORY_ID = %s"
                params.append(main_category_id)
            if sub_category_id is not None:
                select_query += " AND BDC.BIZ_SUB_CATEGORY_ID = %s"
                params.append(sub_category_id)
            if detail_category_id is not None:
                select_query += " AND BDC.BIZ_DETAIL_CATEGORY_ID = %s"
                params.append(detail_category_id)

            select_query += " ORDER BY BDC.BIZ_DETAIL_CATEGORY_ID DESC"

            # logger.info(f"Executing query: {select_query % tuple(params)}")
            cursor.execute(select_query, tuple(params))

            rows = cursor.fetchall()

            for row in rows:
                rising_business_ouput = CategoryListOutput(
                    category_id=row.get("BIZ_DETAIL_CATEGORY_ID"),
                    main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
                    sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
                    detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
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


def select_all_biz_detail_category_id() -> BizDetailCategoryId:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    results: List[BizDetailCategoryId] = []
    try:
        if connection.open:
            select_query = """
                SELECT
                    BIZ_DETAIL_CATEGORY_ID
                FROM
                    BIZ_DETAIL_CATEGORY
            """

            # logger.info(f"Executing query: {select_query % tuple(params)}")
            cursor.execute(select_query)

            rows = cursor.fetchall()

            for row in rows:
                results.append(row.get("BIZ_DETAIL_CATEGORY_ID"))

            return results

    except pymysql.MySQLError as e:
        # logger.error(f"MySQL Error: {e}")
        rollback(connection)
    except Exception as e:
        # logger.error(f"Unexpected Error: {e}")
        rollback(connection)
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def select_biz_detail_category_id_by_biz_detail_category_name(
    biz_detail_category_name: str,
) -> BizDetailCategoryId:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: BizDetailCategoryId = 0
    # print(biz_detail_category_name)
    try:
        if connection.open:
            select_query = """
                SELECT
                    BIZ_DETAIL_CATEGORY_ID
                FROM
                    BIZ_DETAIL_CATEGORY
                WHERE BIZ_DETAIL_CATEGORY_NAME = %s
                ;
            """

            # logger.info(
            #     f"Executing query: {select_query} with category name: {biz_detail_category_name}"
            # )

            cursor.execute(select_query, (biz_detail_category_name,))

            results = cursor.fetchone() or 0

            if results is None or results == 0:
                raise Exception(
                    f"No result found for category: {biz_detail_category_name}"
                )
            return results["BIZ_DETAIL_CATEGORY_ID"] if results else 0

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


def select_all_category_id_by_biz_detail_category_id(detail_category_id: int) -> str:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        select_query = """
        SELECT 
            bmc.BIZ_MAIN_CATEGORY_ID,
            bsc.BIZ_SUB_CATEGORY_ID
        FROM
            biz_detail_category bdc
        JOIN biz_sub_category bsc ON bsc.BIZ_SUB_CATEGORY_ID = bdc.BIZ_SUB_CATEGORY_ID
        JOIN biz_main_category bmc ON bmc.BIZ_MAIN_CATEGORY_ID = bsc.BIZ_MAIN_CATEGORY_ID
        WHERE bdc.BIZ_DETAIL_CATEGORY_ID = %s;
        """
        cursor.execute(select_query, (detail_category_id,))
        result = cursor.fetchone()

        # print(result)

        return result
    except Exception as e:
        rollback(connection)
        print(f"Can't get_detail_category_name:{e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


if __name__ == "__main__":
    select_all_category_id_by_biz_detail_category_id(337)
