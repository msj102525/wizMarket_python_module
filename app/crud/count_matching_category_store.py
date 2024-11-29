from app.db.connect import *

# 소분류별 매장 수
def how_to_count():
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        query = """
            SELECT SMALL_CATEGORY_NAME, COUNT(*) AS category_count
            FROM local_store
            WHERE IS_EXIST = 1
            GROUP BY SMALL_CATEGORY_NAME
            ORDER BY SMALL_CATEGORY_NAME;
        """
        cursor.execute(query)
        all_region_list = cursor.fetchall()

        return all_region_list

    finally:
        close_connection(connection)
   