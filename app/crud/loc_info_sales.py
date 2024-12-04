import pymysql
from app.db.connect import get_db_connection, close_connection, close_cursor, get_re_db_connection



def select_all_factor():
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                SALES,
                SHOP,
                MOVE_POP,
                WORK_POP,
                INCOME,
                SPEND,
                HOUSE,
                RESIDENT
            FROM 
                LOC_INFO
            WHERE
                Y_M = '2024-08-01'
        """

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()

        return rows

    finally:
        close_cursor(cursor)
        close_connection(connection)
