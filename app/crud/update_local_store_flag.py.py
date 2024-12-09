from app.db.connect import *


def add_local_store_column():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            ALTER TABLE local_store
            ADD COLUMN ktmyshop TINYINT(1) DEFAULT 0 COMMENT 'KT My Shop 여부',
            ADD COLUMN jsam TINYINT(1) DEFAULT 0 COMMENT 'JSAM 여부';
        """
        cursor.execute(query)

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def update_local_store_flag_column():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = """
            ALTER TABLE local_store
            ADD COLUMN ktmyshop TINYINT(1) DEFAULT 0 COMMENT 'KT My Shop 여부',
            ADD COLUMN jsam TINYINT(1) DEFAULT 0 COMMENT 'JSAM 여부';
        """
        cursor.execute(query)

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)
