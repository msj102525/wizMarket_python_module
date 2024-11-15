import pymysql
import os
from dotenv import load_dotenv
from pymysql import OperationalError, InternalError, ProgrammingError, Error

load_dotenv()

# Source DB
def get_db_connection():
    connection = None
    try:
        connection = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_DATABASE"),
            autocommit=False,
        )
        # print("TEST Database connection established successfully.")
    except OperationalError as e:
        print(f"OperationalError: {e}")
    except InternalError as e:
        print(f"InternalError: {e}")
    except ProgrammingError as e:
        print(f"ProgrammingError: {e}")
    except Error as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return connection

# 리포트 DB 연결
def get_re_db_connection():
    connection = None
    try:
        connection = pymysql.connect(
            host=os.getenv("DB_RE_HOST"),
            user=os.getenv("DB_RE_USER"),
            password=os.getenv("DB_RE_PASSWORD"),
            database=os.getenv("DB_RE_DATABASE"),
            autocommit=False,
        )
        # print("TEST Database connection established successfully.")
    except OperationalError as e:
        print(f"OperationalError: {e}")
    except InternalError as e:
        print(f"InternalError: {e}")
    except ProgrammingError as e:
        print(f"ProgrammingError: {e}")
    except Error as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return connection

def get_report_db_connection(is_dev=False):
    connection = None
    try:
        # 개발 모드 또는 배포 모드에 따라 다른 환경 변수 사용
        if is_dev:
            host = os.getenv("REPORT_DB_HOST_DEV")
            user = os.getenv("REPORT_DB_USER_DEV")
            password = os.getenv("REPORT_DB_PASSWORD_DEV")
            database = os.getenv("REPORT_DB_DATABASE_DEV")
        else:
            host = os.getenv("REPORT_DB_HOST_DEP")
            user = os.getenv("REPORT_DB_USER_DEP")
            password = os.getenv("REPORT_DB_PASSWORD_DEP")
            database = os.getenv("REPORT_DB_DATABASE_DEP")

        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )
        # print("ReportDB Database connection established successfully.")
    except OperationalError as e:
        print(f"OperationalError: {e}")
    except InternalError as e:
        print(f"InternalError: {e}")
    except ProgrammingError as e:
        print(f"ProgrammingError: {e}")
    except Error as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return connection


def get_service_report_db_connection(is_dev=True):
    connection = None
    try:
        # 개발 모드 또는 배포 모드에 따라 다른 환경 변수 사용
        if is_dev:
            host = os.getenv("SERVICE_REPORT_DB_HOST_DEV")
            user = os.getenv("SERVICE_REPORT_DB_USER_DEV")
            password = os.getenv("SERVICE_REPORT_DB_PASSWORD_DEV")
            database = os.getenv("SERVICE_REPORT_DB_DATABASE_DEV")
        else:
            # host = os.getenv("REPORT_DB_HOST_DEP")
            # user = os.getenv("REPORT_DB_USER_DEP")
            # password = os.getenv("REPORT_DB_PASSWORD_DEP")
            # database = os.getenv("REPORT_DB_DATABASE_DEP")
            pass

        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )
        # print("Service Report Database connection established successfully.")
    except OperationalError as e:
        print(f"OperationalError: {e}")
    except InternalError as e:
        print(f"InternalError: {e}")
    except ProgrammingError as e:
        print(f"ProgrammingError: {e}")
    except Error as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return connection


# DB 연결 종료
def close_connection(connection):
    try:
        if connection:
            connection.close()
            # print("Database connection closed successfully.")
    except pymysql.MySQLError as e:
        print(f"Error closing connection: {e}")


# 커서 종료
def close_cursor(cursor):
    try:
        if cursor is not None:
            cursor.close()
            # print("Cursor closed successfully.")
    except pymysql.MySQLError as e:
        print(f"Error closing cursor: {e}")


# 커밋
def commit(connection):
    try:
        if connection:
            connection.commit()
            # print("Transaction committed successfully.")
    except pymysql.MySQLError as e:
        print(f"Error committing transaction: {e}")


# 롤백
def rollback(connection):
    try:
        if connection:
            connection.rollback()
            print("Transaction rolled back successfully.")
    except pymysql.MySQLError as e:
        print(f"Error rolling back transaction: {e}")
