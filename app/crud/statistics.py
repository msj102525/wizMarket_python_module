import logging
from typing import Dict, List, Optional
import pymysql
from app.db.connect import get_db_connection, close_connection, close_cursor
from app.schemas.commercial_district import CommercialStatisticsData
from app.schemas.statistics import CommercialStatistics, StatisticsJscoreOutput


################# 전국 범위 동별 j_score 가중치 적용 평균 구하기 #############
def get_weighted_jscore(sub_district_id):
    # DB 연결 설정
    connection = get_db_connection()
    cursor = None

    try:
        # 쿼리 실행
        query_statistics = """
            SELECT 
                   city.city_name AS city_name, 
                   district.district_name AS district_name, 
                   sub_district.sub_district_name AS sub_district_name,
                   sub_district.sub_district_id AS sub_district_id,
                   stat_item.table_name AS table_name,
                   stat_item.column_name AS column_name,
                   J_SCORE
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            LEFT JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            JOIN stat_item ON statistics.STAT_ITEM_ID = stat_item.STAT_ITEM_ID
            WHERE statistics.sub_district_id = %s
            AND stat_item.STAT_ITEM_ID BETWEEN 1 AND 8
        """
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query_statistics, (sub_district_id,))
        statistics_result = cursor.fetchall()

        return statistics_result  # 딕셔너리가 아닌 쿼리 결과를 바로 반환

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료


################## stat_item id 조회 ##################
def select_state_item_id(table_name: str, column_name: str) -> int:

    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    # print(table_name, column_name)

    try:
        select_query = """
            SELECT
                STAT_ITEM_ID
            FROM
                STAT_ITEM
            WHERE
                TABLE_NAME = %s
            AND
                COLUMN_NAME = %s
            ;
        """

        cursor.execute(select_query, (table_name, column_name))
        row = cursor.fetchone()

        return row["STAT_ITEM_ID"]

    finally:
        if cursor:
            cursor.close()
        connection.close()


############### 값 조회 ######################
# 전국 J-Score 값 조회
def get_stat_data(filters_dict):
    # print(filters_dict)

    # 여기서 직접 DB 연결을 설정
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                   city.city_name AS city_name, 
                   district.district_name AS district_name, 
                   sub_district.sub_district_name AS sub_district_name,
                   statistics.sub_district_id,
                   stat_item.column_name as column_name,
                   AVG_VAL, MED_VAL, STD_VAL, MAX_VALUE, MIN_VALUE, J_SCORE
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            LEFT JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            JOIN stat_item ON statistics.STAT_ITEM_ID = stat_item.STAT_ITEM_ID
            WHERE 1=1
        """
        query_params = []

        # 필터 값이 존재할 때만 쿼리에 조건 추가
        if filters_dict.get("city") is not None:
            query += " AND statistics.city_id = %s"
            query_params.append(filters_dict["city"])

        if filters_dict.get("district") is not None:
            query += " AND statistics.district_id = %s"
            query_params.append(filters_dict["district"])

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, query_params)
        result = cursor.fetchall()

        return result

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료

# 시/도 내의 동 끼리 비교 J-Score 값 조회
def get_stat_data_by_city(filters_dict):
    # print(filters_dict)

    # 여기서 직접 DB 연결을 설정
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                   city.city_name AS city_name, 
                   district.district_name AS district_name, 
                   sub_district.sub_district_name AS sub_district_name,
                   statistics.sub_district_id,
                   stat_item.column_name as column_name,
                   AVG_VAL, MED_VAL, STD_VAL, MAX_VALUE, MIN_VALUE, J_SCORE
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            LEFT JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            JOIN stat_item ON statistics.STAT_ITEM_ID = stat_item.STAT_ITEM_ID
            WHERE 1=1
        """
        query_params = []

        # 필터 값이 존재할 때만 쿼리에 조건 추가
        if filters_dict.get("city") is not None:
            query += " AND statistics.city_id = %s"
            query_params.append(filters_dict["city"])

        if filters_dict.get("district") is not None:
            query += " AND statistics.district_id = %s"
            query_params.append(filters_dict["district"])

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, query_params)
        result = cursor.fetchall()

        return result

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료
    

# 시/군/구 내의 동 끼리 비교 J-Score 값 조회
def get_stat_data_by_distirct(filters_dict):
    # print(filters_dict)

    # 여기서 직접 DB 연결을 설정
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                   city.city_name AS city_name, 
                   district.district_name AS district_name, 
                   sub_district.sub_district_name AS sub_district_name,
                   statistics.sub_district_id,
                   stat_item.column_name as column_name,
                   AVG_VAL, MED_VAL, STD_VAL, MAX_VALUE, MIN_VALUE, J_SCORE
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            LEFT JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            JOIN stat_item ON statistics.STAT_ITEM_ID = stat_item.STAT_ITEM_ID
            WHERE 1=1
        """
        query_params = []

        # 필터 값이 존재할 때만 쿼리에 조건 추가
        if filters_dict.get("city") is not None:
            query += " AND statistics.city_id = %s"
            query_params.append(filters_dict["city"])

        if filters_dict.get("district") is not None:
            query += " AND statistics.district_id = %s"
            query_params.append(filters_dict["district"])

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, query_params)
        result = cursor.fetchall()

        return result

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료


########## 모든 city_id 값 가져오기 ###################
def get_all_city_ids():
    """
    모든 city_id를 가져오는 함수
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        query = """
            SELECT DISTINCT city_id
            FROM city
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # city_id 목록을 반환
        return [row["city_id"] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############ 전국 단위 모든 city_id, district_id ##############
def get_all_city_district_pairs():
    """
    모든 city_id와 district_id 쌍을 가져옴
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 모든 city_id와 district_id 쌍을 가져오는 쿼리
        query = """
            SELECT DISTINCT city_id, district_id
            FROM district
        """

        cursor.execute(query)
        result = cursor.fetchall()

        # city_id, district_id 쌍을 반환
        return [(row["city_id"], row["district_id"]) for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############ 전국 단위 모든 city_id, district_id, sub_district_id 값 ##############


def get_all_city_district_sub_district():
    """
    모든 city_id와 district_id, sub_district_id 쌍을 가져옴
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 모든 city_id와 district_id 쌍을 가져오는 쿼리
        query = """
            SELECT DISTINCT city_id, district_id, sub_district_id
            FROM sub_district
        """

        cursor.execute(query)
        result = cursor.fetchall()

        # city_id, district_id 쌍을 반환
        return [
            (row["city_id"], row["district_id"], row["sub_district_id"])
            for row in result
        ]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


########### stat_item_id 값 가져오기 ##############
def get_stat_item_id():

    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        query = """
            SELECT DISTINCT(stat_item_id)
            FROM statistics;
        """
        cursor.execute(query)
        stat_item_id_list = cursor.fetchall()

        # city_id 목록을 반환
        return stat_item_id_list

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


###########


def get_data_for_city_and_district(city_id, district_id):
    """
    특정 city_id와 district_id에 대한 컬럼 수(count)를 가져옴
    여러 sub_district의 매장 수를 합산하여 반환
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # city_id와 district_id에 해당하는 모든 sub_district_id의 매장 수를 가져오는 쿼리
        query = """
            SELECT SUM(resident) AS total_count
            FROM loc_info
            WHERE city_id = %s AND district_id = %s
        """

        cursor.execute(query, (city_id, district_id))
        result = cursor.fetchone()

        # 매장 수가 존재하지 않으면 0으로 처리

        total_count = result["total_count"] if result["total_count"] is not None else 0

        return total_count

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


########### 전국 단위 매장 데이터를 가져오는 함수 ###############


def get_national_data():
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 전국 데이터를 가져오는 쿼리
        query = """
            SELECT resident
            FROM loc_info
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # 데이터만 리스트로 반환
        return [row[0] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


def get_national_data_by_detail_category(column_name, table_name, detail_category_id):
    connection = None
    cursor = None
    logger = logging.getLogger(__name__)
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        print(column_name)
        print(table_name)
        print(detail_category_id)

        # 전국 데이터를 가져오는 쿼리
        select_query = f"""
            SELECT {column_name}
            FROM {table_name}
            WHERE BIZ_DETAIL_CATEGORY_ID = %s;
        """
        cursor.execute(select_query, (detail_category_id,))
        logger.info(f"Executing query: {select_query % (detail_category_id)}")
        result = cursor.fetchall()

        print(result)

        # 데이터만 리스트로 반환
        return [row[column_name] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############## 특정 시/도 데이터를 가져오는 함수 ################
def get_city_data(city_id):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 특정 시/군/구의 매장 데이터를 가져오는 쿼리
        query = """
            SELECT resident
            FROM loc_info
            WHERE city_id = %s
        """
        cursor.execute(query, (city_id))
        result = cursor.fetchall()

        # 데이터만 리스트로 반환
        return [row[0] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############## 특정 시/군/구의 매장 데이터를 가져오는 함수 ################
def get_city_district_data(city_id, district_id):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 특정 시/군/구의 매장 데이터를 가져오는 쿼리
        query = """
            SELECT resident
            FROM loc_info
            WHERE city_id = %s AND district_id = %s
        """
        cursor.execute(query, (city_id, district_id))
        result = cursor.fetchall()

        # 데이터만 리스트로 반환
        return [row[0] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############## 읍면동 단위 모든 컬럼 정보 가져오기 ################
# 컬럼 값 테이블에서 가져오는 함수
def get_j_score_national_data(national_data):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    j_score_data = []

    try:
        for city_id, district_id, sub_district_id in national_data:
            query = """
                SELECT resident
                FROM loc_info
                WHERE city_id = %s AND district_id = %s AND sub_district_id = %s
            """
            cursor.execute(query, (city_id, district_id, sub_district_id))
            result = cursor.fetchone()  # 해당 읍/면/동의 컬럼 데이터를 하나 가져옴

            if result:

                # 튜플 (city_id, district_id, sub_district_id, 컬럼)를 리스트에 추가
                j_score_data.append(
                    (city_id, district_id, sub_district_id, result["resident"])
                )

            else:
                # 만약 데이터가 없는 경우 count를 0으로 설정
                j_score_data.append((city_id, district_id, sub_district_id, 0))

    except Exception as e:
        print(f"Error fetching j_score national data: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)

    return j_score_data  # j_score 데이터를 반환


############ 전국 j_score 데이터 넣기 ##################


def insert_j_score_nation(data):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            INSERT INTO statistics (STAT_ITEM_ID, city_id, district_id, sub_district_id, j_score, CREATED_AT, reference_id, ref_date, stat_level)
            VALUES (%s, %s, %s, %s, %s, now(), %s, %s, '전국')
        """
        # query = """
        #     UPDATE statistics 
        #     SET 
        #     J_SCORE = %s
        #     WHERE STAT_ITEM_ID = %s AND CITY_ID = %s AND DISTRICT_ID = %s 
        #     AND SUB_DISTRICT_ID = %s AND reference_id = %s AND ref_date = %s;
        # """

        # 순서를 맞추기 위해 data에서 j_score를 맨 앞에 두도록 순서 변경
        # data_to_update = [
        #     (item[4], item[0], item[1], item[2], item[3], item[5], item[6])
        #     for item in data
        # ]

        cursor.executemany(query, data)
        connection.commit()

    except Exception as e:
        print(f"Error inserting data: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


########## 전국 통계 업데이트 ###############
def update_stat_nation(national_stats):
    connection = None
    cursor = None
    try:
        # DB 연결
        connection = get_db_connection()
        cursor = connection.cursor()

        # SQL 업데이트 쿼리
        update_query = """
        UPDATE statistics 
        SET 
            AVG_VAL = %s, 
            MED_VAL = %s, 
            STD_VAL = %s, 
            MAX_VALUE = %s, 
            MIN_VALUE = %s
        WHERE STAT_ITEM_ID = %s;
        """

        # 딕셔너리에서 통계 값 추출
        avg_val = national_stats.get("average")
        med_val = national_stats.get("median")
        std_val = national_stats.get("stddev")
        max_val = national_stats.get("max")
        min_val = national_stats.get("min")
        stat_item_id = national_stats.get("stat_item_id")

        # 쿼리 실행
        cursor.execute(
            update_query, (avg_val, med_val, std_val, max_val, min_val, stat_item_id)
        )

        # 변경사항 커밋
        connection.commit()
        print("Statistics updated successfully.")

    except pymysql.MySQLError as e:
        print(f"Error updating statistics: {e}")
        connection.rollback()
    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############## 시/도 별 통계 인서트 ###################
def insert_stat_city(city_stat_list):
    connection = None
    cursor = None

    try:
        # 데이터베이스 연결
        connection = get_db_connection()
        cursor = connection.cursor()

        # INSERT 쿼리 작성 (sub_district_id와 j_score는 지금 없으므로 NULL 또는 적절히 처리)
        query = """
            INSERT INTO statistics (STAT_ITEM_ID, city_id, district_id, sub_district_id, AVG_VAL, MED_VAL, STD_VAL, MAX_VALUE, MIN_VALUE, stat_level, CREATED_AT, reference_id, ref_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '시도', NOW(), 4, '2024-08-01')
        """

        # 데이터를 튜플 형식으로 변환 후 executemany로 여러 행을 한 번에 삽입
        data_to_insert = [
            (
                item["stat_item_id"],
                item["city_id"],
                None,
                None,  # sub_district_id가 없으므로 None
                item["statistics"]["average"],
                item["statistics"]["median"],
                item["statistics"]["stddev"],
                item["statistics"]["max"],
                item["statistics"]["min"],
            )
            for item in city_stat_list
        ]

        # 여러 행을 한 번에 인서트
        cursor.executemany(query, data_to_insert)
        connection.commit()

        print(f"Successfully inserted {cursor.rowcount} rows into the statistics table")

    except Exception as e:
        print(f"Error inserting data: {e}")
        if connection:
            connection.rollback()

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############## 지역별 통계 인서트 ###################
def insert_stat_region(city_district_stats_list):
    connection = None
    cursor = None

    try:
        # 데이터베이스 연결
        connection = get_db_connection()
        cursor = connection.cursor()

        # INSERT 쿼리 작성 (sub_district_id와 j_score는 지금 없으므로 NULL 또는 적절히 처리)
        query = """
            INSERT INTO statistics (STAT_ITEM_ID, city_id, district_id, sub_district_id, AVG_VAL, MED_VAL, STD_VAL, MAX_VALUE, MIN_VALUE, stat_level, CREATED_AT, reference_id, ref_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '시군구', NOW(), 4, '2024-08-01')
        """

        # 데이터를 튜플 형식으로 변환 후 executemany로 여러 행을 한 번에 삽입
        data_to_insert = [
            (
                item["stat_item_id"],
                item["city_id"],
                item["district_id"],
                None,  # sub_district_id가 없으므로 None
                item["statistics"]["average"],
                item["statistics"]["median"],
                item["statistics"]["stddev"],
                item["statistics"]["max"],
                item["statistics"]["min"],
            )
            for item in city_district_stats_list
        ]

        # 여러 행을 한 번에 인서트
        cursor.executemany(query, data_to_insert)
        connection.commit()

        print(f"Successfully inserted {cursor.rowcount} rows into the statistics table")

    except Exception as e:
        print(f"Error inserting data: {e}")
        if connection:
            connection.rollback()

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


########### 지역별 j_score 업데이트 ###############
def update_j_score_data_region(j_score_data_region):
    connection = None
    cursor = None
    try:
        # DB 연결
        connection = get_db_connection()
        cursor = connection.cursor()

        # SQL 업데이트 쿼리
        query = """
            UPDATE statistics
            SET j_score = %s
            WHERE city_id = %s
            AND district_id = %s
            AND sub_district_id is NULL
            AND stat_item_id = %s
        """

        # 각 튜플에 대해 업데이트 쿼리를 실행
        for data in j_score_data_region:
            stat_item_id, city_id, district_id, sub_district_id, j_score = (
                data  # 튜플에서 값 가져오기
            )
            cursor.execute(query, (j_score, city_id, district_id, stat_item_id))

        # 변경사항 커밋
        connection.commit()
        print(f"Successfully updated {cursor.rowcount} rows.")

    except Exception as e:
        print(f"Error updating j_score: {e}")
        if connection:
            connection.rollback()  # 문제가 생기면 롤백

    finally:
        if cursor:
            close_cursor(cursor)  # 커서 닫기
        if connection:
            close_connection(connection)  # 연결 닫기


######################## 전국 범위 동별 mz 세대 인구 값 가져오기 ##############################
def get_j_score_national_data_mz(national_data):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    mz_population_data = []

    try:
        for city_id, district_id, sub_district_id in national_data:
            query = """
                SELECT 
                SUM(age_14 + age_15 + age_16 + age_17 + age_18 + 
                    age_19 + age_20 + age_21 + age_22 + age_23 + 
                    age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS mz_population
                FROM population
                JOIN city ON population.city_id = city.city_id
                JOIN district ON population.district_id = district.district_id
                LEFT JOIN sub_district ON population.sub_district_id = sub_district.sub_district_id
                WHERE population.reference_date = '2024-07-31'
                AND population.city_id = %s AND population.district_id = %s AND population.sub_district_id = %s
            """
            cursor.execute(query, (city_id, district_id, sub_district_id))
            result = cursor.fetchone()  # 해당 읍/면/동의 컬럼 데이터를 하나 가져옴

            if result:
                # 튜플 (city_id, district_id, sub_district_id, 컬럼)를 리스트에 추가
                mz_population_data.append(
                    (city_id, district_id, sub_district_id, result["mz_population"])
                )

            else:
                # 만약 데이터가 없는 경우 count를 0으로 설정
                mz_population_data.append((city_id, district_id, sub_district_id, 0))

    except Exception as e:
        print(f"Error fetching j_score national data: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)

    return mz_population_data


################### 전국 mz 세대 인구 값 가져오기 ##################
def get_national_data_mz_population():
    """
    전국 단위 mz 세대 인구수 데이터를 가져오는 함수
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 전국 데이터를 가져오는 쿼리
        query = """
            SELECT 
            SUM(age_14 + age_15 + age_16 + age_17 + age_18 + 
                    age_19 + age_20 + age_21 + age_22 + age_23 + 
                    age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS mz_population
            FROM population
            group by sub_district_id
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # 데이터만 리스트로 반환
        return [row[0] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


############ 특정 시/군/구의 mz 세대 인구 데이터를 가져오는 함수 ##############
def get_city_district_data_mz_population(city_id, district_id):
    """
    특정 시/군/구의 mz 세대 인구 데이터를 가져오는 함수
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 특정 시/군/구의 매장 데이터를 가져오는 쿼리
        query = """
            SELECT 
            SUM(age_14 + age_15 + age_16 + age_17 + age_18 + 
                    age_19 + age_20 + age_21 + age_22 + age_23 + 
                    age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS mz_population
            FROM population
            WHERE city_id = %s AND district_id = %s
            group by sub_district_id
        """
        cursor.execute(query, (city_id, district_id))
        result = cursor.fetchall()

        # 데이터만 리스트로 반환
        return [row[0] for row in result]

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


########## mz 세대 인구 데이터 j_score 업데이트 #################
def get_data_for_city_and_district_mz_population(city_id, district_id):
    """
    특정 city_id와 district_id에 대한 컬럼 수(count)를 가져옴
    여러 sub_district의 매장 수를 합산하여 반환
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # city_id와 district_id에 해당하는 모든 sub_district_id의 매장 수를 가져오는 쿼리
        query = """
            SELECT
            SUM(age_14 + age_15 + age_16 + age_17 + age_18 + 
                    age_19 + age_20 + age_21 + age_22 + age_23 + 
                    age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS mz_population
            FROM population
            WHERE city_id = %s AND district_id = %s
        """

        cursor.execute(query, (city_id, district_id))
        result = cursor.fetchone()

        # 매장 수가 존재하지 않으면 0으로 처리
        total_count = (
            result["mz_population"] if result["mz_population"] is not None else 0
        )
        return total_count

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)


####################################### 리포트 부분 ###########################################


################# 전국 범위 동별 j_score 가중치 적용 평균 구하기 #####################
def get_weighted_jscore(sub_district_id):
    # DB 연결 설정
    connection = get_db_connection()
    cursor = None

    try:
        # 쿼리 실행
        query_statistics = """
            SELECT 
                   city.city_name AS city_name, 
                   district.district_name AS district_name, 
                   sub_district.sub_district_name AS sub_district_name,
                   sub_district.sub_district_id AS sub_district_id,
                   stat_item.table_name AS table_name,
                   stat_item.column_name AS column_name,
                   J_SCORE, ref_date
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            LEFT JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            JOIN stat_item ON statistics.STAT_ITEM_ID = stat_item.STAT_ITEM_ID
            WHERE statistics.sub_district_id = %s
            AND (stat_item.STAT_ITEM_ID BETWEEN 1 AND 8 OR stat_item.STAT_ITEM_ID = 14)
        """
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query_statistics, (sub_district_id,))
        statistics_result = cursor.fetchall()

        return statistics_result  # 딕셔너리가 아닌 쿼리 결과를 바로 반환

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료


########### 동별 주거 환경 ###########
def get_living_env(sub_district_id):
    # DB 연결 설정
    connection = get_db_connection()
    cursor = None

    try:
        # 쿼리 실행
        query_statistics = """
            SELECT 
                city_name, district_name, sub_district_name,
                work_pop, resident
            FROM loc_info
            JOIN city ON loc_info.city_id = city.city_id
            JOIN district ON loc_info.district_id = district.district_id
            JOIN sub_district ON loc_info.sub_district_id = sub_district.sub_district_id
            WHERE loc_info.sub_district_id = %s
        """
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query_statistics, (sub_district_id,))
        statistics_result = cursor.fetchone()

        return statistics_result  # 딕셔너리가 아닌 쿼리 결과를 바로 반환

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료


################ 인근 유동 인구 #####################
def get_move_pop_and_j_score(sub_district_id):
    connection = get_db_connection()
    cursor = None

    try:
        # 첫 번째 쿼리: 이동 인구 정보
        query_move_pop = """
            SELECT 
                city_name, district_name, sub_district_name,
                move_pop, y_m
            FROM loc_info
            JOIN city ON loc_info.city_id = city.city_id
            JOIN district ON loc_info.district_id = district.district_id
            JOIN sub_district ON loc_info.sub_district_id = sub_district.sub_district_id
            WHERE loc_info.sub_district_id = %s
        """

        # 두 번째 쿼리: 속한 시/도의 값 불러오기
        query_move_pop_list = """
            SELECT 
                city.city_name, district.district_name, sub_district.sub_district_name,
                loc_info.move_pop
            FROM loc_info
            JOIN city ON loc_info.city_id = city.city_id
            JOIN district ON loc_info.district_id = district.district_id
            JOIN sub_district ON loc_info.sub_district_id = sub_district.sub_district_id
            WHERE loc_info.city_id = (SELECT sub_district.city_id FROM sub_district WHERE sub_district.sub_district_id = %s)
        """

        # 세 번째 쿼리: 시/도 통계 정보 정보
        query_stat_city = """
            SELECT 
                AVG_VAL
            FROM statistics
            WHERE statistics.stat_item_id = 2 AND city_id = (select city_id from sub_district where sub_district_id = %s)
            AND district is null and sub_district is null
        """

        # 네 번째 쿼리: J_SCORE 정보
        query_j_score = """
            SELECT 
                city_name, district_name, sub_district_name,
                j_score
            FROM statistics
            JOIN city ON statistics.city_id = city.city_id
            JOIN district ON statistics.district_id = district.district_id
            JOIN sub_district ON statistics.sub_district_id = sub_district.sub_district_id
            WHERE statistics.stat_item_id = 2 AND sub_district.SUB_DISTRICT_ID = %s
        """
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 첫 번째 쿼리 실행
        cursor.execute(query_move_pop, (sub_district_id,))
        move_pop_result = cursor.fetchall()

        # 두 번째 쿼리 실행
        cursor.execute(query_move_pop_list, (sub_district_id,))
        move_pop_list = cursor.fetchall()

        # 두 번째 쿼리 실행
        cursor.execute(query_move_pop_list, (sub_district_id,))
        move_pop_city_stat = cursor.fetchall()

        # 세 번째 쿼리 실행
        cursor.execute(query_stat_city, (sub_district_id,))
        j_score_result = cursor.fetchall()

        # 결과를 함께 반환
        return {
            "move_pop_data": move_pop_result,
            "move_pop_list": move_pop_list,
            "move_pop_city_stat": move_pop_city_stat,
            "j_score_data": j_score_result,
        }

    finally:
        if cursor:
            cursor.close()
        connection.close()  # 연결 종료


####################################################
# 전국 jscore 조회
def select_nationwide_jscore_by_stat_item_id_and_sub_district_id(
    stat_item_id: int, sub_district_id: int
) -> StatisticsJscoreOutput:

    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        select_query = """
            SELECT 
                J_SCORE,
                REF_DATE
            FROM
                statistics
            WHERE STAT_ITEM_ID = %s
            AND
                SUB_DISTRICT_ID = %s
            ORDER BY REF_DATE DESC
            LIMIT 1
            ;
        """

        cursor.execute(select_query, (stat_item_id, sub_district_id))
        row = cursor.fetchone()

        # logger.info(f"Executing query: {select_query % (stat_item_id,sub_district_id)}")

        # print(row)

        return row

    finally:
        if cursor:
            cursor.close()
        connection.close()


############## 읍면동 단위 모든 컬럼 정보 가져오기 ################
# 컬럼 값 테이블에서 가져오는 함수
def get_j_score_national_data_by_detail_categroy_id(
    national_data, detail_category_id, stat_item_table_name, stat_item_column_name
):
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    j_score_data = []

    try:
        for city_id, district_id, sub_district_id in national_data:
            select_query = f"""
                SELECT `{stat_item_column_name}`
                FROM `{stat_item_table_name}`
                WHERE city_id = %s AND district_id = %s AND sub_district_id = %s AND biz_detail_category_id = %s
                ;
            """
            # logger.info(
            #     f"Executing query: {select_query % (city_id, district_id, sub_district_id, detail_category_id)}"
            # )

            # 쿼리 실행: 숫자나 문자열 값은 매개변수로 전달
            cursor.execute(
                select_query,
                (city_id, district_id, sub_district_id, detail_category_id),
            )
            result = cursor.fetchone()  # 해당 읍/면/동의 컬럼 데이터를 하나 가져옴

            if result:
                # 튜플 (city_id, district_id, sub_district_id, 컬럼)를 리스트에 추가
                j_score_data.append(
                    (
                        city_id,
                        district_id,
                        sub_district_id,
                        result[stat_item_column_name],
                    )
                )

    except Exception as e:
        print(f"Error fetching j_score national data: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)

    return j_score_data  # j_score 데이터를 반환


def select_statistics_data_by_sub_district_id_detail_category_id(
    sub_district_id: int, stat_item_id_list: List[Dict[str, str]]
) -> CommercialStatisticsData:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    statistics_data = {
        "market_size": CommercialStatistics(),
        "average_sales": CommercialStatistics(),
        "average_payment": CommercialStatistics(),
        "usage_count": CommercialStatistics(),
        "sub_district_density": CommercialStatistics(),
    }

    # print(stat_item_id_list)

    try:
        for stat_item in stat_item_id_list:
            stat_item_id = stat_item["STAT_ITEM_ID"]
            column_name = stat_item["COLUMN_NAME"]

            query = """
                SELECT 
                    AVG_VAL, 
                    MED_VAL, 
                    STD_VAL, 
                    MAX_VALUE, 
                    MIN_VALUE, 
                    J_SCORE 
                FROM statistics 
                WHERE stat_item_id = %s AND sub_district_id = %s 
                ;
            """
            cursor.execute(query, (stat_item_id, sub_district_id))
            result = cursor.fetchone()

            if result and column_name in statistics_data:
                statistics_data[column_name] = CommercialStatistics(
                    avg_val=result.get("AVG_VAL", 0.0),
                    med_val=result.get("MED_VAL", 0.0),
                    std_val=result.get("STD_VAL", 0.0),
                    max_val=result.get("MAX_VALUE", 0.0),
                    min_val=result.get("MIN_VALUE", 0.0),
                    j_score=result.get("J_SCORE", 0.0),
                )
            elif column_name not in statistics_data:
                print(f"Column '{column_name}' is not in statistics_data")

        # print(statistics_data)

        data = CommercialStatisticsData(**statistics_data)

        return data

    except Exception as e:
        print(f"Error fetching statistics data: {e}")
        raise
    finally:
        close_cursor(cursor)
        close_connection(connection)
