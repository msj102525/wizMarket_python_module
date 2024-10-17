from app.schemas.sub_district import AllRegionIdOutPut
from app.schemas.population_age import PopAgeByRegionOutPut
import logging
from pymysql import MySQLError
from typing import List, Optional
import pymysql

from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)

# 1. 전 지역 ID 값 조회
def select_all_region_id() -> List[AllRegionIdOutPut]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[AllRegionIdOutPut] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID
                FROM
                    sub_district
            """

            cursor.execute(select_query)
            rows = cursor.fetchall()

            for row in rows:
                all_region_id = AllRegionIdOutPut(
                    city_id=row.get("CITY_ID"),
                    district_id=row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID")
                )
                results.append(all_region_id)
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


# 2. 지역 별 연령별 인구 조회
def select_pop_age_by_region(
        city_id:int, district_id:int, sub_district_id:int
) -> List[PopAgeByRegionOutPut]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[PopAgeByRegionOutPut] = []

    try:
        if connection.open:
            select_query = """
                SELECT
                    CITY_ID,
                    DISTRICT_ID,
                    SUB_DISTRICT_ID,
                    GENDER_ID,
                    reference_date as REF_DATE,
                    (age_0 + age_1 + age_2 + age_3 + age_4 + age_5 + age_6 + age_7 + age_8 + age_9) AS AGE_UNDER_10s,
                    (age_10 + age_11 + age_12 + age_13 + age_14 + age_15 + age_16 + age_17 + age_18 + age_19) AS AGE_10s,
                    (age_20 + age_21 + age_22 + age_23 + age_24 + age_25 + age_26 + age_27 + age_28 + age_29) AS AGE_20s,
                    (age_30 + age_31 + age_32 + age_33 + age_34 + age_35 + age_36 + age_37 + age_38 + age_39) AS AGE_30s,
                    (age_40 + age_41 + age_42 + age_43 + age_44 + age_45 + age_46 + age_47 + age_48 + age_49) AS AGE_40s,
                    (age_50 + age_51 + age_52 + age_53 + age_54 + age_55 + age_56 + age_57 + age_58 + age_59) AS AGE_50s,
                    (age_60 + age_61 + age_62 + age_63 + age_64 + age_65 + age_66 + age_67 + age_68 + age_69 + 
                    age_70 + age_71 + age_72 + age_73 + age_74 + age_75 + age_76 + age_77 + age_78 + age_79 + 
                    age_80 + age_81 + age_82 + age_83 + age_84 + age_85 + age_86 + age_87 + age_88 + age_89 + 
                    age_90 + age_91 + age_92 + age_93 + age_94 + age_95 + age_96 + age_97 + age_98 + age_99 + 
                    age_100 + age_101 + age_102 + age_103 + age_104 + age_105 + age_106 + age_107 + age_108 + 
                    age_109 + age_110_over) AS AGE_PLUS_60s,
                    (age_0 + age_1 + age_2 + age_3 + age_4 + age_5 + age_6 + age_7 + age_8 + age_9 +
                    age_10 + age_11 + age_12 + age_13 + age_14 + age_15 + age_16 + age_17 + age_18 + age_19 +
                    age_20 + age_21 + age_22 + age_23 + age_24 + age_25 + age_26 + age_27 + age_28 + age_29 +
                    age_30 + age_31 + age_32 + age_33 + age_34 + age_35 + age_36 + age_37 + age_38 + age_39 +
                    age_40 + age_41 + age_42 + age_43 + age_44 + age_45 + age_46 + age_47 + age_48 + age_49 +
                    age_50 + age_51 + age_52 + age_53 + age_54 + age_55 + age_56 + age_57 + age_58 + age_59 +
                    age_60 + age_61 + age_62 + age_63 + age_64 + age_65 + age_66 + age_67 + age_68 + age_69 + 
                    age_70 + age_71 + age_72 + age_73 + age_74 + age_75 + age_76 + age_77 + age_78 + age_79 + 
                    age_80 + age_81 + age_82 + age_83 + age_84 + age_85 + age_86 + age_87 + age_88 + age_89 + 
                    age_90 + age_91 + age_92 + age_93 + age_94 + age_95 + age_96 + age_97 + age_98 + age_99 + 
                    age_100 + age_101 + age_102 + age_103 + age_104 + age_105 + age_106 + age_107 + age_108 + 
                    age_109 + age_110_over) AS TOTAL_POPULATION_BY_GENDER
                FROM
                    population
                where city_id = %s and district_id = %s and sub_district_id = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id))
            temp_list = cursor.fetchall()

            # 총 인구수 값 더하기
            rows = []
            for i in range(0, len(temp_list), 2):
                if i + 1 < len(temp_list):
                    # 두 항목을 합침
                    combined_population = temp_list[i]["TOTAL_POPULATION_BY_GENDER"] + temp_list[i + 1]["TOTAL_POPULATION_BY_GENDER"]
                    # 새 항목 생성 및 total_population 추가
                    merged_result = temp_list[i].copy()  # 첫 번째 항목을 복사
                    merged_result["TOTAL_POPULATION"] = combined_population
                    rows.append(merged_result)

            for row in rows:
                pop_age_by_region = PopAgeByRegionOutPut(
                    city_id= row.get("CITY_ID"),
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    gender_id= row.get("GENDER_ID"),
                    ref_date= row.get("REF_DATE"),
                    age_under_10s= row.get("AGE_UNDER_10s"),
                    age_10s= row.get("AGE_10s"),
                    age_20s= row.get("AGE_20s"),
                    age_30s= row.get("AGE_30s"),
                    age_40s= row.get("AGE_40s"),
                    age_50s= row.get("AGE_50s"),
                    age_plus_60s= row.get("AGE_PLUS_60s"),
                    total_population_by_gender= row.get("TOTAL_POPULATION_BY_GENDER"),
                    total_population= row.get("TOTAL_POPULATION")
                )
                results.append(pop_age_by_region)
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