from app.schemas.sub_district import AllRegionIdOutPut
from app.schemas.population_age import PopAgeByRegionOutPut
import logging
from pymysql import MySQLError
from typing import List, Optional
import pymysql
from datetime import date

from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


# 1. 지역 별 연령별 인구 조회
def select_pop_age_by_region(
        city_id:int, district_id:int, sub_district_id:int, ref_date: date
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
                    REFERENCE_ID,
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
                where city_id = %s and district_id = %s and sub_district_id = %s and reference_date = %s
            """

            cursor.execute(select_query, (city_id, district_id, sub_district_id, ref_date))
            temp_list = cursor.fetchall()
            

            # 총 인구수 값 더하기
            rows = []
            for i in range(0, len(temp_list), 2):
                if i + 1 < len(temp_list):
                    # 남자와 여자의 총 인구수 합산
                    combined_total = temp_list[i]["TOTAL_POPULATION_BY_GENDER"] + temp_list[i + 1]["TOTAL_POPULATION_BY_GENDER"]
                    
                    # 남자 데이터에 합산 값을 추가
                    male_data = temp_list[i].copy()  # 딕셔너리 복사
                    male_data["TOTAL_POPULATION"] = combined_total  # 합산 값을 추가

                    # 여자 데이터에 합산 값을 추가
                    female_data = temp_list[i + 1].copy()  # 딕셔너리 복사
                    female_data["TOTAL_POPULATION"] = combined_total  # 합산 값을 추가
                    
                    # 결과 리스트에 추가
                    rows.append(male_data)
                    rows.append(female_data)

            for row in rows:
                pop_age_by_region = PopAgeByRegionOutPut(
                    city_id= row.get("CITY_ID"),
                    district_id= row.get("DISTRICT_ID"),
                    sub_district_id=row.get("SUB_DISTRICT_ID"),
                    gender_id= row.get("GENDER_ID"),
                    reference_id = row.get("REFERENCE_ID"),
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


# 3. 각 지역에 해당하는 연령 별 인구 수 인서트
def insert_pop_age_by_region(all_pop_age_by_region: List[PopAgeByRegionOutPut]) -> None:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)

    try:
        if connection.open:
            insert_query = """
                INSERT INTO population_age (
                    city_id, district_id, sub_district_id, gender_id, reference_id, ref_date,
                    age_under_10s, age_10s, age_20s, age_30s, age_40s,
                    age_50s, age_plus_60s, total_population_by_gender, total_population
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """

            # 각 PopAgeByRegionOutPut 객체를 인서트
            for pop_age in all_pop_age_by_region:
                cursor.execute(insert_query, (
                    pop_age.city_id,
                    pop_age.district_id,
                    pop_age.sub_district_id,
                    pop_age.gender_id,
                    pop_age.reference_id,
                    pop_age.ref_date,
                    pop_age.age_under_10s,
                    pop_age.age_10s,
                    pop_age.age_20s,
                    pop_age.age_30s,
                    pop_age.age_40s,
                    pop_age.age_50s,
                    pop_age.age_plus_60s,
                    pop_age.total_population_by_gender,
                    pop_age.total_population
                ))

            # 변경사항 커밋
            connection.commit()

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