import pymysql
from app.db.connect import get_db_connection, close_connection, close_cursor, get_re_db_connection
from app.schemas.loc_info import LocInfoGenerateImageText 


def select_generage_image_text():
    results = []
    connection = get_re_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                ROAD_NAME,
                LOC_INFO_RESIDENT_J_SCORE,
                LOC_INFO_WORK_POP_J_SCORE,
                LOC_INFO_SHOP_J_SCORE,
                LOC_INFO_INCOME_J_SCORE,
                LOC_INFO_MZ_POPULATION_J_SCORE,
                LOC_INFO_AVERAGE_SPEND_J_SCORE,
                LOC_INFO_AVERAGE_SALES_J_SCORE,
                LOC_INFO_HOUSE_J_SCORE
            FROM 
                REPORT
            WHERE
                STORE_BUSINESS_NUMBER = 'JS0001'
        """

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()

        # 변환된 데이터를 스키마에 맞게 저장
        results = [LocInfoGenerateImageText(
            road_name=row.get("ROAD_NAME"),
            resident_j_score=row.get("LOC_INFO_RESIDENT_J_SCORE"),
            work_pop_j_score=row.get("LOC_INFO_WORK_POP_J_SCORE"),
            shop_j_score=row.get("LOC_INFO_SHOP_J_SCORE"),
            income_j_score=row.get("LOC_INFO_INCOME_J_SCORE"),
            mz_population_j_score=row.get("LOC_INFO_MZ_POPULATION_J_SCORE"),
            average_spend_j_score=row.get("LOC_INFO_AVERAGE_SPEND_J_SCORE"),
            average_sales_j_score=row.get("LOC_INFO_AVERAGE_SALES_J_SCORE"),
            house_j_score=row.get("LOC_INFO_HOUSE_J_SCORE")
        ) for row in rows]

        return results

    finally:
        close_cursor(cursor)
        close_connection(connection)
