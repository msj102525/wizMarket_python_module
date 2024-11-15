import pymysql
from app.db.connect import get_db_connection, close_connection, close_cursor, commit, rollback, get_re_db_connection
from app.schemas.commercial_district import CommercialDistrictDrawPlot

def select_social_data():
    results = []
    connection = get_db_connection()
    cursor = None

    try:
        query = """
            SELECT 
                CD.BIZ_DETAIL_CATEGORY_ID,
                CD.SUB_DISTRICT_ID,
                CD.COMMERCIAL_DISTRICT_ID,
                CI.CITY_NAME,
                DI.DISTRICT_NAME,
                SD.SUB_DISTRICT_NAME,
                BMC.BIZ_MAIN_CATEGORY_NAME,
                BSC.BIZ_SUB_CATEGORY_NAME,
                BDC.BIZ_DETAIL_CATEGORY_NAME,
                CD.NATIONAL_DENSITY,
                CD.CITY_DENSITY,
                CD.DISTRICT_DENSITY,
                CD.SUB_DISTRICT_DENSITY,
                CD.MARKET_SIZE,
                CD.AVERAGE_SALES,
                CD.AVERAGE_PAYMENT,
                CD.USAGE_COUNT,
                CD.OPERATING_COST,
                CD.FOOD_COST,
                CD.EMPLOYEE_COST,
                CD.RENTAL_COST,
                CD.TAX_COST,
                CD.FAMILY_EMPLOYEE_COST,
                CD.CEO_COST,
                CD.ETC_COST,
                CD.AVERAGE_PROFIT,
                CD.AVG_PROFIT_PER_MON,
                CD.AVG_PROFIT_PER_TUE,
                CD.AVG_PROFIT_PER_WED,
                CD.AVG_PROFIT_PER_THU,
                CD.AVG_PROFIT_PER_FRI,
                CD.AVG_PROFIT_PER_SAT,
                CD.AVG_PROFIT_PER_SUN,
                CD.AVG_PROFIT_PER_06_09,
                CD.AVG_PROFIT_PER_09_12,
                CD.AVG_PROFIT_PER_12_15,
                CD.AVG_PROFIT_PER_15_18,
                CD.AVG_PROFIT_PER_18_21,
                CD.AVG_PROFIT_PER_21_24,
                CD.AVG_PROFIT_PER_24_06,
                CD.AVG_CLIENT_PER_M_20,
                CD.AVG_CLIENT_PER_M_30,
                CD.AVG_CLIENT_PER_M_40,
                CD.AVG_CLIENT_PER_M_50,
                CD.AVG_CLIENT_PER_M_60,
                CD.AVG_CLIENT_PER_F_20,
                CD.AVG_CLIENT_PER_F_30,
                CD.AVG_CLIENT_PER_F_40,
                CD.AVG_CLIENT_PER_F_50,
                CD.AVG_CLIENT_PER_F_60,
                CD.TOP_MENU_1,
                CD.TOP_MENU_2,
                CD.TOP_MENU_3,
                CD.TOP_MENU_4,
                CD.TOP_MENU_5,
                CD.Y_M,
                CD.CREATED_AT,
                CD.UPDATED_AT
            FROM 
                COMMERCIAL_DISTRICT cd
            JOIN
                CITY CI ON cd.CITY_ID = CI.CITY_ID
            JOIN
                DISTRICT DI ON cd.DISTRICT_ID = DI.DISTRICT_ID
            JOIN
                SUB_DISTRICT SD ON cd.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
            JOIN
                BIZ_MAIN_CATEGORY BMC ON cd.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
            JOIN
                BIZ_SUB_CATEGORY BSC ON cd.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
            JOIN
                BIZ_DETAIL_CATEGORY BDC ON cd.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
            WHERE cd.city_id = 9 and bmc.BIZ_MAIN_CATEGORY_ID = 1
            ORDER BY di.DISTRICT_ID, sd.SUB_DISTRICT_ID
        """

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()

        # 변환된 데이터를 스키마에 맞게 저장
        results = [CommercialDistrictDrawPlot(
            biz_detail_category_id=row.get("BIZ_DETAIL_CATEGORY_ID"),
            sub_district_id=row.get("SUB_DISTRICT_ID"),
            commercial_district_id=row.get("COMMERCIAL_DISTRICT_ID"),
            city_name=row.get("CITY_NAME"),
            district_name=row.get("DISTRICT_NAME"),
            sub_district_name=row.get("SUB_DISTRICT_NAME"),
            biz_main_category_name=row.get("BIZ_MAIN_CATEGORY_NAME"),
            biz_sub_category_name=row.get("BIZ_SUB_CATEGORY_NAME"),
            biz_detail_category_name=row.get("BIZ_DETAIL_CATEGORY_NAME"),
            national_density=row.get("NATIONAL_DENSITY"),
            city_density=row.get("CITY_DENSITY"),
            district_density=row.get("DISTRICT_DENSITY"),
            sub_district_density=row.get("SUB_DISTRICT_DENSITY"),
            market_size=row.get("MARKET_SIZE"),
            average_sales=row.get("AVERAGE_SALES"),
            average_payment=row.get("AVERAGE_PAYMENT"),
            usage_count=row.get("USAGE_COUNT"),
            operating_cost=row.get("OPERATING_COST"),
            food_cost=row.get("FOOD_COST"),
            employee_cost=row.get("EMPLOYEE_COST"),
            rental_cost=row.get("RENTAL_COST"),
            tax_cost=row.get("TAX_COST"),
            family_employee_cost=row.get("FAMILY_EMPLOYEE_COST"),
            ceo_cost=row.get("CEO_COST"),
            etc_cost=row.get("ETC_COST"),
            average_profit=row.get("AVERAGE_PROFIT"),
            avg_profit_per_mon=row.get("AVG_PROFIT_PER_MON"),
            avg_profit_per_tue=row.get("AVG_PROFIT_PER_TUE"),
            avg_profit_per_wed=row.get("AVG_PROFIT_PER_WED"),
            avg_profit_per_thu=row.get("AVG_PROFIT_PER_THU"),
            avg_profit_per_fri=row.get("AVG_PROFIT_PER_FRI"),
            avg_profit_per_sat=row.get("AVG_PROFIT_PER_SAT"),
            avg_profit_per_sun=row.get("AVG_PROFIT_PER_SUN"),
            avg_profit_per_06_09=row.get("AVG_PROFIT_PER_06_09"),
            avg_profit_per_09_12=row.get("AVG_PROFIT_PER_09_12"),
            avg_profit_per_12_15=row.get("AVG_PROFIT_PER_12_15"),
            avg_profit_per_15_18=row.get("AVG_PROFIT_PER_15_18"),
            avg_profit_per_18_21=row.get("AVG_PROFIT_PER_18_21"),
            avg_profit_per_21_24=row.get("AVG_PROFIT_PER_21_24"),
            avg_profit_per_24_06=row.get("AVG_PROFIT_PER_24_06"),
            avg_client_per_m_20=row.get("AVG_CLIENT_PER_M_20"),
            avg_client_per_m_30=row.get("AVG_CLIENT_PER_M_30"),
            avg_client_per_m_40=row.get("AVG_CLIENT_PER_M_40"),
            avg_client_per_m_50=row.get("AVG_CLIENT_PER_M_50"),
            avg_client_per_m_60=row.get("AVG_CLIENT_PER_M_60"),
            avg_client_per_f_20=row.get("AVG_CLIENT_PER_F_20"),
            avg_client_per_f_30=row.get("AVG_CLIENT_PER_F_30"),
            avg_client_per_f_40=row.get("AVG_CLIENT_PER_F_40"),
            avg_client_per_f_50=row.get("AVG_CLIENT_PER_F_50"),
            avg_client_per_f_60=row.get("AVG_CLIENT_PER_F_60"),
            top_menu_1=row.get("TOP_MENU_1"),
            top_menu_2=row.get("TOP_MENU_2"),
            top_menu_3=row.get("TOP_MENU_3"),
            top_menu_4=row.get("TOP_MENU_4"),
            top_menu_5=row.get("TOP_MENU_5"),
            y_m=row.get("Y_M"),
            created_at=row.get("CREATED_AT"),
            updated_at=row.get("UPDATED_AT")
        ) for row in rows]

        return results

    finally:
        close_cursor(cursor)     
        close_connection(connection)