import logging
from typing import List, Optional
import pymysql
from app.schemas.commercial_district import (
    CommercialDistrictInsert,
    CommercialDistrictOutput,
)
from app.db.connect import (
    get_db_connection,
    close_connection,
    close_cursor,
    commit,
    rollback,
)


def insert_commercial_district(data: CommercialDistrictInsert):
    connection = get_db_connection()
    cursor = connection.cursor()
    logger = logging.getLogger(__name__)

    try:
        insert_query = """
        INSERT INTO commercial_district (
            city_id, district_id, sub_district_id, 
            biz_main_category_id, biz_sub_category_id, biz_detail_category_id,
            national_density, city_density, district_density, sub_district_density,
            market_size, average_payment, usage_count,
            average_sales, operating_cost, food_cost, employee_cost, rental_cost, tax_cost, 
            family_employee_cost, ceo_cost, etc_cost, average_profit,
            avg_profit_per_mon, avg_profit_per_tue, avg_profit_per_wed, avg_profit_per_thu, avg_profit_per_fri, avg_profit_per_sat, avg_profit_per_sun,
            avg_profit_per_06_09, avg_profit_per_09_12, avg_profit_per_12_15, avg_profit_per_15_18, avg_profit_per_18_21, avg_profit_per_21_24, avg_profit_per_24_06,
            avg_client_per_m_20, avg_client_per_m_30, avg_client_per_m_40, avg_client_per_m_50, avg_client_per_m_60,
            avg_client_per_f_20, avg_client_per_f_30, avg_client_per_f_40, avg_client_per_f_50, avg_client_per_f_60,
            top_menu_1, top_menu_2, top_menu_3, top_menu_4, top_menu_5
        ) VALUES (
            %(city_id)s, %(district_id)s, %(sub_district_id)s, 
            %(biz_main_category_id)s, %(biz_sub_category_id)s, %(biz_detail_category_id)s,
            %(national_density)s, %(city_density)s, %(district_density)s, %(sub_district_density)s,
            %(market_size)s, %(average_payment)s, %(usage_count)s,
            %(average_sales)s, %(operating_cost)s, %(food_cost)s, %(employee_cost)s, %(rental_cost)s, %(tax_cost)s, 
            %(family_employee_cost)s, %(ceo_cost)s, %(etc_cost)s, %(average_profit)s,
            %(avg_profit_per_mon)s, %(avg_profit_per_tue)s, %(avg_profit_per_wed)s, %(avg_profit_per_thu)s, %(avg_profit_per_fri)s, %(avg_profit_per_sat)s, %(avg_profit_per_sun)s,
            %(avg_profit_per_06_09)s, %(avg_profit_per_09_12)s, %(avg_profit_per_12_15)s, %(avg_profit_per_15_18)s, %(avg_profit_per_18_21)s, %(avg_profit_per_21_24)s, %(avg_profit_per_24_06)s,
            %(avg_client_per_m_20)s, %(avg_client_per_m_30)s, %(avg_client_per_m_40)s, %(avg_client_per_m_50)s, %(avg_client_per_m_60)s,
            %(avg_client_per_f_20)s, %(avg_client_per_f_30)s, %(avg_client_per_f_40)s, %(avg_client_per_f_50)s, %(avg_client_per_f_60)s,
            %(top_menu_1)s, %(top_menu_2)s, %(top_menu_3)s, %(top_menu_4)s, %(top_menu_5)s
        );
        """

        cursor.execute(insert_query, data)
        commit(connection)
        logger.info("Executing query: %s with data: %s", insert_query, data)

    except pymysql.MySQLError as e:
        rollback(connection)
        logger.error(f"Error inserting data: {e}")
    finally:
        close_cursor(cursor)
        close_connection(connection)


def select_all_commercial_district_by_sub_district_id(
    sub_district_id: int,
) -> List[CommercialDistrictOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictOutput] = []

    try:
        select_query = """
        SELECT 
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
            CD.CREATED_AT,
            CD.UPDATED_AT
        FROM 
            COMMERCIAL_DISTRICT CD
        JOIN 
            CITY CI ON CD.CITY_ID = CI.CITY_ID
        JOIN 
            DISTRICT DI ON CD.DISTRICT_ID = DI.DISTRICT_ID
        JOIN 
            SUB_DISTRICT SD ON CD.SUB_DISTRICT_ID = SD.SUB_DISTRICT_ID
        JOIN 
            BIZ_MAIN_CATEGORY BMC ON CD.BIZ_MAIN_CATEGORY_ID = BMC.BIZ_MAIN_CATEGORY_ID
        JOIN 
            BIZ_SUB_CATEGORY BSC ON CD.BIZ_SUB_CATEGORY_ID = BSC.BIZ_SUB_CATEGORY_ID
        JOIN 
            BIZ_DETAIL_CATEGORY BDC ON CD.BIZ_DETAIL_CATEGORY_ID = BDC.BIZ_DETAIL_CATEGORY_ID
        WHERE 
            CD.SUB_DISTRICT_ID = %s
        ;
        """
        cursor.execute(select_query, (sub_district_id,))
        rows = cursor.fetchall()

        # logger.info(f"Executing query: {select_query % (sub_district_id, )}")

        for row in rows:
            commercial_district_output = CommercialDistrictOutput(
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
                average_payment=row.get("AVERAGE_PAYMENT"),
                usage_count=row.get("USAGE_COUNT"),
                average_sales=row.get("AVERAGE_SALES"),
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
                created_at=row.get("CREATED_AT"),
                updated_at=row.get("UPDATED_AT"),
            )
            results.append(commercial_district_output)
        return results

    except pymysql.MySQLError as e:
        rollback(connection)
        logger.error(f"Error fetching data: {e}")
    except Exception as e:
        print(e)
    finally:
        close_cursor(cursor)
        close_connection(connection)


def select_commercial_district_by_dynamic_query(
    city_id: Optional[int] = None,
    district_id: Optional[int] = None,
    sub_district_id: Optional[int] = None,
    biz_main_category_id: Optional[int] = None,
    biz_sub_category_id: Optional[int] = None,
    biz_detail_category_id: Optional[int] = None,
    market_size_min: Optional[int] = None,
    market_size_max: Optional[int] = None,
    avg_sales_min: Optional[int] = None,
    avg_sales_max: Optional[int] = None,
    operating_cost_min: Optional[int] = None,
    operating_cost_max: Optional[int] = None,
    food_cost_min: Optional[int] = None,
    food_cost_max: Optional[int] = None,
    employee_cost_min: Optional[int] = None,  # 인건비X -> 평균 결제
    employee_cost_max: Optional[int] = None,  # 인건비X -> 평균 결제
    rental_cost_min: Optional[int] = None,
    rental_cost_max: Optional[int] = None,
    avg_profit_min: Optional[int] = None,
    avg_profit_max: Optional[int] = None,
) -> List[CommercialDistrictOutput]:
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger = logging.getLogger(__name__)
    results: List[CommercialDistrictOutput] = []

    try:
        if connection.open:
            select_query = """
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
                WHERE 1=1
            """

            # 조건을 동적으로 추가
            params = []
            if city_id is not None:
                select_query += " AND cd.CITY_ID = %s"
                params.append(city_id)
            if district_id is not None:
                select_query += " AND cd.DISTRICT_ID = %s"
                params.append(district_id)
            if sub_district_id is not None:
                select_query += " AND cd.SUB_DISTRICT_ID = %s"
                params.append(sub_district_id)
            if biz_main_category_id is not None:
                select_query += " AND cd.BIZ_MAIN_CATEGORY_ID = %s"
                params.append(biz_main_category_id)
            if biz_sub_category_id is not None:
                select_query += " AND cd.BIZ_SUB_CATEGORY_ID = %s"
                params.append(biz_sub_category_id)
            if biz_detail_category_id is not None:
                select_query += " AND cd.BIZ_DETAIL_CATEGORY_ID = %s"
                params.append(biz_detail_category_id)
            if market_size_min is not None:
                select_query += " AND cd.MARKET_SIZE >= %s"
                params.append(market_size_min)
            if market_size_max is not None:
                select_query += " AND cd.MARKET_SIZE <= %s"
                params.append(market_size_max)
            if avg_sales_min is not None:
                select_query += " AND cd.AVERAGE_SALES >= %s"
                params.append(avg_sales_min)
            if avg_sales_max is not None:
                select_query += " AND cd.AVERAGE_SALES <= %s"
                params.append(avg_sales_max)
            if operating_cost_min is not None:
                select_query += " AND cd.OPERATING_COST >= %s"
                params.append(operating_cost_min)
            if operating_cost_max is not None:
                select_query += " AND cd.OPERATING_COST <= %s"
                params.append(operating_cost_max)
            if food_cost_min is not None:
                select_query += " AND cd.FOOD_COST >= %s"
                params.append(food_cost_min)
            if food_cost_max is not None:
                select_query += " AND cd.FOOD_COST <= %s"
                params.append(food_cost_max)
            # if employee_cost_min is not None:
            #     select_query += " AND cd.EMPLOYEE_COST >= %s"
            #     params.append(employee_cost_min)
            # if employee_cost_max is not None:
            #     select_query += " AND cd.EMPLOYEE_COST <= %s"
            #     params.append(employee_cost_max)
            if employee_cost_min is not None:  # 인건비X -> 평균 결제
                select_query += " AND cd.AVERAGE_PAYMENT >= %s"
                params.append(employee_cost_min)
            if employee_cost_max is not None:  # 인건비X -> 평균 결제
                select_query += " AND cd.AVERAGE_PAYMENT <= %s"
                params.append(employee_cost_max)
            if rental_cost_min is not None:
                select_query += " AND cd.RENTAL_COST >= %s"
                params.append(rental_cost_min)
            if rental_cost_max is not None:
                select_query += " AND cd.RENTAL_COST <= %s"
                params.append(rental_cost_max)
            if avg_profit_min is not None:
                select_query += " AND cd.AVERAGE_PROFIT >= %s"
                params.append(avg_profit_min)
            if avg_profit_max is not None:
                select_query += " AND cd.AVERAGE_PROFIT <= %s"
                params.append(avg_profit_max)

            select_query += " ORDER BY cd.COMMERCIAL_DISTRICT_ID DESC"

            cursor.execute(select_query, tuple(params))

            rows = cursor.fetchall()

            for row in rows:
                commercial_district_output = CommercialDistrictOutput(
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
                    average_payment=row.get("AVERAGE_PAYMENT"),
                    usage_count=row.get("USAGE_COUNT"),
                    average_sales=row.get("AVERAGE_SALES"),
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
                    created_at=row.get("CREATED_AT"),
                    updated_at=row.get("UPDATED_AT"),
                )
                # print(row)
                results.append(commercial_district_output)

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


# if __name__ == "__main__":
