from app.db.connect import *

def get_all_region_id():
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
            SELECT 
                   city.city_name AS city_name, 
                   city.city_id AS city_id, 
                   district.district_name AS district_name, 
                   district.district_id AS district_id, 
                   sub_district.sub_district_name AS sub_district_name,
                   sub_district.sub_district_id AS sub_district_id
            FROM sub_district
            JOIN city ON sub_district.city_id = city.city_id
            JOIN district ON sub_district.district_id = district.district_id
        """

        cursor.execute(query)
        result = cursor.fetchall()

        # city_id, district_id 쌍을 반환
        return result

    finally:
        if cursor:
            close_cursor(cursor)
        if connection:
            close_connection(connection)



def fetch_keywords_from_db():
    """DB에서 지역 목록을 가져와 city_id, district_id, sub_district_id와 합쳐진 지역명을 포함한 리스트 생성"""
    connection = get_db_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    try:
        # 지역 목록을 DB에서 조회
        query = """
            SELECT city.city_name, city.city_id,
                   district.district_name, district.district_id,
                   sub_district.sub_district_name, sub_district.sub_district_id
            FROM city
            JOIN district ON city.city_id = district.city_id
            JOIN sub_district ON district.district_id = sub_district.district_id
        """
        cursor.execute(query)
        all_region_list = cursor.fetchall()

        # 키워드 리스트 생성
        keywords = []
        for region in all_region_list:
            # 지역명을 합치기
            region_name = f"{region['city_name']} {region['district_name']} {region['sub_district_name']}"
            keywords.append({
                'city_id': region['city_id'],
                'district_id': region['district_id'],
                'sub_district_id': region['sub_district_id'],
                'keyword': region_name
            })

    finally:
        close_connection(connection)
    # print(keywords)
    return keywords


if __name__=="__main__":
    fetch_keywords_from_db()