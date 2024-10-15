from app.db.connect import *


# 조회
def get_crime_by_city_id(cursor, city_id: int, quarter: str):
    query = """
    SELECT * FROM crime
    WHERE CITY_ID = %s AND QUARTER = %s
    """
    cursor.execute(query, (city_id, quarter))
    rows = cursor.fetchall()
    
    # 튜플을 딕셔너리로 변환
    columns = ["CRIME_ID", "CITY_ID", "QUARTER", "CRIME_MAJOR_CATEGORY", "CRIME_MINOR_CATEGORY",
               "INCIDENT_COUNT", "ARREST_COUNT", "INCIDENT_TO_ARREST_RATIO", "ARREST_PERSONNEL", "LEGAL_ENTITY"]
    result = [dict(zip(columns, row)) for row in rows]
    
    return result



def insert_crime_data(connection, df, city_id, quarter):
    # "-" 값을 0으로 변환하고, NaN 값을 0으로 변환
    df.replace('-', 0, inplace=True)
    df.fillna(0, inplace=True)

    # 데이터 삽입
    with connection.cursor() as cursor:
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crime (
                    CITY_ID, QUARTER, CRIME_MAJOR_CATEGORY, CRIME_MINOR_CATEGORY, 
                    INCIDENT_COUNT, ARREST_COUNT, INCIDENT_TO_ARREST_RATIO, 
                    ARREST_PERSONNEL, LEGAL_ENTITY
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                city_id,
                quarter,
                row['MajorCategory'],
                row['MinorCategory'],
                int(row['IncidentCount']),
                int(row['ArrestCount']),
                float(row['ArrestRatio']),
                int(row['ArrestPersonnel']),
                int(row['LegalEntity'])
            ))

    # 트랜잭션 커밋
    commit(connection)
