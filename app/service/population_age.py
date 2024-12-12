from app.crud.sub_district import select_all_region_id
from app.crud.population_age import (
    select_pop_age_by_region,
    insert_pop_age_by_region
)
from datetime import date
from app.db.connect import get_db_connection


# 동 별 인구 연령별 조회 후 합산 후 인서트
def fetch_population_by_age_and_insert(ref_date: date):
    # 1. city_id, district_id, sub_district_id 값 리스트로 조회
    connection = get_db_connection()
    region_id_list = select_all_region_id(connection)
    
    # 모든 지역의 인구 수 연령별 데이터를 저장할 리스트 초기화
    all_pop_age_by_region = []
    
    # 2. 각 지역에 해당하는 인구 수 연령별 조회
    for region in region_id_list:
        pop_age_by_region_list = select_pop_age_by_region(
            city_id=region.city_id, 
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date = ref_date
        )
        # 조회된 데이터를 리스트에 추가
        all_pop_age_by_region.extend(pop_age_by_region_list)
    
    # 3. 각 지역에 해당하는 인구 수 연령 인서트
    insert_pop_age_by_region(all_pop_age_by_region)
    
    


if __name__ == "__main__":  
    fetch_population_by_age_and_insert('2024-11-30')