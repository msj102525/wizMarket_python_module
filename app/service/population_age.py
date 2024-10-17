from app.db.connect import *
from app.crud.population_age import select_all_region_id, select_pop_age_by_region


# 동 별 인구 연령별 조회 후 합산 후 인서트
def fetch_population_by_age_and_insert():
    # city_id, district_id, sub_district_id 값 리스트로 조회
    region_id_list = select_all_region_id()
    
    # 각 지역에 해당하는 인구 수 연령별 조회
    for region in region_id_list:
        pop_age_by_region_list = select_pop_age_by_region(
            city_id=region.city_id, 
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id
        )
    
    
    print(pop_age_by_region_list)
    


if __name__ == "__main__":  
    fetch_population_by_age_and_insert()