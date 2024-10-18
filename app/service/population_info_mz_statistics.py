from app.crud.sub_district import (
    select_all_region_id,
    select_city_id_sub_district_id,
    select_district_id_sub_district_id
)
from app.crud.population_info_mz_statistics import (
    select_nation_mz_pop_by_region,
    select_city_mz_pop_by_region,
    select_district_mz_pop_by_region,
    insert_mz_population_statistics
)
import numpy as np
from collections import defaultdict

def insert_by_date():
    date_list = ['2024-01-31', '2024-02-29', '2024-03-31', '2024-04-30', '2024-05-31', '2024-06-30', '2024-07-31', '2024-08-31', '2024-09-30']
    for date_str in date_list:
        # 날짜를 각 반복 시마다 fetch_mz_population_and_insert 함수에 전달
        print(date_str, '시작')
        fetch_mz_population_and_insert(date_str)
        print(date_str, '끝')


def fetch_mz_population_and_insert(ref_date):
    # 전국 범위
    # 1. city_id, district_id, sub_district_id 값 리스트로 조회
    region_id_list = select_all_region_id()

    # 2.각 지역에 해당하는 mz 세대 인구 수 값 조회
    # mz 는 1995~2010년 생 : 만 14~29세
    nation_mz_pop_by_region = []

    for region in region_id_list:
        nation_mz_pop_by_region_list = select_nation_mz_pop_by_region(
            city_id=region.city_id, 
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date
        )
        # 조회된 데이터를 리스트에 추가
        nation_mz_pop_by_region.extend(nation_mz_pop_by_region_list)
    
    # 통계 계산 및 j_score 계산 을 위해 mz_pop 값만 추출하여 리스트 생성
    nation_mz_pop_values = [item.mz_pop for item in nation_mz_pop_by_region]

    # 3. 통계 계산
    statistics = calculate_statistics(nation_mz_pop_values)

    # mz_pop_age_by_region 리스트의 각 항목에 통계 값을 개별 키-값 쌍으로 추가
    nation_mz_pop_by_region = [
        {**item.__dict__, "avg_val": statistics["average"], "med_val": statistics["median"],
        "std_val": statistics["stddev"], "max_val": statistics["max"], "min_val": statistics["min"]}
        for item in nation_mz_pop_by_region
    ]

    # 4. J-Score 계산
    nation_mz_pop_values_rank = sorted(nation_mz_pop_values, reverse=True)  # 내림차순으로 정렬

    for item in nation_mz_pop_by_region:
        mz_pop = item["mz_pop"]
        if mz_pop > 0:
            rank = nation_mz_pop_values_rank.index(mz_pop) + 1
            totals = len(nation_mz_pop_values)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # mz_pop이 0인 경우 j_score도 0

        # j_score를 각 항목에 추가
        item["j_score"] = j_score

    for item in nation_mz_pop_by_region:
        item['stat_level'] = '전국'
        del item['mz_pop']

    # 인서트용 이름만 바꾸기
    nation_mz_pop_for_insert = nation_mz_pop_by_region
    # 인서트
    print(len(nation_mz_pop_for_insert), ref_date)
    insert_mz_population_statistics(nation_mz_pop_for_insert)

    # 시/도 범위
    # 1. city_id, sub_district_id 값 리스트로 조회
    city_id_sub_district_id_list = select_city_id_sub_district_id()

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    city_mz_pop_by_region = []

    for region in city_id_sub_district_id_list:
        city_mz_pop_age_by_region_list = select_city_mz_pop_by_region(
            city_id=region.city_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date
        )
        # 조회된 데이터를 리스트에 추가
        city_mz_pop_by_region.extend(city_mz_pop_age_by_region_list)

    # Pydantic 객체를 딕셔너리로 변환하여 작업
    city_mz_pop_dicts = [item.dict() for item in city_mz_pop_by_region]

    # city_id 기준으로 mz_pop 값을 그룹화
    from collections import defaultdict

    city_grouped_mz_pop = defaultdict(list)
    for item in city_mz_pop_dicts:
        city_grouped_mz_pop[item['city_id']].append(item['mz_pop'])

    # 각 city_id 그룹 내에서 j_score 계산
    for city_id, mz_pop_list in city_grouped_mz_pop.items():
        mz_pop_list_sorted = sorted(mz_pop_list, reverse=True)

        # 각 항목에 대해 j_score 계산
        for item in city_mz_pop_dicts:
            if item['city_id'] == city_id:
                mz_pop = item['mz_pop']
                if mz_pop > 0:
                    rank = mz_pop_list_sorted.index(mz_pop) + 1
                    totals = len(mz_pop_list_sorted)

                    # j_score 계산
                    j_score = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score = 0

                # j_score를 추가
                item['j_score'] = j_score

    # 여기서 필드 추가 및 mz_pop 제거 작업 수행
    for item in city_mz_pop_dicts:
        # 필요한 필드 추가
        item['district_id'] = None
        item['avg_val'] = None
        item['med_val'] = None
        item['std_val'] = None
        item['max_val'] = None
        item['min_val'] = None
        item['stat_level'] = '시/도'

        # mz_pop 필드 제거
        if 'mz_pop' in item:
            del item['mz_pop']

    # 인서트용 이름만 바꾸기
    city_mz_pop_for_insert = city_mz_pop_dicts
    # 인서트
    print(len(city_mz_pop_for_insert), ref_date)
    insert_mz_population_statistics(city_mz_pop_for_insert)
    

    # 시/군/구 범위
    # 1. city_id, sub_district_id 값 리스트로 조회
    district_id_sub_district_id_list = select_district_id_sub_district_id()

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    district_mz_pop_by_region = []

    for region in district_id_sub_district_id_list:
        district_mz_pop_age_by_region_list = select_district_mz_pop_by_region(
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date
        )
        # 조회된 데이터를 리스트에 추가
        district_mz_pop_by_region.extend(district_mz_pop_age_by_region_list)

    # Pydantic 객체를 딕셔너리로 변환하여 작업
    district_mz_pop_dicts = [item.dict() for item in district_mz_pop_by_region]

    # district_id 기준으로 mz_pop 값을 그룹화
    from collections import defaultdict

    district_grouped_mz_pop = defaultdict(list)
    for item in district_mz_pop_dicts:
        district_grouped_mz_pop[item['district_id']].append(item['mz_pop'])

    # 각 district_id 그룹 내에서 j_score 계산
    for district_id, mz_pop_list in district_grouped_mz_pop.items():
        mz_pop_list_sorted = sorted(mz_pop_list, reverse=True)

        # 각 항목에 대해 j_score 계산
        for item in district_mz_pop_dicts:
            if item['district_id'] == district_id:
                mz_pop = item['mz_pop']
                if mz_pop > 0:
                    rank = mz_pop_list_sorted.index(mz_pop) + 1
                    totals = len(mz_pop_list_sorted)

                    # j_score 계산
                    j_score = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score = 0

                # j_score를 추가
                item['j_score'] = j_score

    # 여기서 필드 추가 및 mz_pop 제거 작업 수행
    for item in district_mz_pop_dicts:
        # 필요한 필드 추가
        item['city_id'] = None
        item['avg_val'] = None
        item['med_val'] = None
        item['std_val'] = None
        item['max_val'] = None
        item['min_val'] = None
        item['stat_level'] = '시/군/구'

        # mz_pop 필드 제거
        if 'mz_pop' in item:
            del item['mz_pop']

    # 인서트용 이름만 바꾸기
    district_mz_pop_for_insert = district_mz_pop_dicts
    # 인서트
    print(len(district_mz_pop_for_insert), ref_date)
    insert_mz_population_statistics(district_mz_pop_for_insert)




# 통계 계산
def calculate_statistics(data):
    """
    주어진 데이터에 대해 평균, 중위수, 표준편차, 최대값, 최소값을 계산하는 함수
    """
    if not data or len(data) == 0:
        return {
            "average": None,
            "median": None,
            "stddev": None,
            "max": None,
            "min": None,
        }

    avg_value = np.mean(data)
    median_value = np.median(data)
    stddev_value = np.std(data)
    max_value = np.max(data)
    min_value = np.min(data)

    return {
        "average": avg_value,
        "median": median_value,
        "stddev": stddev_value,
        "max": max_value,
        "min": min_value,
    }
    
    

if __name__ == "__main__":  
    insert_by_date()