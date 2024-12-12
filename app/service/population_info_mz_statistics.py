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
from app.db.connect import *

def insert_by_date():
    date_list = ['2024-11-01']
    connection = get_db_connection()
    for date_str in date_list:
        # 날짜를 각 반복 시마다 fetch_mz_population_and_insert 함수에 전달
        print(date_str, '시작')
        fetch_mz_population_and_insert_j_score_rank(date_str, connection)
        print(date_str, '끝')


def fetch_mz_population_and_insert_j_score_rank(ref_date, connection):
    # # 전국 범위
    # 1. city_id, district_id, sub_district_id 값 리스트로 조회
    region_id_list = select_all_region_id(connection)

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

    # 4. 이상치 제거 - 내림차순 정렬 후, 차이가 표준편차보다 크다면 제거
    stddev = statistics["stddev"]
    sorted_values = sorted(nation_mz_pop_values, reverse=True)
    outlier_count = int(len(nation_mz_pop_values) * 0.002)  # 0.2% 계산

    filtered_values = sorted_values[:]
    for idx in range(outlier_count):
        if len(filtered_values) > 1 and (filtered_values[0] - filtered_values[1]) > stddev:
            print(f"{idx}번: {filtered_values[0]} 제거, {idx + 1}번: {filtered_values[1]} 유지")
            print(f"각각의 차이: {filtered_values[0] - filtered_values[1]}")
            filtered_values.pop(0)
        else:
            break

    # 제거된 이상치 처리 - 제거된 값은 10점으로 처리
    outlier_values = set(nation_mz_pop_values) - set(filtered_values)

    # 5. J-Score-Rank 계산
    for item in nation_mz_pop_by_region:
        mz_pop = item["mz_pop"]
        if mz_pop in outlier_values:
            item["j_score_rank_non_outliers"] = 10  # 이상치는 10점 처리
            item["j_score_per_non_outliers"] = 10
        else:
            # j_score_rank_non_outliers 계산
            rank = filtered_values.index(mz_pop) + 1
            totals = len(filtered_values)
            j_score_rank_non_outliers = 10 * ((totals + 1 - rank) / totals)
            item["j_score_rank_non_outliers"] = j_score_rank_non_outliers
            
            # j_score_per_non_outliers 계산
            max_mz_pop_filtered = max(filtered_values) if filtered_values else 1
            j_score_per_non_outliers = (mz_pop / max_mz_pop_filtered) * 10 if mz_pop > 0 else 0
            item["j_score_per_non_outliers"] = j_score_per_non_outliers

        # 기존 j_score_rank, j_score_per도 유지
        nation_mz_pop_values_rank = sorted(nation_mz_pop_values, reverse=True)
        if mz_pop > 0:
            rank = nation_mz_pop_values_rank.index(mz_pop) + 1
            totals = len(nation_mz_pop_values)
            j_score_rank = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score_rank = 0  # mz_pop이 0인 경우 j_score도 0

        item["j_score_rank"] = j_score_rank

        max_mz_pop = max(nation_mz_pop_values)
        j_score_per = (mz_pop / max_mz_pop) * 10 if mz_pop > 0 else 0
        item["j_score_per"] = j_score_per

        # 최종 j_score 및 j_score_non_outliers 계산
        item['j_score'] = (item["j_score_rank"] + item["j_score_per"]) / 2 if item["j_score_rank"] > 0 and item["j_score_per"] > 0 else 0
        item['j_score_non_outliers'] = (item["j_score_rank_non_outliers"] + item["j_score_per_non_outliers"]) / 2 if item["j_score_rank_non_outliers"] > 0 and item["j_score_per_non_outliers"] > 0 else 0
        item['stat_level'] = '전국'
        del item['mz_pop']

    # 인서트용 이름만 바꾸기
    nation_mz_pop_for_insert = nation_mz_pop_by_region
    insert_mz_population_statistics(nation_mz_pop_for_insert)

    # 시/도 범위
    # 1. city_id, sub_district_id 값 리스트로 조회
    city_id_sub_district_id_list = select_city_id_sub_district_id(connection)

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    city_mz_pop_by_region = []

    for region in city_id_sub_district_id_list:
        city_mz_pop_age_by_region_list = select_city_mz_pop_by_region(
            city_id=region.city_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date
        )
        city_mz_pop_by_region.extend(city_mz_pop_age_by_region_list)

    # Pydantic 객체를 딕셔너리로 변환하여 작업
    city_mz_pop_dicts = [item.dict() for item in city_mz_pop_by_region]

    # city_id 기준으로 mz_pop 값을 그룹화
    city_grouped_mz_pop = defaultdict(list)
    for item in city_mz_pop_dicts:
        city_grouped_mz_pop[item['city_id']].append(item['mz_pop'])

    # 3. 각 city_id 그룹 내에서 j_score_rank 계산 및 통계 계산
    for city_id, mz_pop_list in city_grouped_mz_pop.items():
        # 통계 계산
        statistics = calculate_statistics(mz_pop_list)
        
        # 이상치 제거 작업 수행
        stddev = statistics["stddev"]
        sorted_values = sorted(mz_pop_list, reverse=True)
        outlier_count = int(len(mz_pop_list) * 0.002)  # 0.2% 계산

        # 0.2%만큼 반복하며 이상치 제거
        filtered_values = sorted_values[:]
        for _ in range(outlier_count):
            if len(filtered_values) > 1 and (filtered_values[0] - filtered_values[1]) > stddev:
                filtered_values.pop(0)
            else:
                break

        # 이상치로 간주된 값들 (10점 처리)
        outlier_values = set(mz_pop_list) - set(filtered_values)

        # 각 항목에 대해 j_score_rank 계산 및 통계 값을 추가
        mz_pop_list_sorted = sorted(mz_pop_list, reverse=True)  # 원본 리스트 정렬 (내림차순)
        
        for item in city_mz_pop_dicts:
            if item['city_id'] == city_id:
                mz_pop = item['mz_pop']
                
                # j_score_rank 계산
                if mz_pop > 0:
                    rank = mz_pop_list_sorted.index(mz_pop) + 1
                    totals = len(mz_pop_list_sorted)
                    j_score_rank = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score_rank = 0
                item['j_score_rank'] = j_score_rank

                # j_score_rank_non_outliers 및 j_score_per_non_outliers 계산
                if mz_pop in outlier_values:
                    item["j_score_rank_non_outliers"] = 10  # 이상치는 10점으로 처리
                    item["j_score_per_non_outliers"] = 10
                else:
                    rank_non_outliers = filtered_values.index(mz_pop) + 1
                    totals_non_outliers = len(filtered_values)
                    j_score_rank_non_outliers = 10 * ((totals_non_outliers + 1 - rank_non_outliers) / totals_non_outliers)
                    item["j_score_rank_non_outliers"] = j_score_rank_non_outliers

                    max_mz_pop_filtered = max(filtered_values) if filtered_values else 1
                    j_score_per_non_outliers = (mz_pop / max_mz_pop_filtered) * 10 if mz_pop > 0 else 0
                    item["j_score_per_non_outliers"] = j_score_per_non_outliers

                # 통계 값 추가
                item['avg_val'] = statistics['average']
                item['med_val'] = statistics['median']
                item['std_val'] = statistics['stddev']
                item['max_val'] = statistics['max']
                item['min_val'] = statistics['min']

    # 4. j_score_per 계산
    for city_id, mz_pop_list in city_grouped_mz_pop.items():
        max_mz_pop = max(mz_pop_list)  # 해당 city_id에서 mz_pop 최대값
        for item in city_mz_pop_dicts:
            if item['city_id'] == city_id:
                mz_pop = item['mz_pop']
                if mz_pop > 0 and max_mz_pop > 0:
                    j_score_per = (mz_pop / max_mz_pop) * 10
                else:
                    j_score_per = 0
                item['j_score_per'] = j_score_per

    # 5. 각 항목에 필요한 필드 추가 및 mz_pop 제거
    for item in city_mz_pop_dicts:
        item['j_score'] = (item["j_score_rank"] + item["j_score_per"]) / 2 if item["j_score_rank"] > 0 and item["j_score_per"] > 0 else 0
        item['j_score_non_outliers'] = (item["j_score_rank_non_outliers"] + item["j_score_per_non_outliers"]) / 2 if item["j_score_rank_non_outliers"] > 0 and item["j_score_per_non_outliers"] > 0 else 0
        item['district_id'] = None
        item['stat_level'] = '시/도'
        
        # mz_pop 필드 제거
        if 'mz_pop' in item:
            del item['mz_pop']

    # 인서트용 이름만 바꾸기
    city_mz_pop_for_insert = city_mz_pop_dicts
    insert_mz_population_statistics(city_mz_pop_for_insert)

    # 시/군/구 범위
    # 1. district_id, sub_district_id 값 리스트로 조회
    district_id_sub_district_id_list = select_district_id_sub_district_id(connection)

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    district_mz_pop_by_region = []

    for region in district_id_sub_district_id_list:
        district_mz_pop_age_by_region_list = select_district_mz_pop_by_region(
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date
        )
        district_mz_pop_by_region.extend(district_mz_pop_age_by_region_list)

    # Pydantic 객체를 딕셔너리로 변환하여 작업
    district_mz_pop_dicts = [item.dict() for item in district_mz_pop_by_region]

    # district_id 기준으로 mz_pop 값을 그룹화
    district_grouped_mz_pop = defaultdict(list)
    for item in district_mz_pop_dicts:
        district_grouped_mz_pop[item['district_id']].append(item['mz_pop'])

    # 3. 각 district_id 그룹 내에서 j_score_rank 계산 및 통계 계산
    for district_id, mz_pop_list in district_grouped_mz_pop.items():
        # 통계 계산
        statistics = calculate_statistics(mz_pop_list)

        # 이상치 제거 조건 설정
        stddev = statistics["stddev"]
        sorted_values = sorted(mz_pop_list, reverse=True)
        outlier_count = max(int(len(mz_pop_list) * 0.002), 2)  # 최소 2개 이상치 처리
        
        # 0.2% 또는 최소 2개만큼 반복하여 이상치 제거
        filtered_values = sorted_values[:]
        for _ in range(outlier_count):
            if len(filtered_values) > 1 and (filtered_values[0] - filtered_values[1]) > stddev:
                filtered_values.pop(0)
            else:
                break

        # 이상치로 간주된 값들
        outlier_values = set(mz_pop_list) - set(filtered_values)

        # 각 항목에 대해 j_score_rank 계산 및 통계 값을 추가
        mz_pop_list_sorted = sorted(mz_pop_list, reverse=True)

        for item in district_mz_pop_dicts:
            if item['district_id'] == district_id:
                mz_pop = item['mz_pop']

                # 기존 j_score_rank 계산
                if mz_pop > 0:
                    rank = mz_pop_list_sorted.index(mz_pop) + 1
                    totals = len(mz_pop_list_sorted)
                    j_score_rank = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score_rank = 0
                item['j_score_rank'] = j_score_rank

                # j_score_rank_non_outliers 및 j_score_per_non_outliers 계산
                if mz_pop in outlier_values:
                    item["j_score_rank_non_outliers"] = 10  # 이상치는 10점으로 처리
                    item["j_score_per_non_outliers"] = 10
                else:
                    rank_non_outliers = filtered_values.index(mz_pop) + 1
                    totals_non_outliers = len(filtered_values)
                    j_score_rank_non_outliers = 10 * ((totals_non_outliers + 1 - rank_non_outliers) / totals_non_outliers)
                    item["j_score_rank_non_outliers"] = j_score_rank_non_outliers

                    max_mz_pop_filtered = max(filtered_values) if filtered_values else 1
                    j_score_per_non_outliers = (mz_pop / max_mz_pop_filtered) * 10 if mz_pop > 0 else 0
                    item["j_score_per_non_outliers"] = j_score_per_non_outliers

                # 통계 값 추가
                item['avg_val'] = statistics['average']
                item['med_val'] = statistics['median']
                item['std_val'] = statistics['stddev']
                item['max_val'] = statistics['max']
                item['min_val'] = statistics['min']

    # 4. j_score_per 계산
    for district_id, mz_pop_list in district_grouped_mz_pop.items():
        max_mz_pop = max(mz_pop_list)

        for item in district_mz_pop_dicts:
            if item['district_id'] == district_id:
                mz_pop = item['mz_pop']
                if mz_pop > 0 and max_mz_pop > 0:
                    j_score_per = (mz_pop / max_mz_pop) * 10
                else:
                    j_score_per = 0
                item['j_score_per'] = j_score_per

    # 5. 각 항목에 stat_level 및 필요한 필드 추가, mz_pop 제거
    for item in district_mz_pop_dicts:
        # 필요한 필드 추가
        item['j_score'] = (item["j_score_rank"] + item["j_score_per"]) / 2 if item["j_score_rank"] > 0 and item["j_score_per"] > 0 else 0
        item['j_score_non_outliers'] = (item["j_score_rank_non_outliers"] + item["j_score_per_non_outliers"]) / 2 if item["j_score_rank_non_outliers"] > 0 and item["j_score_per_non_outliers"] > 0 else 0
        item['city_id'] = None
        item['stat_level'] = '시/군/구'

        # mz_pop 필드 제거
        if 'mz_pop' in item:
            del item['mz_pop']

    # 인서트용 이름만 바꾸기
    district_mz_pop_for_insert = district_mz_pop_dicts
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