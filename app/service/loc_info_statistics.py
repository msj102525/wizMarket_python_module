from app.crud.sub_district import (
    select_all_region_id,
    select_city_id_sub_district_id,
    select_district_id_sub_district_id
)
from app.crud.loc_info_statistics import (
    select_nation_loc_info_by_region,
    select_city_loc_info_by_region,
    select_district_loc_info_by_region,
    insert_loc_info_statistics,
    select_loc_info_j_score_rank,
    select_mz_j_score_rank,
    select_loc_info_j_score_per,
    select_mz_j_score_per,
    insert_loc_info_statistics_avg_j_score
)
from app.db.connect import (
    get_db_connection,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from collections import defaultdict


############################ 병렬 처리 ###########################################


from concurrent.futures import ThreadPoolExecutor, as_completed

def insert_by_date():
    date_list = ['2024-08-01']
    target_list = ['shop', 'move_pop', 'sales', 'work_pop', 'income', 'spend', 'house', 'resident']

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_loc_info_and_insert, ref_date, target_item)
            for ref_date in date_list for target_item in target_list
        ]

        print("Tasks submitted")  # 스레드 제출 확인

        for future in as_completed(futures):
            try:
                future.result()  # 각 작업의 결과 확인
            except Exception as e:
                print(f"Error during task execution: {e}")

def fetch_loc_info_and_insert(ref_date, target_item):

    connection = get_db_connection()

    try:
        # 전국 범위 처리
        process_nationwide(connection, ref_date, target_item)
        
        # 시/도 범위 처리
        process_city(connection, ref_date, target_item)
        
        # 시/군/구 범위 처리
        process_district(connection, ref_date, target_item)
    finally:
        connection.close()  # 작업이 끝난 후 연결을 닫음

def process_nationwide(connection, ref_date, target_item):
    region_id_list = select_all_region_id(connection)

    nation_loc_info_by_region = []
    for region in region_id_list:
        nation_loc_info_by_region_list = select_nation_loc_info_by_region(
            connection,
            city_id=region.city_id,
            district_id=region.district_id,
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item=target_item
        )
        nation_loc_info_by_region.extend(nation_loc_info_by_region_list)

    # null 값 제외 후 통계 계산
    nation_loc_info_values = [item.target_item for item in nation_loc_info_by_region]
    filtered_values = [value for value in nation_loc_info_values if value is not None]
    statistics = calculate_statistics(filtered_values)

    # J-Score 및 통계값 추가된 데이터를 가져옴
    updated_nation_info = process_j_score(nation_loc_info_by_region, filtered_values, '전국', target_item, statistics)
    print("Nationwide:", updated_nation_info[:3])  # 데이터 일부만 출력하여 확인

    insert_loc_info_statistics(connection, updated_nation_info)  # 인서트 주석 처리


def process_city(connection, ref_date, target_item):
    city_id_sub_district_id_list = select_city_id_sub_district_id(connection)

    city_loc_info_by_region = []
    for region in city_id_sub_district_id_list:
        city_loc_info_by_region_list = select_city_loc_info_by_region(
            connection,
            city_id=region.city_id,
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item=target_item
        )
        city_loc_info_by_region.extend(city_loc_info_by_region_list)

    # null 값 제외 후 통계 계산
    city_grouped_loc_info = defaultdict(list)
    for item in city_loc_info_by_region:
        city_grouped_loc_info[item.city_id].append(item.target_item)

    updated_city_info = []
    for city_id, loc_info_values in city_grouped_loc_info.items():
        filtered_values = [value for value in loc_info_values if value is not None]
        statistics = calculate_statistics(filtered_values)

        # 원본 데이터의 각 항목을 전달하여 통계 및 J-Score 계산
        region_info_for_city = [i for i in city_loc_info_by_region if i.city_id == city_id]
        updated_city_info.extend(process_j_score(region_info_for_city, filtered_values, '시/도', target_item, statistics))

    for item in updated_city_info:
        item['district_id'] = None

    print("City Level:", updated_city_info[:3])  # 데이터 일부만 출력하여 확인

    insert_loc_info_statistics(connection, updated_city_info)  # 인서트 주석 처리


def process_district(connection, ref_date, target_item):
    district_id_sub_district_id_list = select_district_id_sub_district_id(connection)

    district_loc_info_by_region = []
    for region in district_id_sub_district_id_list:
        district_loc_info_by_region_list = select_district_loc_info_by_region(
            connection,
            district_id=region.district_id,
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item=target_item
        )
        district_loc_info_by_region.extend(district_loc_info_by_region_list)

    # null 값 제외 후 통계 계산
    district_grouped_loc_info = defaultdict(list)
    for item in district_loc_info_by_region:
        district_grouped_loc_info[item.district_id].append(item.target_item)

    updated_district_info = []
    for district_id, loc_info_values in district_grouped_loc_info.items():
        filtered_values = [value for value in loc_info_values if value is not None]
        statistics = calculate_statistics(filtered_values)

        # 원본 데이터의 각 항목을 전달하여 통계 및 J-Score 계산
        region_info_for_district = [i for i in district_loc_info_by_region if i.district_id == district_id]
        updated_district_info.extend(process_j_score(region_info_for_district, filtered_values, '시/군/구', target_item, statistics))

    for item in updated_district_info:
        item['city_id'] = None

    print("District Level:", updated_district_info[:3])  # 데이터 일부만 출력하여 확인

    insert_loc_info_statistics(connection, updated_district_info)  # 인서트 주석 처리


def process_j_score(region_info, filtered_values, stat_level, target_item, statistics):
    # None 값을 제외한 max_value 계산
    max_value = max(filtered_values) if filtered_values else 0
    # None 값을 제외한 정렬된 리스트 생성
    loc_info_values_sorted = sorted(filtered_values, reverse=True)

    updated_region_info = []  # 통계 및 J-Score가 포함된 새 리스트

    for item in region_info:
        target_value = item.target_item

        # target_value가 None이면 j_score_rank와 j_score_per도 None
        if target_value is None:
            j_score_rank = None
            j_score_per = None
        else:
            # target_value가 None이 아닌 경우 j_score 계산
            if target_value > 0:
                # target_value가 정렬된 리스트에 있는지 확인 후 계산
                if target_value in loc_info_values_sorted:
                    rank = loc_info_values_sorted.index(target_value) + 1
                    totals = len(loc_info_values_sorted)
                    j_score_rank = 10 * ((totals + 1 - rank) / totals)
                else:
                    print("Value not found:", target_value)  # 값이 없는 경우 출력
                    j_score_rank = None
                # max_value가 0보다 클 때 j_score_per 계산
                j_score_per = (target_value / max_value) * 10 if max_value > 0 else 0
            else:
                j_score_rank = 0
                j_score_per = 0

        item_dict = item.__dict__.copy()
        item_dict["j_score_rank"] = j_score_rank
        item_dict["j_score_per"] = j_score_per
        item_dict['stat_level'] = stat_level
        item_dict['target_item'] = target_item

        # 통계 값을 추가
        item_dict["avg_val"] = statistics["average"]
        item_dict["med_val"] = statistics["median"]
        item_dict["std_val"] = statistics["stddev"]
        item_dict["max_val"] = statistics["max"]
        item_dict["min_val"] = statistics["min"]

        # 새 리스트에 통계 및 J-Score가 포함된 항목 추가
        updated_region_info.append(item_dict)

    return updated_region_info



#################### 가중치 병렬 처리 #################
def fetch_and_weight_j_score(connection, region, target_item, weight, ref_date, is_mz=False):
    """J-Score 데이터를 조회하고 가중치를 적용하는 함수"""
    if not is_mz:
        # loc_info에서 j_score 데이터 가져오기
        j_score_per_list = select_loc_info_j_score_per(
            connection, region.city_id, region.district_id, region.sub_district_id, target_item, ref_date
        )
        j_score_rank_list = select_loc_info_j_score_rank(
            connection, region.city_id, region.district_id, region.sub_district_id, target_item, ref_date
        )
    else:
        # mz에서 j_score 데이터 가져오기
        j_score_per_list = select_mz_j_score_per(connection, region.city_id, region.district_id, region.sub_district_id, ref_date)
        j_score_rank_list = select_mz_j_score_rank(connection, region.city_id, region.district_id, region.sub_district_id, ref_date)

    # Null 확인 후 가중치 적용
    weighted_j_score_per = [(j.j_score_per * weight) if j.j_score_per is not None else None for j in j_score_per_list]
    weighted_j_score_rank = [(j.j_score_rank * weight) if j.j_score_rank is not None else None for j in j_score_rank_list]

    return weighted_j_score_per, weighted_j_score_rank

def calculate_weighted_j_scores(connection, target_items, ref_date):
    weights = [1, 2.5, 1.5, 1.5, 1.5, 1.5, 1, 1]  # 각 타겟 아이템별 가중치
    region_id_list = select_all_region_id(connection)
    region_scores = { (region.city_id, region.district_id, region.sub_district_id): {'j_score_per': [], 'j_score_rank': []} for region in region_id_list }

    # loc_info j_score 데이터 병렬 처리
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_target_item = {
            executor.submit(fetch_and_weight_j_score, connection, region, target_item, weights[i], ref_date): (region, target_item)
            for i, target_item in enumerate(target_items)
            for region in region_id_list
        }

        for future in as_completed(future_to_target_item):
            region, target_item = future_to_target_item[future]
            weighted_j_score_per, weighted_j_score_rank = future.result()
            region_key = (region.city_id, region.district_id, region.sub_district_id)
            region_scores[region_key]['j_score_per'].extend(weighted_j_score_per)
            region_scores[region_key]['j_score_rank'].extend(weighted_j_score_rank)

    # mz j_score 데이터 병렬 처리 없이 가중치 적용
    for region in region_id_list:
        weighted_mz_j_score_per, weighted_mz_j_score_rank = fetch_and_weight_j_score(connection, region, target_item=None, weight=1.5, ref_date=ref_date, is_mz=True)
        region_key = (region.city_id, region.district_id, region.sub_district_id)
        region_scores[region_key]['j_score_per'].extend(weighted_mz_j_score_per)
        region_scores[region_key]['j_score_rank'].extend(weighted_mz_j_score_rank)

    # 동별 평균 계산
    final_j_score_per = {}
    final_j_score_rank = {}

    for region_key, scores in region_scores.items():
        # None 값을 제외하고 평균 계산
        valid_j_score_per = [score for score in scores['j_score_per'] if score is not None]
        valid_j_score_rank = [score for score in scores['j_score_rank'] if score is not None]
        
        final_j_score_per[region_key] = sum(valid_j_score_per) / len(valid_j_score_per) if valid_j_score_per else None
        final_j_score_rank[region_key] = sum(valid_j_score_rank) / len(valid_j_score_rank) if valid_j_score_rank else None

    # 1. j_score_per 조정
    max_j_score_per = max([score for score in final_j_score_per.values() if score is not None], default=1)

    adjusted_j_score_per = {
        region_key: (score / max_j_score_per) * 10 if score is not None else None
        for region_key, score in final_j_score_per.items()
    }

    # 2. j_score_rank 조정
    all_rank_values = [score for score in final_j_score_rank.values() if score is not None]
    all_rank_values_sorted = sorted(all_rank_values, reverse=True)

    adjusted_j_score_rank = {}
    for region_key, score in final_j_score_rank.items():
        if score is None:
            adjusted_j_score_rank[region_key] = None
        else:
            rank = all_rank_values_sorted.index(score) + 1
            totals = len(all_rank_values_sorted)
            adjusted_j_score_rank[region_key] = 10 * ((totals + 1 - rank) / totals)


    insert_data = prepare_insert_data(adjusted_j_score_per, adjusted_j_score_rank)

    insert_loc_info_statistics_avg_j_score(insert_data)

    return final_j_score_per, final_j_score_rank



# 데이터 병합 및 인서트 리스트 생성
def prepare_insert_data(adjusted_j_score_per, adjusted_j_score_rank):
    insert_data = []
    
    for region_key in adjusted_j_score_per.keys():
        # region_key는 (city_id, district_id, sub_district_id)의 튜플로 가정
        city_id, district_id, sub_district_id = region_key
        j_score_per = adjusted_j_score_per.get(region_key, None)
        j_score_rank = adjusted_j_score_rank.get(region_key, None)
        
        # 인서트할 각 행 생성
        insert_row = {
            'city_id': city_id,
            'district_id': district_id,
            'sub_district_id': sub_district_id,
            'j_score_rank': j_score_rank,
            'j_score_per': j_score_per
        }
        insert_data.append(insert_row)

    return insert_data




def execute_calculate_weighted_j_scores():
    target_items = ['shop', 'move_pop', 'sales', 'work_pop', 'income', 'spend', 'house', 'resident']
    ref_date = '2024-08-01'

    # 데이터베이스 연결 설정
    connection = get_db_connection()

    # 최종 가중치 적용 J-Score 계산
    final_j_score_per, final_j_score_rank = calculate_weighted_j_scores(connection, target_items, ref_date)

    # 데이터베이스 연결 해제
    connection.close()

    return final_j_score_per, final_j_score_rank





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
    # insert_by_date()

    execute_calculate_weighted_j_scores()