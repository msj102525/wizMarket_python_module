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
    select_loc_info_j_score_per_non_outliers,
    select_mz_j_score_per_non_outliers,
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
    date_list = ['2024-11-01']
    target_list = ['shop', 'move_pop', 'sales', 'work_pop', 'income', 'spend', 'house', 'resident', 'apart_price']

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
    updated_nation_info = process_j_score(nation_loc_info_by_region, filtered_values, '전국', target_item, statistics, ref_date)
    # print("Nation Level:", updated_nation_info[:3])  # 데이터 일부만 출력하여 확인
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
    # print(city_loc_info_by_region[:3])
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
        updated_city_info.extend(process_j_score(region_info_for_city, filtered_values, '시/도', target_item, statistics, ref_date))

    for item in updated_city_info:
        item['district_id'] = None

    # print("City Level:", updated_city_info[:3])  # 데이터 일부만 출력하여 확인
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
    # print(district_loc_info_by_region[:3])
    # null 값 제외 후 통계 계산
    district_grouped_loc_info = defaultdict(list)
    for item in district_loc_info_by_region:
        district_grouped_loc_info[item.district_id].append(item.target_item)
    # print(district_grouped_loc_info)
    updated_district_info = []
    for district_id, loc_info_values in district_grouped_loc_info.items():
        filtered_values = [value for value in loc_info_values if value is not None]
        statistics = calculate_statistics(filtered_values)
        # print(filtered_values[:3])
        # 원본 데이터의 각 항목을 전달하여 통계 및 J-Score 계산
        region_info_for_district = [i for i in district_loc_info_by_region if i.district_id == district_id]
        updated_district_info.extend(process_j_score(region_info_for_district, filtered_values, '시/군/구', target_item, statistics, ref_date))
    
    for item in updated_district_info:
        item['city_id'] = None

    # print("District Level:", updated_district_info[:3])  # 데이터 일부만 출력하여 확인

    insert_loc_info_statistics(connection, updated_district_info)  # 인서트 주석 처리


def process_j_score(region_info, filtered_values, stat_level, target_item, statistics, ref_date):
    if not filtered_values:  # 필터링된 값이 비어 있으면 처리하지 않음
        return []
    # 이상치 제거 기준 설정: 무조건 3개로 통일
    outlier_removal_limit = 3
    # print('프로세스 J-score 전')
    # 내림차순 정렬 후 이상치 제거
    loc_info_values_sorted = sorted(filtered_values, reverse=True)
    stddev = statistics["stddev"]
    non_outliers = []

    # 이상치 제거 수행
    removed_outliers = 0
    for i in range(1, len(loc_info_values_sorted)):
        if removed_outliers >= outlier_removal_limit:
            non_outliers.extend(loc_info_values_sorted[i:])
            break
        if loc_info_values_sorted[i - 1] - loc_info_values_sorted[i] > stddev:
            removed_outliers += 1
            continue
        non_outliers.append(loc_info_values_sorted[i - 1])
    if removed_outliers < outlier_removal_limit:
        non_outliers.append(loc_info_values_sorted[-1])

    # 이상치가 제거된 데이터로 새로운 최대값 설정
    max_value_non_outliers = max(non_outliers) if non_outliers else 0

    updated_region_info = []

    for item in region_info:
        target_value = item.target_item
        item_dict = item.__dict__.copy()

        # target_value가 None인 경우 모든 J-Score 값을 None으로 설정하고 다음 항목으로
        if target_value is None:
            item_dict.update({
                "j_score_rank": None,
                "j_score_per": None,
                "j_score": None,
                "j_score_per_non_outliers": None,
                "j_score_non_outliers": None
            })
        else:
            # 기존 J-Score 계산 (rank는 이상치 제거 전 기준으로 계산)
            original_scores = calculate_j_scores(target_value, loc_info_values_sorted, max(filtered_values), stat_level, target_item)
            item_dict.update(original_scores)

            # 이상치 제거 후 J-Score per만 재계산
            if target_value in non_outliers:
                non_outlier_scores = calculate_j_scores(target_value, loc_info_values_sorted, max_value_non_outliers, stat_level, target_item, is_non_outlier=True)
                
                # non_outlier_scores에서 rank 제거하고 per 관련 필드만 추가
                item_dict['j_score_per_non_outliers'] = non_outlier_scores['j_score_per_non_outliers']
                item_dict['j_score_non_outliers'] = (original_scores['j_score_rank'] + non_outlier_scores['j_score_per_non_outliers']) / 2
                
                # 기존 값이 이상치 제거 후 per 값보다 큰 경우 출력
                if (original_scores['j_score_per'] is not None and item_dict['j_score_per_non_outliers'] is not None and
                    original_scores['j_score_per'] > item_dict['j_score_per_non_outliers']):
                    print(f"Per exceeded for {target_item}: Original Per {original_scores['j_score_per']} > Non-outlier Per {item_dict['j_score_per_non_outliers']}")

            else:
                # 이상치일 경우 10으로 설정
                item_dict['j_score_per_non_outliers'] = 10
                item_dict['j_score_non_outliers'] = 10

        # 통계 및 추가 필드 삽입
        item_dict.update({
            "avg_val": statistics["average"],
            "med_val": statistics["median"],
            "std_val": statistics["stddev"],
            "max_val": statistics["max"],
            "min_val": statistics["min"],
            "target_item": target_item,
            "stat_level": stat_level,
            "ref_date": ref_date
        })

        updated_region_info.append(item_dict)
    # print('프로세스 J-score 후')
    return updated_region_info


def calculate_j_scores(value, sorted_values, max_value, stat_level, target_item, is_non_outlier=False):
    # print('J-score 계산 전')
    if value is None:
        return {
            f'j_score_rank': None if not is_non_outlier else None,
            f'j_score_per{"_non_outliers" if is_non_outlier else ""}': None,
            f'j_score{"_non_outliers" if is_non_outlier else ""}': None,
        }

    # 순위 계산 (rank는 항상 이상치 제거 전 기준으로 계산)
    rank = sorted_values.index(value) + 1  # Rank calculation with the original sorted_values list
    j_score_rank = ((len(sorted_values) + 1 - rank) / len(sorted_values)) * 10 if value in sorted_values else None
    # print('J-score 계산 후')
    # j_score_per는 max_value가 변경될 때만 재계산 (is_non_outlier=True일 때만)
    if is_non_outlier:
        # 이상치 제거 후 per 계산
        j_score_per_non_outliers = (value / max_value) * 10 if max_value > 0 else 0
        j_score_non_outliers = (j_score_rank + j_score_per_non_outliers) / 2 if j_score_rank is not None else None
        return {
            'j_score_rank': j_score_rank,  # 원래의 rank 그대로 유지
            'j_score_per_non_outliers': j_score_per_non_outliers,
            'j_score_non_outliers': j_score_non_outliers
        }
    else:
        # 원래 per 계산
        j_score_per = (value / max_value) * 10 if max_value > 0 else 0
        j_score = (j_score_rank + j_score_per) / 2 if j_score_rank is not None else None
        return {
            'j_score_rank': j_score_rank,
            'j_score_per': j_score_per,
            'j_score': j_score
        }





#################### 가중치 병렬 처리 #################
def fetch_and_weight_j_score(connection, region, target_item, weight, ref_date, is_mz=False):
    """J-Score 데이터를 조회하고 가중치를 적용하는 함수"""
    if not is_mz:
        # loc_info에서 j_score 데이터 가져오기
        j_score_rank_list = select_loc_info_j_score_rank(
            connection, region.city_id, region.district_id, region.sub_district_id, target_item, ref_date
        )
        j_score_per_list = select_loc_info_j_score_per(
            connection, region.city_id, region.district_id, region.sub_district_id, target_item, ref_date
        )
        # 이상치 제거 후 per만 가져오기 (rank는 제외)
        j_score_per_non_outliers_list = select_loc_info_j_score_per_non_outliers(
            connection, region.city_id, region.district_id, region.sub_district_id, target_item, ref_date
        )
        # target_item이 apart_price일 경우 null 값을 0으로 대체
        if target_item == 'apart_price':
            for score in j_score_rank_list:
                if score.j_score_rank is None:
                    score.j_score_rank = 0
            for score in j_score_per_list:
                if score.j_score_per is None:
                    score.j_score_per = 0
            for score in j_score_per_non_outliers_list:
                if score.j_score_per_non_outliers is None:
                    score.j_score_per_non_outliers = 0

    else:
        # mz에서 j_score 데이터 가져오기
        j_score_rank_list = select_mz_j_score_rank(connection, region.city_id, region.district_id, region.sub_district_id, ref_date)
        j_score_per_list = select_mz_j_score_per(connection, region.city_id, region.district_id, region.sub_district_id, ref_date)
        # 이상치 제거 후 per만 가져오기 (rank는 제외)
        j_score_per_non_outliers_list = select_mz_j_score_per_non_outliers(connection, region.city_id, region.district_id, region.sub_district_id, ref_date)


    # Null 확인 후 가중치 적용
    weighted_j_score_per = [(j.j_score_per * weight) if j.j_score_per is not None else None for j in j_score_per_list]
    weighted_j_score_rank = [(j.j_score_rank * weight) if j.j_score_rank is not None else None for j in j_score_rank_list]

    # 이상치 제거 후 j_score_per에만 가중치 적용
    weighted_j_score_per_non_outliers = [(j.j_score_per_non_outliers * weight) if j.j_score_per_non_outliers is not None else None for j in j_score_per_non_outliers_list]

    return weighted_j_score_per, weighted_j_score_rank, weighted_j_score_per_non_outliers


def calculate_weighted_j_scores(connection, target_items, ref_date):
    weights = [1, 2.5, 1.5, 1.5, 1.5, 1.5, 1, 1, 1]  # 각 타겟 아이템별 가중치
    region_id_list = select_all_region_id(connection)
    
    # 기존 및 이상치 제거 후의 j_score 데이터를 저장하는 구조
    region_scores = {
        (region.city_id, region.district_id, region.sub_district_id): {
            'j_score_per': [], 'j_score_rank': [], 
            'j_score_per_non_outliers': []
        } for region in region_id_list
    }

    # loc_info j_score 데이터 병렬 처리
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_target_item = {
            executor.submit(fetch_and_weight_j_score, connection, region, target_item, weights[i], ref_date): (region, target_item)
            for i, target_item in enumerate(target_items)
            for region in region_id_list
        }

        for future in as_completed(future_to_target_item):
            region, target_item = future_to_target_item[future]
            (weighted_j_score_per, weighted_j_score_rank, weighted_j_score_per_non_outliers) = future.result()
            
            region_key = (region.city_id, region.district_id, region.sub_district_id)
            
            # 기존 j_score와 이상치 제거 후 j_score 추가
            region_scores[region_key]['j_score_per'].extend(weighted_j_score_per)
            region_scores[region_key]['j_score_rank'].extend(weighted_j_score_rank)
            region_scores[region_key]['j_score_per_non_outliers'].extend(weighted_j_score_per_non_outliers)

    # mz j_score 데이터 병렬 처리 없이 가중치 적용
    for region in region_id_list:
        (weighted_mz_j_score_per, weighted_mz_j_score_rank, weighted_mz_j_score_per_non_outliers) = fetch_and_weight_j_score(
            connection, region, target_item=None, weight=1.5, ref_date=ref_date, is_mz=True
        )
        
        region_key = (region.city_id, region.district_id, region.sub_district_id)
        
        region_scores[region_key]['j_score_per'].extend(weighted_mz_j_score_per)
        region_scores[region_key]['j_score_rank'].extend(weighted_mz_j_score_rank)
        region_scores[region_key]['j_score_per_non_outliers'].extend(weighted_mz_j_score_per_non_outliers)

    # 동별 평균 계산
    final_j_score_per = {}
    final_j_score_rank = {}
    final_j_score_per_non_outliers = {}

    for region_key, scores in region_scores.items():
        # 기존 j_score 평균 계산 - 모든 항목에 유효한 값이 있어야 평균 계산, 하나라도 None이면 None으로 설정
        if all(score is not None for score in scores['j_score_per']) and scores['j_score_per']:
            valid_j_score_per = [score for score in scores['j_score_per']]
            final_j_score_per[region_key] = sum(valid_j_score_per) / len(valid_j_score_per)
        else:
            final_j_score_per[region_key] = None

        if all(score is not None for score in scores['j_score_rank']) and scores['j_score_rank']:
            valid_j_score_rank = [score for score in scores['j_score_rank']]
            final_j_score_rank[region_key] = sum(valid_j_score_rank) / len(valid_j_score_rank)
        else:
            final_j_score_rank[region_key] = None

        # 이상치 제거된 j_score_per 평균 계산
        if all(score is not None for score in scores['j_score_per_non_outliers']) and scores['j_score_per_non_outliers']:
            valid_j_score_per_non_outliers = [score for score in scores['j_score_per_non_outliers']]
            final_j_score_per_non_outliers[region_key] = sum(valid_j_score_per_non_outliers) / len(valid_j_score_per_non_outliers)
        else:
            final_j_score_per_non_outliers[region_key] = None

    # j_score_per 조정
    max_j_score_per = max([score for score in final_j_score_per.values() if score is not None], default=1)
    max_j_score_per_non_outliers = max([score for score in final_j_score_per_non_outliers.values() if score is not None], default=1)
    adjusted_j_score_per = {
        region_key: (score / max_j_score_per) * 10 if score is not None else None
        for region_key, score in final_j_score_per.items()
    }

    adjusted_j_score_per_non_outliers = {
        region_key: (score / max_j_score_per_non_outliers) * 10 if score is not None else None
        for region_key, score in final_j_score_per_non_outliers.items()
    }


    # j_score_rank 조정 (이상치 제거 전의 rank만 사용)
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
            # 10을 초과하는 값이 있는지 확인 및 출력
            if adjusted_j_score_rank[region_key] > 10:
                print(f"Exceeded 10: Region = {region_key}, Score = {score}, Rank = {rank}, Adjusted j_score_rank = {adjusted_j_score_rank[region_key]}")

    # 데이터 준비 및 인서트
    insert_data = prepare_insert_data(adjusted_j_score_per, adjusted_j_score_rank, adjusted_j_score_per_non_outliers, ref_date)
    # for row in insert_data:
    #     print(row)
    insert_loc_info_statistics_avg_j_score(insert_data)

    return final_j_score_per, final_j_score_rank, final_j_score_per_non_outliers


def prepare_insert_data(adjusted_j_score_per, adjusted_j_score_rank, adjusted_j_score_per_non_outliers, ref_date):
    insert_data = []
    
    for region_key in adjusted_j_score_per.keys():
        city_id, district_id, sub_district_id = region_key
        j_score_per = adjusted_j_score_per.get(region_key)
        j_score_rank = adjusted_j_score_rank.get(region_key)
        j_score_per_non_outliers = adjusted_j_score_per_non_outliers.get(region_key)

        # 기존 j_score의 평균 계산 (rank는 이상치 제거 전 기준)
        if j_score_per is not None and j_score_rank is not None:
            j_score = (j_score_per + j_score_rank) / 2
        else:
            j_score = None
        
        # j_score_non_outliers는 기존 rank와 이상치 제거한 per의 평균 계산
        if j_score_per_non_outliers is not None and j_score_rank is not None:
            j_score_non_outliers = (j_score_rank + j_score_per_non_outliers) / 2
        else:
            j_score_non_outliers = None

        # 인서트할 각 행 생성
        insert_row = {
            'city_id': city_id,
            'district_id': district_id,
            'sub_district_id': sub_district_id,
            'j_score_rank': j_score_rank,
            'j_score_per': j_score_per,
            'j_score': j_score,
            'j_score_per_non_outliers': j_score_per_non_outliers,
            'j_score_non_outliers': j_score_non_outliers,
            'ref_date':ref_date
        }
        insert_data.append(insert_row)

    return insert_data



def execute_calculate_weighted_j_scores():
    target_items = ['shop', 'move_pop', 'sales', 'work_pop', 'income', 'spend', 'house', 'resident', 'apart_price']
    ref_dates = ['2024-11-01']

    # 데이터베이스 연결 설정
    connection = get_db_connection()

    results = []
    for ref_date in ref_dates:
        # 각 날짜별로 가중치 적용 J-Score 계산 실행
        (final_j_score_per, final_j_score_rank, 
         final_j_score_per_non_outliers) = calculate_weighted_j_scores(connection, target_items, ref_date)
        
        # 결과 저장 (이상치 제거 전후 J-Score를 포함하여 저장)
        results.append({
            'ref_date': ref_date,
            'final_j_score_per': final_j_score_per,
            'final_j_score_rank': final_j_score_rank,
            'final_j_score_per_non_outliers': final_j_score_per_non_outliers
        })

    # 데이터베이스 연결 해제
    connection.close()

    return results






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