from app.crud.sub_district import (
    select_all_region_id,
    select_city_id_sub_district_id,
    select_district_id_sub_district_id
)
from app.crud.loc_info_statistics import (
    select_nation_loc_info_by_region,
    select_city_loc_info_by_region,
    select_district_loc_info_by_region,
    insert_loc_info_statistics
)
import numpy as np
from collections import defaultdict

# 반복 시행
def insert_by_date():
    date_list = ['2024-08-01']
    target_list = ['shop', 'move_pop', 'sales', 'work_pop', 'income', 'spend', 'house', 'resident']

    for ref_date in date_list:
        for target_item in target_list:
            # 날짜와 타겟을 각각 전달
            print(target_item, '시작')
            fetch_loc_info_and_insert(ref_date, target_item)
            


def fetch_loc_info_and_insert(ref_date, target_item):
    # 전국 범위
    # 1. city_id, district_id, sub_district_id 값 리스트로 조회
    region_id_list = select_all_region_id()

    # 2. 각 지역에 해당하는 타겟 항목 값 조회
    nation_loc_info_by_region = []

    for region in region_id_list:
        nation_loc_info_by_region_list = select_nation_loc_info_by_region(
            city_id=region.city_id, 
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item=target_item
        )
        # 조회된 데이터를 리스트에 추가
        nation_loc_info_by_region.extend(nation_loc_info_by_region_list)

    # 통계 계산 및 j_score 계산을 위해 타겟 항목 값만 추출하여 리스트 생성
    nation_loc_info_values = [item.target_item for item in nation_loc_info_by_region]  # mz_pop 대신 target 항목을 사용해야 함

    # 3. 통계 계산
    statistics = calculate_statistics(nation_loc_info_values)

    # nation_loc_info_by_region 리스트의 각 항목에 통계 값을 개별 키-값 쌍으로 추가
    nation_loc_info_by_region = [
        {**item.__dict__, "avg_val": statistics["average"], "med_val": statistics["median"],
        "std_val": statistics["stddev"], "max_val": statistics["max"], "min_val": statistics["min"]}
        for item in nation_loc_info_by_region
    ]

    # 4. J-Score 계산
    nation_loc_info_values_rank = sorted(nation_loc_info_values, reverse=True)  # 내림차순으로 정렬

    for item in nation_loc_info_by_region:
        target_value = item["target_item"]  
        if target_value > 0:
            rank = nation_loc_info_values_rank.index(target_value) + 1
            totals = len(nation_loc_info_values)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # target 값이 0인 경우 j_score도 0

        # j_score를 각 항목에 추가
        item["j_score"] = j_score

    # 최종 정리 - 불필요한 mz_pop 대신 target 관련 정보를 넣고, 추가 필드를 설정
    for item in nation_loc_info_by_region:
        item['stat_level'] = '전국'
        del item['target_item']
        item['target_item'] = target_item

    # 인서트 처리 - 필요시 주석을 풀어 사용
    # 이름만 변경
    nation_loc_info_for_insert = nation_loc_info_by_region
    print(len(nation_loc_info_for_insert))
    insert_loc_info_statistics(nation_loc_info_for_insert)


    # 시/도 범위
    # 1. city_id, sub_district_id 값 리스트로 조회
    city_id_sub_district_id_list = select_city_id_sub_district_id()

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    city_loc_info_by_region = []

    for region in city_id_sub_district_id_list:
        city_loc_info_by_region_list = select_city_loc_info_by_region(
            city_id=region.city_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item=target_item
        )
        # 조회된 데이터를 리스트에 추가
        city_loc_info_by_region.extend(city_loc_info_by_region_list)

    # 3. city_id 별로 그룹화
    city_grouped_loc_info = defaultdict(list)
    for item in city_loc_info_by_region:
        city_grouped_loc_info[item.city_id].append(item.target_item)

    # 통계 및 J-Score 계산
    city_loc_info_with_statistics = []

    # 각 city_id 그룹에 대해 처리
    for city_id, loc_info_values in city_grouped_loc_info.items():
        
        # 통계 계산
        statistics = calculate_statistics(loc_info_values)

        # J-Score 계산을 위해 정렬
        loc_info_values_sorted = sorted(loc_info_values, reverse=True)

        # city_loc_info_by_region 리스트의 각 항목에 통계 값을 추가
        for item in city_loc_info_by_region:
            if item.city_id == city_id:
                target_value = item.target_item

                # 통계 값 추가
                item_dict = item.__dict__.copy()  # 객체를 딕셔너리로 변환
                item_dict["avg_val"] = statistics["average"]
                item_dict["med_val"] = statistics["median"]
                item_dict["std_val"] = statistics["stddev"]
                item_dict["max_val"] = statistics["max"]
                item_dict["min_val"] = statistics["min"]

                # J-Score 계산
                if target_value > 0:
                    rank = loc_info_values_sorted.index(target_value) + 1
                    totals = len(loc_info_values_sorted)
                    j_score = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score = 0

                item_dict["j_score"] = j_score
                item_dict['stat_level'] = '시/도'

                city_loc_info_with_statistics.append(item_dict)

    # 최종 정리 - 불필요한 mz_pop 대신 target 관련 정보를 넣고, 추가 필드를 설정
    for item in city_loc_info_with_statistics:
        item['district_id'] = None
        del item['target_item']
        item['target_item'] = target_item

    # 인서트용 이름만 바꾸기
    city_loc_info_for_insert = city_loc_info_with_statistics
    # 인서트
    print(len(city_loc_info_for_insert))
    insert_loc_info_statistics(city_loc_info_for_insert)
    

    # 시/군/구 범위
    # 1. city_id, sub_district_id 값 리스트로 조회
    district_id_sub_district_id_list = select_district_id_sub_district_id()

    # 2. 각 지역에 해당하는 mz 세대 인구 수 값 조회
    district_loc_info_by_region = []

    for region in district_id_sub_district_id_list:
        district_loc_info_by_region_list = select_district_loc_info_by_region(
            district_id=region.district_id, 
            sub_district_id=region.sub_district_id,
            ref_date=ref_date,
            target_item = target_item
        )
        # 조회된 데이터를 리스트에 추가
        district_loc_info_by_region.extend(district_loc_info_by_region_list)

    # 3. district_id 별로 그룹화
    district_grouped_loc_info = defaultdict(list)
    for item in district_loc_info_by_region:
        district_grouped_loc_info[item.district_id].append(item.target_item)

    # 통계 및 J-Score 계산
    district_loc_info_with_statistics = []

    # 각 district_id 그룹에 대해 처리
    for district_id, loc_info_values in district_grouped_loc_info.items():
        
        # 통계 계산
        statistics = calculate_statistics(loc_info_values)

        # J-Score 계산을 위해 정렬
        loc_info_values_sorted = sorted(loc_info_values, reverse=True)

        # district_loc_info_by_region 리스트의 각 항목에 통계 값을 추가
        for item in district_loc_info_by_region:
            if item.district_id == district_id:
                target_value = item.target_item

                # 통계 값 추가
                item_dict = item.__dict__.copy()  # 객체를 딕셔너리로 변환
                item_dict["avg_val"] = statistics["average"]
                item_dict["med_val"] = statistics["median"]
                item_dict["std_val"] = statistics["stddev"]
                item_dict["max_val"] = statistics["max"]
                item_dict["min_val"] = statistics["min"]

                # J-Score 계산
                if target_value > 0:
                    rank = loc_info_values_sorted.index(target_value) + 1
                    totals = len(loc_info_values_sorted)
                    j_score = 10 * ((totals + 1 - rank) / totals)
                else:
                    j_score = 0

                item_dict["j_score"] = j_score
                item_dict['stat_level'] = '시/군/구'

                district_loc_info_with_statistics.append(item_dict)

    # 최종 정리 - 불필요한 mz_pop 대신 target 관련 정보를 넣고, 추가 필드를 설정
    for item in district_loc_info_with_statistics:
        item['city_id'] = None
        del item['target_item']
        item['target_item'] = target_item

    # 인서트용 이름만 바꾸기
    district_loc_info_for_insert = district_loc_info_with_statistics
    # 인서트
    print(len(district_loc_info_for_insert))
    insert_loc_info_statistics(district_loc_info_for_insert)




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