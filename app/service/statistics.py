import numpy as np
from tqdm import tqdm
from app.crud.biz_detail_category import (
    select_biz_detail_category_id_by_biz_detail_category_name as crud_select_biz_detail_category_id_by_biz_detail_category_name,
)
from app.crud.city import get_city_id as crud_get_city_id
from app.crud.district import get_district_id as crud_get_district_id
from app.crud.loc_store import (
    get_region_id_by_store_business_number as crud_get_region_id_by_store_business_number,
    select_local_store_sub_distirct_id_by_store_business_number as crud_select_local_store_sub_distirct_id_by_store_business_number,
)
from app.crud.stat_item import (
    select_detail_category_id_by_stat_item_id as crud_select_detail_category_id_by_stat_item_id,
    select_stat_item_info_by_stat_item_id as crud_select_stat_item_info_by_stat_item_id,
)
from app.crud.statistics import (
    select_statistics_data_by_sub_district_id_detail_category_id as crud_select_statistics_data_by_sub_district_id_detail_category_id,
)
from app.crud.statistics import *
from app.crud.sub_district import get_sub_district_id_by as crud_get_sub_district_id_by
from app.schemas.commercial_district import CommercialStatisticsData
from app.schemas.loc_store import LocalStoreSubdistrict
from app.schemas.stat_item import StatItemInfo
from app.schemas.statistics import (
    LocInfoAvgJscoreOutput,
    PopulationCompareResidentWorkPop,
)
from app.crud.stat_item import (
    select_all_stat_item_id_by_detail_category_id as crud_select_all_stat_item_id_by_detail_category_id,
)


################# 입지 정보 동 기준 가중치 평균 j_score 값 계산 ###################
# 리포트 보여주기
# div 입지분석 : x.x p
# 시/도명 시/군/구명 읍/면/동명
# '전자정부 상권정보' ref_date
def select_avg_j_score(store_business_id: str) -> LocInfoAvgJscoreOutput:

    local_store_sub_district_data: LocalStoreSubdistrict = (
        crud_select_local_store_sub_distirct_id_by_store_business_number(
            store_business_id
        )
    )

    sub_district_id = local_store_sub_district_data.get("SUB_DISTRICT_ID")

    # 1. 해당 동의 필요한 j_score 목록 가져오기
    result = get_weighted_jscore(sub_district_id)
    # result 리스트에서 J_SCORE 값만 추출
    j_scores = [item["J_SCORE"] for item in result]

    # 2. 가중치 값 적용
    shop_k = 1
    move_pop_k = 2.5
    sales_k = 1.5
    work_pop_k = 1.5
    income_k = 1.5
    spend_k = 1.5
    house_k = 1
    resident_k = 1
    mz_population_k = 1.5

    # 3. 가중치 값 적용 후 평균 계산
    weights = [
        shop_k,
        move_pop_k,
        sales_k,
        work_pop_k,
        income_k,
        spend_k,
        house_k,
        resident_k,
        mz_population_k,
    ]
    weighted_sum = sum(j_score * weight for j_score, weight in zip(j_scores, weights))
    total_weight = sum(weights)

    weighted_avg_val = weighted_sum / total_weight if total_weight != 0 else 0
    # print(weighted_avg_val)

    # 4. 이쁘게 포장
    final_item = result[0]
    del final_item["column_name"]
    del final_item["J_SCORE"]
    del final_item["table_name"]
    final_item["weighted_avg_val"] = weighted_avg_val

    # print(final_item)

    return final_item


################# 동 주거 환경 ###################
# 리포트 보여주기
# div x.x 동 주거 환경 :  ~~ % 차지
# 시/도명 시/군/구명 읍/면/동명


def fetch_living_env(store_business_id: str) -> PopulationCompareResidentWorkPop:
    local_store_sub_district_data: LocalStoreSubdistrict = (
        crud_select_local_store_sub_distirct_id_by_store_business_number(
            store_business_id
        )
    )
    sub_district_id = local_store_sub_district_data.get("SUB_DISTRICT_ID")

    result = get_living_env(sub_district_id)
    work_pop = result["work_pop"]
    resident = result["resident"]

    # resident의 비율을 계산
    total_population = work_pop + resident
    resident_percentage = (
        (resident / total_population) * 100 if total_population > 0 else 0
    )
    work_pop_percentage = (
        (work_pop / total_population) * 100 if total_population > 0 else 0
    )
    resident_percentage = int(resident_percentage)
    work_pop_percentage = int(work_pop_percentage)

    # 결과에 resident_percentage 추가
    result["resident_percentage"] = resident_percentage
    result["work_pop_percentage"] = work_pop_percentage

    # print(result)
    return result


################# 매장 인근 (xx동) 유동 인구 ###################
# 리포트 보여주기
# div x.x 동 주거 환경 :  ~~ % 차지
# 시/도명 시/군/구명 읍/면/동명
def fetch_move_pop(sub_district_id):
    data = get_move_pop_and_j_score(sub_district_id)

    # move_pop_data와 j_score_data를 딕셔너리에서 추출
    result = data["move_pop_data"]
    list = data["move_pop_list"]
    j_score_data = data["j_score_data"]

    # 1. 일 평균 구하기
    move_pop = result[0].get("move_pop", 0)  # move_pop 값을 가져옴

    # 일 평균 이동 인구 계산 (30일 기준)
    days_in_month = 30
    daily_average_move_pop = move_pop / days_in_month if move_pop > 0 else 0
    daily_average_move_pop = int(daily_average_move_pop)  # 소수점 버리기

    # 일 평균값을 result에 추가
    result[0]["daily_average_move_pop"] = daily_average_move_pop

    # 2. 시/도 내에서 선택한 동의 유동인구가 낮은지 높은지 판단
    city_name = result[0]["city_name"]
    sub_district_name = result[0]["sub_district_name"]
    move_pop_value = result[0]["move_pop"]

    # 같은 city_name에 속하고, sub_district_name이 다른 항목들을 필터링
    same_city_data = [
        item
        for item in list
        if item["city_name"] == city_name
        and item["sub_district_name"] != sub_district_name
    ]
    move_pop_values = sorted([item["move_pop"] for item in same_city_data])

    # 33% 기준으로 구간 나누기
    total_count = len(move_pop_values)
    lower_threshold = total_count // 3  # 하위 33% 경계
    upper_threshold = total_count * 2 // 3  # 상위 33% 경계

    # 현재 result의 move_pop 값이 어느 구간에 속하는지 판단
    if move_pop_value <= move_pop_values[lower_threshold]:  # 하위 33% 구간
        move_pop_comparison = "lower 33%"
    elif move_pop_value >= move_pop_values[upper_threshold]:  # 상위 33% 구간
        move_pop_comparison = "upper 33%"
    else:
        move_pop_comparison = "middle 33%"  # 중간 33% 구간

    result[0]["move_pop_comparison"] = move_pop_comparison

    # 3. 시/도 내의 유동인구 평균 값
    city_move_pop_average = data["move_pop_city_stat"]

    # 평균값을 result에 추가
    result[0]["city_move_pop_average"] = city_move_pop_average

    # 4. 해당 동의 j_score 값 result 에 추가
    j_score = j_score_data[0].get("j_score", 0)
    result[0]["j_score"] = j_score

    # print(result)

    return result


################## 입지 정보 통계 값 조회 #############################


def select_stat_data(filters_dict):
    result = get_stat_data(filters_dict)
    return result


################# 시/도, 시/군/구 id 값 가져오기 ############################

# 전역 변수 선언
city_ids_cache = None
city_district_pairs_cache = None


def fetch_city():
    global city_ids_cache
    if city_ids_cache is None:
        # print("Fetching city_district_pairs from DB...")
        city_ids_cache = get_all_city_ids()  # DB에서 한 번만 가져옴
    return city_ids_cache


def fetch_city_district_pairs():
    """
    city_district_pairs를 전역 변수에 캐싱하여 한 번만 가져오는 함수
    """
    global city_district_pairs_cache
    if city_district_pairs_cache is None:
        # print("Fetching city_district_pairs from DB...")
        city_district_pairs_cache = (
            get_all_city_district_pairs()
        )  # DB에서 한 번만 가져옴
    return city_district_pairs_cache


################# stat_item_id 값 가져오기 ########################

stat_item_id_list = None


def fetch_stat_item_id():
    global stat_item_id_list
    if stat_item_id_list is None:
        stat_item_id_list = get_stat_item_id()

    # print(stat_item_id_list)

    return stat_item_id_list


################## 데이터 통계 값 구하기 ###################
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


################### 전국 단위 j_score 계산 후 인서트 #######################
def get_j_score_national(stat_item_id):
    national_data = get_all_city_district_sub_district()

    # data는 city_id, district_id, sub_district_id, count로 구성된 리스트
    data = get_j_score_national_data(national_data)

    # 원하는 컬럼만 추출하여 별도의 리스트 생성
    counts = [item[-1] for item in data]

    # 데이터 기준으로 순위 계산
    ranked_counts = sorted(counts, reverse=True)  # 내림차순으로 정렬

    j_score_data_nation = []

    for city_id, district_id, sub_district_id, j_column in data:
        if j_column > 0:
            # 해당 매장 수의 순위
            rank = ranked_counts.index(j_column) + 1
            totals = len(counts)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # 매장 수가 0인 경우 j_score도 0

        # j_score_data에 (city_id, district_id, sub_district_id, count, j_score) 형태로 추가
        j_score_data_nation.append(
            (stat_item_id, city_id, district_id, sub_district_id, j_score)
        )

    # print(j_score_data_nation)
    # insert_j_score_nation(j_score_data_nation)

    return j_score_data_nation


################### 전국 통계 값 업데이트 후 지역 통계 값 인서트 ####################
def get_city_district_and_national_statistics(stat_item_id):
    """
    전국 통계와 시/군/구별 통계를 계산하는 함수
    """
    # 1. 전국 데이터를 가져와 통계 계산
    national_data = get_national_data()
    national_stats = calculate_statistics(national_data)

    # 2. 전국 단위 통계값 업데이트
    national_stats = {"stat_item_id": stat_item_id, **national_stats}
    # print(national_stats)
    # update_stat_nation(national_stats)

    # 3. 모든 시/도 값 가져옴
    city_value = fetch_city()

    # 4. 시/도 별 통계 계산
    city_stat_list = []
    for city_id in city_value:
        city_data = get_city_data(city_id)
        city_stats = calculate_statistics(city_data)
        city_stat_list.append(
            {
                "city_id": city_id,
                "statistics": city_stats,
            }
        )
    city_stat_list = [
        {"stat_item_id": stat_item_id, **entry} for entry in city_stat_list
    ]

    # 5. 시/도 별 통계 값 인서트
    insert_stat_city(city_stat_list)

    # 6. 모든 시/군/구(city_id, district_id) 쌍을 가져옴
    city_district_pairs = fetch_city_district_pairs()

    # 7. 시/군/구별 통계 계산
    city_district_stats_list = []
    for city_id, district_id in city_district_pairs:
        city_district_data = get_city_district_data(city_id, district_id)
        city_district_stats = calculate_statistics(city_district_data)
        city_district_stats_list.append(
            {
                "city_id": city_id,
                "district_id": district_id,
                "statistics": city_district_stats,
            }
        )

    # 8. 시/군/구 별 통계 값 인서트
    city_district_stats_list = [
        {"stat_item_id": stat_item_id, **entry} for entry in city_district_stats_list
    ]
    # print(city_district_stats_list)
    # insert_stat_region(city_district_stats_list)

    return {
        "national_statistics": national_stats,
        "city_stat_list": city_stat_list,
        "city_district_statistics": city_district_stats_list,
    }


############# 모든 지역 내 시군구 j_score 계산 후 업데이트 ##############
def get_j_score_for_region(stat_item_id):
    """
    특정 지역(city_id) 내 시/군/구 간의 j_score를 계산하는 함수
    """
    # 해당 지역(city_id) 내 모든 district_id와 그 하위의 매장 데이터를 가져옴
    city_district_pairs = fetch_city_district_pairs()

    # 매장 데이터를 담을 리스트
    data = []

    for city_id, district_id in city_district_pairs:
        # 각 city_id와 district_id에 대한 매장 수 합산
        total_count = get_data_for_city_and_district(city_id, district_id)

        # (city_id, district_id, total_count) 형태로 리스트에 추가
        data.append((city_id, district_id, total_count))

    # 매장 수 기준으로 순위 계산 (내림차순)
    counts = [item[2] for item in data]
    ranked_counts = sorted(counts, reverse=True)

    j_score_data_region = []

    for city_id, district_id, count in data:
        if count > 0:
            # 해당 매장 수의 순위
            rank = ranked_counts.index(count) + 1
            totals = len(counts)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # 매장 수가 0인 경우 j_score도 0

        # j_score_data에 (city_id, district_id, count, j_score) 형태로 추가
        j_score_data_region.append((stat_item_id, city_id, district_id, None, j_score))

    # print(j_score_data_region)
    update_j_score_data_region(j_score_data_region)

    return j_score_data_region


#################################################### 인구 관련 통계 ###############################################


##################### 전국 범위 동별 mz 세대 인구 j_score 구해서 인서트 ########################
def get_j_score_national_mz_population(stat_item_id):

    # 1. 전국 지역 id 값 가져오기
    national_data = get_all_city_district_sub_district()

    # 2. 전국 지역 id 값으로 mz 세대 인구 읍면동 별 값 가져오기
    data = get_j_score_national_data_mz(national_data)

    # 3. 전국의 동별 mz 세대 인구 j_score 값 계산
    counts = [
        item[-1] if item[-1] is not None else 0 for item in data
    ]  # None 값을 0으로 변환

    # 데이터 기준으로 순위 계산
    ranked_counts = sorted(counts, reverse=True)  # 내림차순으로 정렬

    j_score_data_nation_mz = []

    for city_id, district_id, sub_district_id_mz, j_column in data:
        # None 값을 0으로 처리
        j_column = j_column if j_column is not None else 0

        if j_column > 0:
            # 해당 mz_population의 순위
            rank = ranked_counts.index(j_column) + 1
            totals = len(counts)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # mz_population이 0인 경우 j_score도 0

        # j_score_data에 (stat_item_id, city_id, district_id, sub_district_id, j_score) 형태로 추가
        j_score_data_nation_mz.append(
            (stat_item_id, city_id, district_id, sub_district_id_mz, j_score)
        )

    insert_j_score_nation(j_score_data_nation_mz)
    # print(j_score_data_nation_mz)

    return j_score_data_nation_mz


################### mz 세대 인구 수 전국 통계 값 업데이트 후 지역 통계 값 인서트 ####################
def get_city_district_and_national_statistics_mz_population(stat_item_id):
    """
    전국 통계와 시/군/구별 통계를 계산하는 함수
    """
    # 1. 전국 데이터를 가져와 통계 계산
    national_data = get_national_data_mz_population()
    national_stats = calculate_statistics(national_data)

    # 2. 모든 시/군/구(city_id, district_id) 쌍을 가져옴
    city_district_pairs = fetch_city_district_pairs()

    # 3. 시/군/구별 통계 계산
    city_district_stats_list = []
    for city_id, district_id in city_district_pairs:
        city_district_data = get_city_district_data_mz_population(city_id, district_id)
        city_district_stats = calculate_statistics(city_district_data)
        city_district_stats_list.append(
            {
                "city_id": city_id,
                "district_id": district_id,
                "statistics": city_district_stats,
            }
        )

    # 전국 단위 통계값 업데이트
    national_stats = {"stat_item_id": stat_item_id, **national_stats}
    # print(national_stats)
    update_stat_nation(national_stats)

    # 시/군/구 별 통계 값 인서트
    city_district_stats_list = [
        {"stat_item_id": stat_item_id, **entry} for entry in city_district_stats_list
    ]
    # print(city_district_stats_list)
    insert_stat_region(city_district_stats_list)

    return {
        "national_statistics": national_stats,
        "city_district_statistics": city_district_stats_list,
    }


############# mz 세대 인구 데이터 모든 지역 내 시군구 j_score 계산 후 업데이트 ##############
def get_j_score_for_region_mz_population(stat_item_id):
    """
    특정 지역(city_id) 내 시/군/구 간의 j_score를 계산하는 함수
    """
    # 해당 지역(city_id) 내 모든 district_id와 그 하위의 매장 데이터를 가져옴
    city_district_pairs = fetch_city_district_pairs()

    # 매장 데이터를 담을 리스트
    data = []

    for city_id, district_id in city_district_pairs:
        # 각 city_id와 district_id에 대한 매장 수 합산
        total_count = get_data_for_city_and_district_mz_population(city_id, district_id)

        # (city_id, district_id, total_count) 형태로 리스트에 추가
        data.append((city_id, district_id, total_count))

    # 매장 수 기준으로 순위 계산 (내림차순)
    counts = [item[2] for item in data]
    ranked_counts = sorted(counts, reverse=True)

    j_score_data_region = []

    for city_id, district_id, count in data:
        if count > 0:
            # 해당 매장 수의 순위
            rank = ranked_counts.index(count) + 1
            totals = len(counts)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0  # 매장 수가 0인 경우 j_score도 0

        # j_score_data에 (city_id, district_id, count, j_score) 형태로 추가
        j_score_data_region.append((stat_item_id, city_id, district_id, None, j_score))
    # print(j_score_data_region)
    update_j_score_data_region(j_score_data_region)

    return j_score_data_region


def get_j_score_national_commercial_distirct(stat_item_id: int):
    # print(f"stat_item_id_1: {stat_item_id}")

    stat_item_info: StatItemInfo = crud_select_stat_item_info_by_stat_item_id(
        stat_item_id
    )
    stat_item_table_name = stat_item_info.table_name
    stat_item_column_name = stat_item_info.column_name
    # print(stat_item_column_name)
    # print(stat_item_table_name)
    national_data = get_all_city_district_sub_district()

    detail_category_id = crud_select_detail_category_id_by_stat_item_id(stat_item_id)

    # print(national_data)
    # print(detail_category_id)
    # print(stat_item_table_name)
    # print(stat_item_column_name)

    # data는 city_id, district_id, sub_district_id, count로 구성된 리스트
    data = get_j_score_national_data_by_detail_categroy_id(
        national_data, detail_category_id, stat_item_table_name, stat_item_column_name
    )

    # print(data)

    # 원하는 컬럼만 추출하여 별도의 리스트 생성
    counts = [item[-1] for item in data]

    # 데이터 기준으로 순위 계산
    ranked_counts = sorted(counts, reverse=True)  # 내림차순으로 정렬

    j_score_data_nation = []

    for city_id, district_id, sub_district_id, j_column in data:
        if j_column > 0:
            # 해당 매장 수의 순위
            rank = ranked_counts.index(j_column) + 1
            totals = len(counts)

            # j_score 계산
            j_score = 10 * ((totals + 1 - rank) / totals)
        else:
            j_score = 0

        # j_score_data에 (city_id, district_id, sub_district_id, count, j_score) 형태로 추가
        j_score_data_nation.append(
            (
                stat_item_id,
                city_id,
                district_id,
                sub_district_id,
                j_score,
                1,
                "2024-08-01",
            )
        )

    # print(j_score_data_nation)
    # insert_j_score_nation(j_score_data_nation)

    return j_score_data_nation


def get_city_district_and_national_statistics_commercial_district(stat_item_id):
    # 1. 전국 데이터를 가져와 통계 계산
    stat_item_info = crud_select_stat_item_info_by_stat_item_id(stat_item_id)

    # biz_detail_category_id가 0인지 확인
    if stat_item_info.biz_detail_category_id == 0:
        return {
            "message": f"No action taken because biz_detail_category_id is 0.  stat_item_info: {stat_item_info}"
        }

    national_data = get_national_data_by_detail_category(
        stat_item_info.column_name,
        stat_item_info.table_name,
        stat_item_info.biz_detail_category_id,
    )

    national_stats = calculate_statistics(national_data)

    # 2. 전국 단위 통계값 업데이트
    national_stats = {"stat_item_id": stat_item_id, **national_stats}

    # print(national_stats)
    update_stat_nation(national_stats)

    return {
        "national_statistics": national_stats,
    }


def loop_commercial_district_statistics():
    statistics_start_id, statistics_end_id = 11, 1699
    for idx in tqdm(
        range(statistics_start_id, statistics_end_id + 1), desc="Processing"
    ):
        tqdm.write(f"Currently processing idx: {idx}")
        get_j_score_national_commercial_distirct(idx)

    for idx in tqdm(
        range(statistics_start_id, statistics_end_id + 1), desc="Processing"
    ):
        tqdm.write(f"Currently processing idx: {idx}")
        get_city_district_and_national_statistics_commercial_district(idx)


def loop_avg_commercial_district_statistics():
    statistics_start_id, statistics_end_id = 11, 1699
    for idx in tqdm(
        range(statistics_start_id, statistics_end_id + 1), desc="Processing"
    ):
        tqdm.write(f"Currently processing idx: {idx}")
        get_city_district_and_national_statistics_commercial_district(idx)


# 테스트 실행 예시
if __name__ == "__main__":
    # 입지 정보 통계값, j_score 테이블에 넣기
    # get_j_score_national(1) # stat_item_id
    # get_city_district_and_national_statistics(8) # stat_item_id
    # get_j_score_for_region(9) # stat_item_id

    ####################################################
    # mz 인구 통계 값, j_score 값 테이블에 넣기
    # get_j_score_national_mz_population(14)    # stat_item_id
    # get_city_district_and_national_statistics_mz_population(14)       # stat_item_id
    # get_j_score_for_region_mz_population(14)  # stat_item_id

    ###################################################
    # 입지 정보 j_score 가중치 평균 구하기
    # select_avg_j_score(50)  # sub_distric_id
    # fetch_living_env(50)    # sub_distric_id
    # fetch_move_pop(50)  # sub_distric_id

    ###############################################
    # 상권분석
    # get_j_score_national_commercial_distirct(11)
    loop_commercial_district_statistics()
    # get_city_district_and_national_statistics_commercial_district(19)
    # loop_avg_commercial_district_statistics()
    pass


def select_statistics_by_sub_district_detail_category(
    city_name: str,
    district_name: str,
    sub_district_name: str,
    biz_detail_category_name: str,
):
    # print(city_name)
    # print(district_name)
    # print(sub_district_name)
    # print(biz_detail_category_name)

    city_id = crud_get_city_id(city_name)
    district_id = crud_get_district_id(city_id, district_name)
    sub_district_id = crud_get_sub_district_id_by(
        city_id, district_id, sub_district_name
    )

    detail_category_id = crud_select_biz_detail_category_id_by_biz_detail_category_name(
        biz_detail_category_name
    )

    # print(city_id)
    # print(district_id)
    # print(sub_district_id)
    # print(detail_category_id)

    stat_item_id_list = []
    stat_item_id_list = crud_select_all_stat_item_id_by_detail_category_id(
        detail_category_id
    )

    statistics_data: CommercialStatisticsData = (
        crud_select_statistics_data_by_sub_district_id_detail_category_id(
            sub_district_id, stat_item_id_list
        )
    )

    # print(statistics_data)

    return statistics_data


def select_statistics_by_store_business_number(
    store_business_number: str,
):
    # print(store_business_number)

    # city_id = crud_get_city_id(city_name)
    # district_id = crud_get_district_id(city_id, district_name)
    # sub_district_id = crud_get_sub_district_id_by(
    #     city_id, district_id, sub_district_name
    # )

    region_id = crud_get_region_id_by_store_business_number(store_business_number)

    print(region_id)

    # detail_category_id = crud_select_biz_detail_category_id_by_biz_detail_category_name(
    #     biz_detail_category_name
    # )

    # print(city_id)
    # print(district_id)
    # print(sub_district_id)
    # print(detail_category_id)

    stat_item_id_list = []
    # stat_item_id_list = crud_select_all_stat_item_id_by_detail_category_id(
    #     detail_category_id
    # )

    # statistics_data: CommercialStatisticsData = (
    #     crud_select_statistics_data_by_sub_district_id_detail_category_id(
    #         sub_district_id, stat_item_id_list
    #     )
    # )

    # print(statistics_data)

    # return statistics_data
    pass
