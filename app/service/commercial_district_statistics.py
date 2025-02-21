import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from typing import List, Dict
from dotenv import load_dotenv
import numpy as np
from tqdm import tqdm
import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager

# 기존 import 문은 그대로 유지합니다
from app.crud.commercial_district import (
    insert_average_sales_statistics as crud_insert_average_sales_statistics,
    insert_sub_district_density_statistics as crud_insert_sub_district_density_statistics,
    select_average_payment_has_value as crud_select_average_payment_has_value,
    insert_average_payment_statistics as crud_insert_average_payment_statistics,
    select_average_sales_has_value as crud_select_average_sales_has_value,
    insert_usage_count_statistics as crud_insert_usage_count_statistics,
    select_market_size_has_value as crud_select_market_size_has_value,
    insert_market_size_statistics as crud_insert_market_size_statistics,
    select_column_name_has_value,
    select_sub_district_density_has_value as crud_select_sub_district_density_has_value,
    select_usage_count_has_value as crud_select_usage_count_has_value,
    select_commercial_district_sub_district_detail_category_ids as crud_select_commercial_district_sub_district_detail_category_ids,
    select_commercial_district_j_score_weight_average_data as crud_select_commercial_district_j_score_weight_average_data,
    insert_or_update_commercial_district_j_score_weight_average_data_batch as crud_insert_or_update_commercial_district_j_score_weight_average_data_batch,
)
from app.schemas.commercial_district import (
    CommercialDistrictStatistics,
    CommercialDistrictSubDistrictDetailCategoryId,
    CommercialDistrictWeightedAvgStatistics,
)

load_dotenv()

# 데이터베이스 연결 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": DictCursor,
}


@contextmanager
def get_db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
    finally:
        connection.close()

# 시간 재는 함수
def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(
            f"Execution time for {func.__name__}: {end_time - start_time:.2f} seconds"
        )
        return result

    return wrapper


def batch_select_category_ids(
    biz_detail_category_ids: List[int],
) -> Dict[int, Dict[str, int]]:
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            query = """
            SELECT 
                bdc.BIZ_DETAIL_CATEGORY_ID,
                bmc.BIZ_MAIN_CATEGORY_ID,
                bsc.BIZ_SUB_CATEGORY_ID
            FROM
                biz_detail_category bdc
            JOIN biz_sub_category bsc ON bsc.BIZ_SUB_CATEGORY_ID = bdc.BIZ_SUB_CATEGORY_ID
            JOIN biz_main_category bmc ON bmc.BIZ_MAIN_CATEGORY_ID = bsc.BIZ_MAIN_CATEGORY_ID
            WHERE bdc.BIZ_DETAIL_CATEGORY_ID IN (%s)
            """
            placeholders = ",".join(["%s"] * len(biz_detail_category_ids))
            cursor.execute(query % placeholders, biz_detail_category_ids)
            results = cursor.fetchall()
            return {
                row["BIZ_DETAIL_CATEGORY_ID"]: {
                    "BIZ_MAIN_CATEGORY_ID": row["BIZ_MAIN_CATEGORY_ID"],
                    "BIZ_SUB_CATEGORY_ID": row["BIZ_SUB_CATEGORY_ID"],
                }
                for row in results
            }


def calculate_j_score(data):
    j_score_data = []

    # biz_detail_category_id로 데이터를 그룹화합니다.
    for biz_detail_category_id, group in itertools.groupby(
        data,
        key=lambda x: x.biz_detail_category_id,  # biz_detail_category_id로만 그룹화
    ):
        group_data = list(group)  # 그룹화된 데이터를 리스트로 변환합니다.
        counts = [
            item.column_name for item in group_data
        ]  # 각 그룹의 column_name를 추출합니다.
        ranked_counts = sorted(
            counts, reverse=True
        )  # column_name를 내림차순으로 정렬합니다.
        totals = len(counts)  # 그룹의 총 항목 수를 계산합니다.

        # 각 항목에 대해 J-Score를 계산합니다.
        for item in group_data:
            column_name = item.column_name  # 컬럼값 가져옵니다.
            if column_name > 0:
                rank = ranked_counts.index(column_name) + 1  # 순위를 찾습니다.

                j_score_rank = 10 * ((totals + 1 - rank) / totals)  # J-Score 계산

                max_value = max([i.column_name for i in group_data])
                j_score_per = 10 * (item.column_name / max_value)  # J-Score_Per 계산

                j_score = (j_score_rank + j_score_per) / 2
            else:
                j_score_rank = 0  # 컬럼값 0일 경우 J-Score는 0
                j_score_per = 0

            j_score_data.append(
                (
                    item.city_id,  # 도시 ID
                    item.district_id,  # 구 ID
                    item.sub_district_id,  # 동 ID
                    biz_detail_category_id,  # 소분류 ID
                    column_name,  # 시장 규모
                    j_score_rank,  # J-Score Rank
                    j_score_per,  # J-Score Percent
                    j_score,  # J-Score 평균
                )
            )

    return j_score_data  # 계산된 J-Score 데이터를 반환합니다.


def calculate_statistics(column_names: List[float]):
    # 시장 규모에 대한 통계를 계산하여 딕셔너리로 반환합니다.
    return {
        "average": np.mean(column_names),  # 평균
        "median": np.median(column_names),  # 중앙값
        "stddev": np.std(column_names),  # 표준편차
        "max": np.max(column_names),  # 최대값
        "min": np.min(column_names),  # 최소값
    }


def process_group(group, ref_date, category_ids):
    # group의 내용을 출력하여 확인
    # print(f"Received group: {group}")

    # group의 첫 번째 요소는 key, 두 번째 요소는 group_data
    key, group_data = group

    # 같은 카테고리의 market_size들을 모두 합침
    total_market_sizes = [item[4] for item in group_data]

    # 묶인 전체 market_sizes에 대해 통계 계산
    statistics = calculate_statistics(total_market_sizes)

    # print(f"키, 통계:{key} {statistics}")
    # print(f"j_score: {group_data}")

    category_id = category_ids.get(key)
    if category_id is None:
        print(f"카테고리 ID를 찾을 수 없습니다. biz_detail_category_id: {key}")
        return None

    biz_main_category_id = category_id["BIZ_MAIN_CATEGORY_ID"]
    biz_sub_category_id = category_id["BIZ_SUB_CATEGORY_ID"]

    statistics_to_insert = []

    # j_score 데이터에서 통계값을 공유
    for j_score_data in group_data:
        stat = CommercialDistrictStatistics(
            city_id=j_score_data[0],  # city_id
            district_id=j_score_data[1],  # district_id
            sub_district_id=j_score_data[2],  # sub_district_id
            biz_main_category_id=biz_main_category_id,
            biz_sub_category_id=biz_sub_category_id,
            biz_detail_category_id=key,
            avg_val=statistics["average"],
            med_val=statistics["median"],
            std_val=statistics["stddev"],
            max_val=statistics["max"],
            min_val=statistics["min"],
            j_score_rank=j_score_data[5],  # 각 데이터의 j_score를 사용
            j_score_per=j_score_data[6],  # 각 데이터의 j_score를 사용
            j_score=j_score_data[7],  # 각 데이터의 j_score를 사용
            stat_level="전국",
            ref_date=ref_date,
        )
        statistics_to_insert.append(stat)

    return statistics_to_insert


# 상권 분석 시장 규모
@time_execution
def commercial_district_market_size_statistics(ref_date: str):
    results = crud_select_market_size_has_value()
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            crud_insert_market_size_statistics(statistics_to_insert)


# 상권 분석 결제 건수
@time_execution
def commercial_district_usage_count_statistics(ref_date: str):
    results = crud_select_usage_count_has_value()
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            crud_insert_usage_count_statistics(statistics_to_insert)


# 상권 분석 평균 매출
@time_execution
def commercial_district_average_sales_statistics(ref_date: str):
    results = crud_select_average_sales_has_value()
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            crud_insert_average_sales_statistics(statistics_to_insert)


# 상권 분석 읍/면/동 밀집도
def commercial_district_sub_district_density_statistics(ref_date: str):
    results = crud_select_sub_district_density_has_value()
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            crud_insert_sub_district_density_statistics(statistics_to_insert)


# 상권 분석 평균 결제
@time_execution
def commercial_average_payment_statistics(ref_date: str):
    results = crud_select_average_payment_has_value()
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            crud_insert_average_payment_statistics(statistics_to_insert)


def commercial_district_column_name_statistics(column_name: str, ref_date: str):
    results = select_column_name_has_value(column_name)
    results_with_j_score = calculate_j_score(results)

    # biz_detail_category_id만을 기준으로 데이터 그룹화
    grouped_data = {}
    unique_biz_detail_category_ids = set()
    for item in results_with_j_score:
        biz_detail_category_id = item[3]
        if biz_detail_category_id not in grouped_data:
            grouped_data[biz_detail_category_id] = []
        grouped_data[biz_detail_category_id].append(item)
        unique_biz_detail_category_ids.add(biz_detail_category_id)

    # 카테고리 ID 일괄 조회
    category_ids = batch_select_category_ids(list(unique_biz_detail_category_ids))

    with tqdm(total=len(grouped_data), desc="데이터 처리 중") as pbar:
        statistics_to_insert = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_group, (key, group), ref_date, category_ids
                ): key
                for key, group in grouped_data.items()
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    statistics_to_insert.extend(result)
                pbar.update(1)

        if statistics_to_insert:
            if column_name == "MARKET_SIZE":
                crud_insert_market_size_statistics(statistics_to_insert)


##############################################################################################


# 상권분석 가중치 통계
def insert_or_update_commercial_district_j_score_weight_average_data_thread(
    commercial_district_j_score_average_data_list: List[
        CommercialDistrictWeightedAvgStatistics
    ],
    batch_size: int = 5000,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(
            0, len(commercial_district_j_score_average_data_list), batch_size
        ):
            batch = commercial_district_j_score_average_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_j_score_weight_average_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd jscore average batches",
        ):
            future.result()


def select_commercial_district_j_score_weight_average(
    commercial_district_sub_district_detail_category_id_list: List[
        CommercialDistrictSubDistrictDetailCategoryId
    ],
) -> List[CommercialDistrictWeightedAvgStatistics]:
    try:
        results = crud_select_commercial_district_j_score_weight_average_data(
            commercial_district_sub_district_detail_category_id_list
        )
    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        return []

    return results


@time_execution
def commercial_district_j_score_weighted_average_statistics():
    commercial_district_sub_district_detail_category_id_list: List[
        CommercialDistrictSubDistrictDetailCategoryId
    ] = crud_select_commercial_district_sub_district_detail_category_ids()

    print(len(commercial_district_sub_district_detail_category_id_list))
    print(commercial_district_sub_district_detail_category_id_list[1])

    commercial_district_j_score_weight_average_list = (
        # select_commercial_district_j_score_weight_average_thread(
        select_commercial_district_j_score_weight_average(
            commercial_district_sub_district_detail_category_id_list
        )
    )
    # print(len(commercial_district_j_score_weight_average_list))
    # print(commercial_district_j_score_weight_average_list[0])
    # print(commercial_district_j_score_weight_average_list[1])
    # print(commercial_district_j_score_weight_average_list[2])
    insert_or_update_commercial_district_j_score_weight_average_data_thread(
        commercial_district_j_score_weight_average_list
    )


if __name__ == "__main__":
    commercial_district_market_size_statistics("2024-12-31")
    commercial_district_usage_count_statistics("2024-12-31")
    commercial_district_average_sales_statistics("2024-12-31")
    commercial_district_sub_district_density_statistics("2024-12-31")
    commercial_average_payment_statistics("2024-12-31")
    # commercial_district_column_name_statistics("MARKET_SIZE", "2024-08-01")
    commercial_district_j_score_weighted_average_statistics()  # 2256.86 seconds
    print("END!!!!!!!!")
