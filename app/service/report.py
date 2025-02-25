from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import logging
from threading import local
from tqdm import tqdm
import time  # 내장 time 모듈을 가져옵니다.
from typing import Any, Callable, List
from app.crud.report import (
    insert_new_report_table as crud_insert_new_report_table,
    select_local_store_info as crud_select_local_store_info,
    select_local_store_mp_detail_cateogry_id as crud_select_local_store_mp_detail_cateogry_id,
    select_local_store_top5_menus as crud_select_local_store_top5_menus,
    insert_or_update_top5_batch as crud_insert_or_update_top5_batch,
    select_local_store_population_data as crud_select_local_store_population_data,
    insert_or_update_population_data_batch as crud_insert_or_update_population_data_batch,
    select_local_store_sub_district_rep_id as crud_select_local_store_sub_district_rep_id,
    insert_or_update_store_info_batch as crud_insert_or_update_store_info_batch,
    select_local_store_sub_district_id as crud_select_local_store_sub_district_id,
    select_local_store_district_id as crud_select_local_store_district_id,
    select_local_store_loc_info_data as crud_select_local_store_loc_info_data,
    insert_or_update_loc_info_data_batch as crud_insert_or_update_loc_info_data_batch,
    select_local_store_loc_info_j_score_data as crud_select_local_store_loc_info_j_score_data,
    insert_or_update_loc_info_j_score_data_batch as crud_insert_or_update_loc_info_j_score_data_batch,
    select_local_store_loc_info_resident_work_pop_data as crud_select_local_store_loc_info_resident_work_pop_data,
    insert_or_update_loc_info_resident_work_pop_data_batch as crud_insert_or_update_loc_info_resident_work_pop_data_batch,
    select_local_store_loc_info_move_pop_data as crud_select_local_store_loc_info_move_pop_data,
    insert_or_update_loc_info_move_pop_data_batch as crud_insert_or_update_loc_info_move_pop_data_batch,
    select_commercial_district_main_detail_category_count_data as crud_select_commercial_district_main_detail_category_count_data,
    insert_or_update_commercial_district_main_category_count_data_batch as crud_insert_or_update_commercial_district_main_category_count_data_batch,
    select_commercial_district_j_score_average_data as crud_select_commercial_district_j_score_average_data,
    insert_or_update_commercial_district_j_score_average_data_batch as crud_insert_or_update_commercial_district_j_score_average_data_batch,
    select_local_store_weekday_time_client_average_sales_data as crud_select_local_store_weekday_time_client_average_sales_data,
    insert_or_update_commercial_district_weekday_time_client_average_sales_data_batch as crud_insert_or_update_commercial_district_weekday_time_client_average_sales_data_batch,
    select_commercial_district_district_average_sales_data_batch as crud_select_commercial_district_district_average_sales_data_batch,
    insert_or_update_commercial_district_district_average_sales_data_batch as crud_insert_or_update_commercial_district_district_average_sales_data_batch,
    select_commercial_district_top5_top3_data_batch as crud_select_commercial_district_top5_top3_data_batch,
    insert_or_update_commercial_district_top5_top3_data_batch as crud_insert_or_update_commercial_district_top5_top3_data_batch,
    select_local_store_loc_info_j_score_average_data as crud_select_local_store_loc_info_j_score_average_data,
    insert_or_update_loc_info_j_score_average_data_batch as crud_insert_or_update_loc_info_j_score_average_data_batch,
    select_commercial_district_j_score_weighted_average_data as crud_select_commercial_district_j_score_weighted_average_data,
    insert_or_update_commercial_district_j_score_weighted_average_data_batch as crud_insert_or_update_commercial_district_j_score_weighted_average_data_batch,
    select_commercial_district_commercial_district_average_data as crud_select_commercial_district_commercial_district_average_data,
    insert_or_update_commercial_district_commercial_district_average_data_batch as crud_insert_or_update_commercial_district_commercial_district_average_data_batch,
    select_report_table,
    select_loc_info_district_hot_place_top5_thread as crud_select_loc_info_district_hot_place_top5_thread,
    insert_or_update_loc_info_district_hot_place_top5_data_thread as crud_insert_or_update_loc_info_district_hot_place_top5_data_thread,
)
from app.db.connect import get_db_connection
from app.schemas.report import (
    LocalStoreBasicInfo,
    LocalStoreCDCommercialDistrict,
    LocalStoreCDDistrictAverageSalesTop5,
    LocalStoreCommercialDistrictJscoreAverage,
    LocalStoreDistrictId,
    LocalStoreLIJSWeightedAverage,
    LocalStoreLocInfoData,
    LocalStoreLocInfoDistrictHotPlaceTop5,
    LocalStoreLocInfoJscoreData,
    LocalStoreMainCategoryCount,
    LocalStoreMappingRepId,
    LocalStoreMappingSubDistrictDetailCategoryId,
    LocalStoreMovePopData,
    LocalStorePopulationData,
    LocalStoreRisingBusinessNTop5SDTop3,
    LocalStoreSubdistrictId,
    LocalStoreTop5Menu,
    Report,
)


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


#################################################################################


# report 컬럼추가 정보 옮기기
def insert_new_report_table_thread(
    old_report_list: List[Report],
    batch_size: int = 5000,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(old_report_list), batch_size):
            batch = old_report_list[i : i + batch_size]
            futures.append(executor.submit(crud_insert_new_report_table, batch))

        with tqdm(
            total=len(futures), desc="Inserting old_report_table batches"
        ) as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)


@time_execution
def migration_old_talbe_to_new_table_report():
    old_report_list: List[Report] = select_report_table()

    print(len(old_report_list))
    print(old_report_list[0])
    print(old_report_list[1])

    insert_new_report_table_thread(old_report_list)


#################################################################################
# 매장 기본 정보 insert 또는 update 함수
def insert_or_update_local_store_info_thread(
    store_info_list: List[LocalStoreBasicInfo], batch_size: int = 2500
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_info_list), batch_size):
            batch = store_info_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_insert_or_update_store_info_batch, batch)
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting local_store batches",
        ):
            future.result()


@time_execution
def insert_or_update_local_store_info():
    local_store_info_list = crud_select_local_store_info()
    insert_or_update_local_store_info_thread(local_store_info_list)


#################################################################################


# 매장 top5 insert 또는 update 함수
def insert_or_update_local_store_top5_menu_thread(
    store_top5_list: List[LocalStoreTop5Menu], batch_size: int = 2500
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_top5_list), batch_size):
            batch = store_top5_list[i : i + batch_size]
            futures.append(executor.submit(crud_insert_or_update_top5_batch, batch))

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting top5 batches"
        ):
            future.result()


def select_local_store_top5_menus_thread(
    local_store_rep_id_list: List[LocalStoreMappingRepId], batch_size: int = 2500
) -> List[LocalStoreTop5Menu]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_rep_id_list), batch_size):
            batch = local_store_rep_id_list[i : i + batch_size]
            futures.append(executor.submit(crud_select_local_store_top5_menus, batch))

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="SELECT TOP5 batches"
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_top5_menu():
    local_store_rep_id_list = crud_select_local_store_sub_district_rep_id()
    local_store_top5_menu_list = select_local_store_top5_menus_thread(
        local_store_rep_id_list
    )
    print(len(local_store_top5_menu_list))
    insert_or_update_local_store_top5_menu_thread(local_store_top5_menu_list)


#################################################################################


# 입지분석 읍/면/동 J_SCORE 평균
def insert_or_update_local_store_loc_info_j_score_average_data_thread(
    store_loc_info_j_score_average_data_list: List[LocalStoreLIJSWeightedAverage],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_j_score_average_data_list), batch_size):
            batch = store_loc_info_j_score_average_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_loc_info_j_score_average_data_batch, batch
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting loc_info_j_score_average batches",
        ):
            future.result()


def select_local_store_loc_info_j_score_average_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreLIJSWeightedAverage]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_local_store_loc_info_j_score_average_data, batch
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT LOC_INFO_j_score_average batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_loc_info_j_score_average_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_loc_info_j_score_avg_list = (
        select_local_store_loc_info_j_score_average_thread(
            local_store_sub_district_id_list
        )
    )
    print(len(local_store_loc_info_j_score_avg_list))
    print(local_store_loc_info_j_score_avg_list[1])
    insert_or_update_local_store_loc_info_j_score_average_data_thread(
        local_store_loc_info_j_score_avg_list
    )


#################################################################################


# 매장 읍/면/동 인구 정보 update 함수
def insert_or_update_local_store_population_data_thread(
    store_population_data_list: List[LocalStorePopulationData], batch_size: int = 2500
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_population_data_list), batch_size):
            batch = store_population_data_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_insert_or_update_population_data_batch, batch)
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting population batches",
        ):
            future.result()


@time_execution
def insert_or_update_local_store_population_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_population_data_list = crud_select_local_store_population_data(
        local_store_sub_district_id_list
    )
    insert_or_update_local_store_population_data_thread(
        local_store_population_data_list
    )


#################################################################################


# 입지분석 데이터 주거인구, 직장인구, 유동인구, 업소수, 소득
def insert_or_update_local_store_loc_info_data_thread(
    store_loc_info_data_list: List[LocalStoreLocInfoData], batch_size: int = 2500
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_data_list), batch_size):
            batch = store_loc_info_data_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_insert_or_update_loc_info_data_batch, batch)
            )

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting loc_info batches"
        ):
            future.result()


def select_local_store_loc_info_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreLocInfoData]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_select_local_store_loc_info_data, batch)
            )

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="SELECT LOC_INFO batches"
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_loc_info_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_loc_info_list = select_local_store_loc_info_thread(
        local_store_sub_district_id_list
    )
    print(len(local_store_loc_info_list))
    print(local_store_loc_info_list[1])
    insert_or_update_local_store_loc_info_data_thread(local_store_loc_info_list)


#################################################################################


# 입지분석 데이터 주거인구, 직장인구, 유동인구, 업소수, 소득, mz인구, 평균 소비, 평균 소득, 매장 평균 매출 JSCORE
def insert_or_update_local_store_loc_info_j_score_data_thread(
    store_loc_info_j_score_data_list: List[LocalStoreLocInfoJscoreData],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_j_score_data_list), batch_size):
            batch = store_loc_info_j_score_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_loc_info_j_score_data_batch, batch
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting loc_info j_score batches",
        ):
            future.result()


def select_local_store_loc_info_j_score_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreLocInfoData]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_select_local_store_loc_info_j_score_data, batch)
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT LOC_INFO_j_score batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_loc_info_j_score_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_loc_info_j_score_list = select_local_store_loc_info_j_score_thread(
        local_store_sub_district_id_list
    )
    # print(len(local_store_loc_info_j_score_list))
    print(local_store_loc_info_j_score_list[1])
    insert_or_update_local_store_loc_info_j_score_data_thread(
        local_store_loc_info_j_score_list
    )


#################################################################################


# 입지분석 주거인구, 직장인구, 수/비율
def insert_or_update_local_store_loc_info_resident_work_pop_data_thread(
    store_loc_info_resident_work_pop_data_list: List[LocalStoreLocInfoData],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_resident_work_pop_data_list), batch_size):
            batch = store_loc_info_resident_work_pop_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_loc_info_resident_work_pop_data_batch, batch
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting resident work_pop batches",
        ):
            future.result()


def select_local_store_loc_info_resident_work_pop_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreLocInfoData]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_local_store_loc_info_resident_work_pop_data, batch
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT LOC_INFO_resident_work_pop batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_loc_info_resident_work_pop_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_loc_info_resident_work_pop_list = (
        select_local_store_loc_info_resident_work_pop_thread(
            local_store_sub_district_id_list
        )
    )
    print(len(local_store_loc_info_resident_work_pop_list))
    print(local_store_loc_info_resident_work_pop_list[1])
    insert_or_update_local_store_loc_info_resident_work_pop_data_thread(
        local_store_loc_info_resident_work_pop_list
    )


#################################################################################


# 입지분석 읍/면/동 유동인구, 시/도 평균 유동인구
def insert_or_update_local_store_loc_info_move_pop_data_thread(
    store_loc_info_move_pop_data_list: List[LocalStoreMovePopData],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_move_pop_data_list), batch_size):
            batch = store_loc_info_move_pop_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_loc_info_move_pop_data_batch, batch
                )
            )

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting move_pop batches"
        ):
            future.result()


def select_local_store_loc_info_move_pop_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreMovePopData]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_select_local_store_loc_info_move_pop_data, batch)
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT LOC_INFO_move_pop batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_local_store_loc_info_move_pop_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    local_store_loc_info_move_pop_list = select_local_store_loc_info_move_pop_thread(
        local_store_sub_district_id_list
    )
    print(len(local_store_loc_info_move_pop_list))
    print(local_store_loc_info_move_pop_list[1])
    insert_or_update_local_store_loc_info_move_pop_data_thread(
        local_store_loc_info_move_pop_list
    )


#################################################################################
# 상권분석 읍/면/동 소분류 J_Score 가중치 평균 합


def insert_or_update_commercial_district_j_score_weighted_average_data_thread(
    store_loc_info_cd_mc_count_data_list: List[
        LocalStoreCommercialDistrictJscoreAverage
    ],
    batch_size: int = 1000,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_j_score_weighted_average_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd jscore average batches",
        ):
            future.result()


def select_commercial_district_j_score_weighted_average_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 10000,
) -> List[LocalStoreMainCategoryCount]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_j_score_weighted_average_data,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT cd jscore weighted average batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_j_score_weighted_average_data():
    local_store_sub_district_detail_category_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ] = crud_select_local_store_mp_detail_cateogry_id()

    print(len(local_store_sub_district_detail_category_id_list))
    print(local_store_sub_district_detail_category_id_list[1])

    commercial_district_j_score_weighted_average_list = (
        select_commercial_district_j_score_weighted_average_thread(
            local_store_sub_district_detail_category_id_list
        )
    )
    print(len(commercial_district_j_score_weighted_average_list))
    print(commercial_district_j_score_weighted_average_list[0])
    print(commercial_district_j_score_weighted_average_list[1])
    print(commercial_district_j_score_weighted_average_list[2])
    print(commercial_district_j_score_weighted_average_list[3])
    insert_or_update_commercial_district_j_score_weighted_average_data_thread(
        commercial_district_j_score_weighted_average_list
    )


#################################################################################


# 상권 분석 대분류 갯수
def insert_or_update_commercial_district_main_detail_category_count_data_thread(
    store_loc_info_cd_mc_count_data_list: List[LocalStoreMainCategoryCount],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_main_category_count_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd main_category batches",
        ):
            future.result()


def select_commercial_district_main_detail_category_count_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreMainCategoryCount]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_main_detail_category_count_data,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="crud_select_commercial_district_main_detail_category_count_data",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_main_detail_category_count_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    commercial_district_main_detail_category_count_list = (
        select_commercial_district_main_detail_category_count_thread(
            local_store_sub_district_id_list
        )
    )
    # print(len(commercial_district_main_detail_category_count_list))
    # print(commercial_district_main_detail_category_count_list[1])
    insert_or_update_commercial_district_main_detail_category_count_data_thread(
        commercial_district_main_detail_category_count_list
    )


#################################################################################


# 상권 분석 JSCORE 시장규모, 평균매출, 결제건수, 밀집도, 결제금액 평균
def insert_or_update_commercial_district_j_score_average_data_thread(
    store_loc_info_cd_mc_count_data_list: List[
        LocalStoreCommercialDistrictJscoreAverage
    ],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_j_score_average_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd jscore average batches",
        ):
            future.result()


def select_commercial_district_j_score_average_thread(
    local_store_sub_district_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ],
    batch_size: int = 10000,
) -> List[LocalStoreMainCategoryCount]:
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_j_score_average_data,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT cd jscore average batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_j_score_average_data():
    local_store_sub_district_detail_category_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ] = crud_select_local_store_mp_detail_cateogry_id()

    # print(len(local_store_sub_district_detail_category_id_list))
    # print(local_store_sub_district_detail_category_id_list[1])

    commercial_district_j_score_average_list = (
        select_commercial_district_j_score_average_thread(
            local_store_sub_district_detail_category_id_list
        )
    )
    # print(len(commercial_district_j_score_average_list))
    # print(commercial_district_j_score_average_list[0])
    insert_or_update_commercial_district_j_score_average_data_thread(
        commercial_district_j_score_average_list
    )


#################################################################################


# 매장 상권분석 동별 소분류별 요일,시간대 매출 비중
def insert_or_update_local_store_weekday_time_client_average_sales_thread(
    store_top5_list: List[LocalStoreTop5Menu], batch_size: int = 2500
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_top5_list), batch_size):
            batch = store_top5_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_weekday_time_client_average_sales_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting weekday_time_average_sales batches",
        ):
            future.result()


def select_local_store_weekday_time_client_average_sales_thread(
    local_store_rep_id_list: List[LocalStoreMappingRepId], batch_size: int = 2500
) -> List[LocalStoreTop5Menu]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_rep_id_list), batch_size):
            batch = local_store_rep_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_local_store_weekday_time_client_average_sales_data,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT weekday time average sales batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_weekday_time_client_average_sales():
    local_store_sub_district_rep_id_list = crud_select_local_store_sub_district_rep_id()
    local_store_weekday_time_client_average_sales_list = (
        select_local_store_weekday_time_client_average_sales_thread(
            local_store_sub_district_rep_id_list
        )
    )

    # print(len(local_store_weekday_time_client_average_sales_list))
    # print(local_store_weekday_time_client_average_sales_list[0])
    # print(local_store_weekday_time_client_average_sales_list[1])
    # print(local_store_weekday_time_client_average_sales_list[2])

    insert_or_update_local_store_weekday_time_client_average_sales_thread(
        local_store_weekday_time_client_average_sales_list
    )


#################################################################################


# 상권 분석 시/군/구에서 매핑된 소분류들 매출합 TOP5
def insert_or_update_commercial_district_district_average_sales_data_thread(
    store_loc_info_cd_mc_count_data_list: List[LocalStoreCDDistrictAverageSalesTop5],
    batch_size: int = 1000,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_district_average_sales_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd district average sales batches",
        ):
            future.result()


def select_commercial_district_district_average_sales_thread(
    local_store_sub_district_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ],
    batch_size: int = 2500,
) -> List[LocalStoreCDDistrictAverageSalesTop5]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_district_average_sales_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT cd district average sales batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_district_average_sales_data():
    local_store_sub_district_detail_category_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ] = crud_select_local_store_mp_detail_cateogry_id()

    # print(len(local_store_sub_district_detail_category_id_list))
    # print(local_store_sub_district_detail_category_id_list[1])

    commercial_district_district_average_sales_list = (
        select_commercial_district_district_average_sales_thread(
            local_store_sub_district_detail_category_id_list
        )
    )
    print(len(commercial_district_district_average_sales_list))

    insert_or_update_commercial_district_district_average_sales_data_thread(
        commercial_district_district_average_sales_list
    )


#################################################################################


# 뜨는 업종 전국 TOP5, 읍/면/동 TOP3 (시/군/구,읍/면/동,소분류명,증가율)
def insert_or_update_commercial_district_top5_top3_data_thread(
    store_loc_info_cd_mc_count_data_list: List[LocalStoreRisingBusinessNTop5SDTop3],
    batch_size: int = 1000,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_top5_top3_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting cd top5 top3 batches",
        ):
            future.result()


def select_commercial_district_top5_top3_thread(
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreCDDistrictAverageSalesTop5]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_top5_top3_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT cd top5 top3 batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_top5_top3_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )

    # print(len(local_store_sub_district_id_list))
    # print(local_store_sub_district_id_list[1])

    commercial_district_top5_top3_list = select_commercial_district_top5_top3_thread(
        local_store_sub_district_id_list
    )
    # print(len(commercial_district_top5_top3_list))
    # print(local_store_sub_district_id_list[0])

    insert_or_update_commercial_district_top5_top3_data_thread(
        commercial_district_top5_top3_list
    )


#################################################################################


# 상권 분석 읍/면/동 소분류 상권분석
def insert_or_update_commercial_district_commercial_district_average_data_thread(
    store_loc_info_cd_mc_count_data_list: List[
        LocalStoreCommercialDistrictJscoreAverage
    ],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_commercial_district_commercial_district_average_data_batch,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting LocalStoreCommercialDistrictJscoreAverage batches",
        ):
            future.result()


def select_commercial_district_commercial_district_average_thread(
    local_store_sub_district_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ],
    batch_size: int = 10000,
) -> List[LocalStoreCDCommercialDistrict]:
    results = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_commercial_district_commercial_district_average_data,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT cd commercial_district average batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_commercial_district_commercial_district_average_data():
    local_store_sub_district_detail_category_id_list: List[
        LocalStoreMappingSubDistrictDetailCategoryId
    ] = crud_select_local_store_mp_detail_cateogry_id()

    commercial_district_commercial_district_average_list = (
        select_commercial_district_commercial_district_average_thread(
            local_store_sub_district_detail_category_id_list
        )
    )

    print(len(commercial_district_commercial_district_average_list))
    print(commercial_district_commercial_district_average_list[0])
    insert_or_update_commercial_district_commercial_district_average_data_thread(
        commercial_district_commercial_district_average_list
    )


#################################################################################

# 입지분석 시/군/구 핫플레이스 TOP5 (읍/면/동, 평균유동인구, 매장평균매출, JSCORE점수)
def insert_or_update_loc_info_district_hot_place_top5_data_thread(
    store_loc_info_cd_mc_count_data_list: List[LocalStoreRisingBusinessNTop5SDTop3],
    batch_size: int = 2500,
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_loc_info_cd_mc_count_data_list), batch_size):
            batch = store_loc_info_cd_mc_count_data_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_insert_or_update_loc_info_district_hot_place_top5_data_thread,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Inserting loc info hot place top5 batches",
        ):
            future.result()


def select_loc_info_district_hot_place_top5_thread(
    local_store_sub_district_id_list: List[LocalStoreDistrictId],
    batch_size: int = 2500,
) -> List[LocalStoreLocInfoDistrictHotPlaceTop5]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_sub_district_id_list), batch_size):
            batch = local_store_sub_district_id_list[i : i + batch_size]
            futures.append(
                executor.submit(
                    crud_select_loc_info_district_hot_place_top5_thread,
                    batch,
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SELECT LOC_INFO Hot Place top5 batches",
        ):
            try:
                batch_result = future.result()
                results.extend(batch_result)
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                continue

    return results


@time_execution
def insert_or_update_loc_info_district_hot_place_top5_data():
    local_store_district_id_list: List[LocalStoreDistrictId] = (
        crud_select_local_store_district_id()
    )

    print(len(local_store_district_id_list))
    print(local_store_district_id_list[0])

    loc_info_district_hot_place_top5_list = (
        select_loc_info_district_hot_place_top5_thread(local_store_district_id_list)
    )
    print(len(loc_info_district_hot_place_top5_list))
    print(loc_info_district_hot_place_top5_list[0])

    insert_or_update_loc_info_district_hot_place_top5_data_thread(
        loc_info_district_hot_place_top5_list
    )


#################################################################################

if __name__ == "__main__":
    # migration_old_talbe_to_new_table_report()  # 874.75 seconds

    # insert_or_update_local_store_info()  #   142.06 seconds
    # insert_or_update_local_store_loc_info_j_score_average_data()  # 550.52 seconds
    # insert_or_update_local_store_population_data()  # 171.43 seconds
    # insert_or_update_local_store_loc_info_data()  # 112.37 seconds
    # insert_or_update_local_store_loc_info_j_score_data()  # 162.84 seconds
    # insert_or_update_local_store_loc_info_resident_work_pop_data()  # 104.71 seconds
    # insert_or_update_local_store_loc_info_move_pop_data()  # 555.05 seconds
    # insert_or_update_loc_info_district_hot_place_top5_data() # 776.31 seconds
    # # 약 30분

    # insert_or_update_local_store_top5_menu()  # 248.23 seconds
    # insert_or_update_commercial_district_j_score_weighted_average_data()  # 9990.48 seconds
    # insert_or_update_commercial_district_main_detail_category_count_data()  # 622.11 seconds
    # insert_or_update_commercial_district_weekday_time_client_average_sales()  #  294.81 seconds
    # insert_or_update_commercial_district_top5_top3_data()  # 777.15 seconds
    # insert_or_update_commercial_district_j_score_average_data()  #  68437.64 seconds
    # insert_or_update_commercial_district_district_average_sales_data()  #  5634.10 seconds
    # insert_or_update_commercial_district_commercial_district_average_data()  # 78961.68 seconds
    # 약 46시간
    print("END!!!!!!!!!!!!!!!")
