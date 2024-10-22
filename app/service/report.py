from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import logging
from threading import local
from tqdm import tqdm
import time  # 내장 time 모듈을 가져옵니다.
from typing import Any, Callable, List
from app.crud.report import (
    select_local_store_info as crud_select_local_store_info,
    insert_or_update_top5_batch as crud_insert_or_update_top5_batch,
    select_local_store_population_data as crud_select_local_store_population_data,
    insert_or_update_population_data_batch as crud_insert_or_update_population_data_batch,
    select_local_store_rep_id as crud_select_local_store_rep_id,
    insert_or_update_store_info_batch as crud_insert_or_update_store_info_batch,
    select_local_store_sub_district_id as crud_select_local_store_sub_district_id,
    select_local_store_top5_menus,
)
from app.db.connect import get_db_connection
from app.schemas.report import (
    LocalStoreBasicInfo,
    LocalStoreMappingRepId,
    LocalStorePopulationData,
    LocalStoreSubdistrictId,
    LocalStoreTop5Menu,
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


# 매장 기본 정보 insert 또는 update 함수
def insert_or_update_local_store_info_thread(
    store_info_list: List[LocalStoreBasicInfo], batch_size: int = 5000
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_info_list), batch_size):
            batch = store_info_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_insert_or_update_store_info_batch, batch)
            )

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting batches"
        ):
            future.result()


@time_execution
def insert_or_update_local_store_info():
    local_store_info_list = crud_select_local_store_info()
    insert_or_update_local_store_info_thread(local_store_info_list)


#################################################################################


# 매장 top5 insert 또는 update 함수
def insert_or_update_local_store_top5_menu_thread(
    store_top5_list: List[LocalStoreTop5Menu], batch_size: int = 5000
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_top5_list), batch_size):
            batch = store_top5_list[i : i + batch_size]
            futures.append(executor.submit(crud_insert_or_update_top5_batch, batch))

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting batches"
        ):
            future.result()


# thread_local = local()


# @contextmanager
# def get_thread_db_connection():
#     """Thread-safe database connection manager"""
#     if not hasattr(thread_local, "connection"):
#         thread_local.connection = get_db_connection()
#     try:
#         yield thread_local.connection
#     except Exception as e:
#         thread_local.connection.rollback()
#         raise e


def select_local_store_top5_menus_thread(
    local_store_rep_id_list: List[LocalStoreMappingRepId], batch_size: int = 5000
) -> List[LocalStoreTop5Menu]:
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(local_store_rep_id_list), batch_size):
            batch = local_store_rep_id_list[i : i + batch_size]
            futures.append(executor.submit(select_local_store_top5_menus, batch))

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
    local_store_rep_id_list = crud_select_local_store_rep_id()
    local_store_top5_menu_list = select_local_store_top5_menus_thread(
        local_store_rep_id_list
    )
    print(len(local_store_top5_menu_list))
    insert_or_update_local_store_top5_menu_thread(local_store_top5_menu_list)


#################################################################################


# 매장 읍/면/동 인구 정보 update 함수
def insert_or_update_local_store_population_data_thread(
    store_population_data_list: List[LocalStorePopulationData], batch_size: int = 5000
) -> None:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for i in range(0, len(store_population_data_list), batch_size):
            batch = store_population_data_list[i : i + batch_size]
            futures.append(
                executor.submit(crud_insert_or_update_population_data_batch, batch)
            )

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Inserting batches"
        ):
            future.result()


@time_execution
def insert_or_update_local_store_population_data():
    local_store_sub_district_id_list: List[LocalStoreSubdistrictId] = (
        crud_select_local_store_sub_district_id()
    )
    # print(local_store_sub_district_id_list[1])
    local_store_population_data_list = crud_select_local_store_population_data(
        local_store_sub_district_id_list
    )
    # print(local_store_population_data_list[1])
    insert_or_update_local_store_population_data_thread(
        local_store_population_data_list
    )


#################################################################################

if __name__ == "__main__":
    # insert_or_update_local_store_info()  # 438.92 seconds
    # insert_or_update_local_store_top5_menu()  # 2916.64 seconds / 57.32 seconds
    # insert_or_update_local_store_population_data()  # 281.83 seconds
    print("END!!!!!!!!!!!!!!!")
