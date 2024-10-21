from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time  # 내장 time 모듈을 가져옵니다.
from typing import Any, Callable, List
from app.crud.report import (
    select_local_store_info as crud_select_local_store_info,
    insert_or_update_top5_batch as crud_insert_or_update_top5_batch,
    select_local_store_rep_id as crud_select_local_store_rep_id,
    insert_or_update_store_info_batch as crud_insert_or_update_store_info_batch,
)
from app.schemas.report import LocalStoreBasicInfo, LocalStoreTop5Menu


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


@time_execution
def insert_or_update_local_store_top5_menu():
    local_store_top5_menu = crud_select_local_store_rep_id()
    # print(local_store_top5_menu)
    insert_or_update_local_store_top5_menu_thread(local_store_top5_menu)


#################################################################################


if __name__ == "__main__":
    # insert_or_update_local_store_info()  # 438.92 seconds
    insert_or_update_local_store_top5_menu()  # 26.76 seconds
    print("END!!!!!!!!!!!!!!!")
