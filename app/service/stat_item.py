from typing import List
from app.crud.biz_detail_category import (
    select_all_biz_detail_category_id as crud_select_all_biz_detail_category_id,
)
from app.crud.stat_item import (
    insert_stat_item,
    insert_stat_item_add_detail_category as crud_insert_stat_item_add_detail_category,
)
from app.schemas.biz_detail_category import BizDetailCategoryId


def insert_stat_item_from_loc_info(
    table_name,
    column_name,
):
    # 삽입할 데이터
    table_name = table_name
    column_name = column_name
    reference_id = 5

    # stat_item 테이블에 데이터 삽입
    insert_stat_item(table_name, column_name, reference_id)


# 함수 실행
if __name__ == "__main__":
    insert_stat_item_from_loc_info("population", "mz_population")


def insert_stat_item_from_commercial_district_by_detail_category(
    table_name,
    column_name,
):

    # detail_cateogry 종류

    search_detail_category_id: List[BizDetailCategoryId] = (
        crud_select_all_biz_detail_category_id()
    )

    # 삽입할 데이터
    table_name = table_name
    column_name = column_name
    reference_id = 1

    # stat_item 테이블에 데이터 삽입
    crud_insert_stat_item_add_detail_category(
        table_name, column_name, reference_id, search_detail_category_id
    )


# 함수 실행
if __name__ == "__main__":
    # insert_stat_item_from_loc_info("population", "mz_population")
    # insert_stat_item_from_commercial_district_by_detail_category(
    #     "commercial_district", "national_density"
    # )
    pass



