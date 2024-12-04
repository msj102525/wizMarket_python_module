from app.crud.copy_local_store import (
    select_local_store_data as crud_select_local_store_data,
    insert_local_store_temp_data as crud_insert_local_store_temp_data,
    delete_local_store_temp, drop_local_store, rename_local_store_temp_to_local_store
)
from app.db.connect import *

def copy_local_store_data():
    # delete_local_store_temp()
    # data = crud_select_local_store_data()
    # print(data)
    # crud_insert_local_store_temp_data(data)
    drop_local_store()
    rename_local_store_temp_to_local_store()



if __name__ == "__main__":
    copy_local_store_data()