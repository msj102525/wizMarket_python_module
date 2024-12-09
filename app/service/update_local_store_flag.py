import pandas as pd
from app.crud.loc_store import update_local_store_flag_column



def update_flag_jsam_from_excel():
    # 엑셀 파일 경로
    file_path = r"C:\Users\jyes_semin\Desktop\Data\locStoreFlag\JSAM.xlsx"

    # 엑셀 파일 불러오기
    try:
        # I열만 불러오기, 헤더가 1행에 있다고 가정
        df = pd.read_excel(file_path, usecols="I")

        # 헤더를 '매장코드'로 지정 (첫 번째 행이 헤더)
        df.columns = ['매장코드']

        # 값이 있는 행만 필터링
        df_filtered = df[df['매장코드'].notna()]

        # 각 매장코드에 대해 JSAM 업데이트 실행
        for store_business_number in df_filtered['매장코드']:
            update_local_store_flag_column(store_business_number)

        print(f"{len(df_filtered)}개의 매장코드가 업데이트되었습니다.")

    except Exception as e:
        print(f"파일을 불러오는 중 오류가 발생했습니다: {e}")

if __name__=="__main__":
    update_flag_jsam_from_excel()
