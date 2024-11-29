import pandas as pd
from app.crud.count_matching_category_store import how_to_count
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc

# 소분류별 매장 수 엑셀파일 업데이트
def loc_store_count_by_small_category():
    # 파일 경로
    file_path = r"C:\Users\jyes_semin\Desktop\Data\mapping count.xlsx"
    try:
        # 엑셀 파일 읽기
        data = pd.read_excel(file_path)

        # '소분류명' 열 확인 및 가져오기
        if '소분류명' not in data.columns:
            raise ValueError("소분류명 열을 찾을 수 없습니다. 엑셀 파일의 헤더를 확인하세요.")
        d_column = data['소분류명']

        # '매장 수' 열 초기화 (기존 열 이름 활용)
        if '매장 수' not in data.columns:
            raise ValueError("매장 수 열을 찾을 수 없습니다. 엑셀 파일의 헤더를 확인하세요.")
        data['매장 수'] = None  # 기존 '매장 수' 열에 값을 덮어씀

        # mapping을 데이터프레임으로 변환
        mapping = how_to_count()
        mapping_df = pd.DataFrame(mapping)

        # DB에서 조회한 SMALL_CATEGORY_NAME 목록
        db_categories = set(mapping_df['SMALL_CATEGORY_NAME'])

        # 엑셀 파일에서 소분류명 목록
        excel_categories = set(d_column)

        # DB에 있고 엑셀에 없는 소분류명 찾기
        missing_categories = db_categories - excel_categories

        # '소분류명'과 매핑된 값 추가
        for index, row in data.iterrows():
            category_name = row['소분류명']
            match = mapping_df[mapping_df['SMALL_CATEGORY_NAME'] == category_name]

            if not match.empty:
                # category_count 값을 '매장 수' 열에 삽입
                data.at[index, '매장 수'] = match.iloc[0]['category_count']

        # DB에 있는데 엑셀에 없는 소분류명 추가
        for missing_category in missing_categories:
            match = mapping_df[mapping_df['SMALL_CATEGORY_NAME'] == missing_category]
            if not match.empty:
                # 새로 추가할 데이터
                new_row = {
                    '소분류명': missing_category,
                    '매장 수': match.iloc[0]['category_count']
                }
                data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)

        # 결과 엑셀 저장
        output_path = file_path.replace(".xlsx", "_updated.xlsx")
        data.to_excel(output_path, index=False)
        print(f"파일이 성공적으로 저장되었습니다: {output_path}")

    except FileNotFoundError:
        print("파일을 찾을 수 없습니다. 경로를 확인하세요.")
    except ValueError as ve:
        print(f"값 처리 오류: {ve}")
    except Exception as e:
        print(f"파일을 읽는 중 오류가 발생했습니다: {e}")



# 대분류별 매장 수 파이 차트 그리기
def draw_pichart_by_large_category():
    # 파일 경로
    file_path = r"C:\Users\jyes_semin\Desktop\Data\loc_store_count_by_small_category.xlsx"
    font_path = r"C:\workspace\python\wizMarket_admin_be\app\font\malgun.ttf"  # 사용자 지정 폰트 경로

    try:
        # 한글 폰트 설정
        font_name = font_manager.FontProperties(fname=font_path).get_name()
        rc('font', family=font_name)
        plt.rcParams['axes.unicode_minus'] = False

        # 엑셀 파일 읽기
        data = pd.read_excel(file_path, usecols=["대분류명", "매장 수"])

        # '대분류명'과 '매장 수' 열 확인
        if "대분류명" not in data.columns or "매장 수" not in data.columns:
            raise ValueError("엑셀 파일에서 '대분류명' 또는 '매장 수' 열을 찾을 수 없습니다.")

        # 대분류명별 매장 수 합계 계산
        category_sums = data.groupby("대분류명")["매장 수"].sum()

        # 퍼센트와 매장 수를 함께 표시하는 함수
        def autopct_with_values(pct, values):
            total = sum(values)
            count = int(round(pct * total / 100.0))
            return f"{pct:.1f}%\n({count}개)"

        # 파이 차트 그리기
        plt.figure(figsize=(8, 8))
        category_sums.plot.pie(
            autopct=lambda pct: autopct_with_values(pct, category_sums),
            startangle=140,
            legend=False
        )
        plt.title("대분류별 매장 수 비율")
        plt.ylabel("")  # y축 레이블 제거

        # 이미지 저장
        output_path = file_path.replace(".xlsx", "_large_category_piechart.png")
        plt.savefig(output_path, format='png', bbox_inches="tight")
        plt.close()
        
        print(f"파이 차트가 성공적으로 저장되었습니다: {output_path}")
    
    except FileNotFoundError:
        print("파일을 찾을 수 없습니다. 경로를 확인하세요.")
    except ValueError as ve:
        print(f"값 처리 오류: {ve}")
    except Exception as e:
        print(f"파이 차트를 그리는 중 오류가 발생했습니다: {e}")



if __name__ == "__main__":
    # loc_store_count_by_small_category()
    draw_pichart_by_large_category()