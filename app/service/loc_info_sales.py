from app.crud.loc_info_sales import select_all_factor
import pandas as pd
import statsmodels.api as sm

def sales_analyze():
    all_factor = select_all_factor()
    print(all_factor)

    # 데이터프레임 생성 및 None 값 제거
    df = pd.DataFrame(all_factor).dropna()

    # 독립 변수(X)와 종속 변수(y) 분리
    X = df[['SHOP', 'MOVE_POP', 'WORK_POP', 'INCOME', 'SPEND', 'HOUSE', 'RESIDENT']]
    y = df['SALES']

    # 상수 추가 (회귀 분석에 필요)
    X = sm.add_constant(X)

    # 회귀 모델 생성 및 적합
    model = sm.OLS(y, X).fit()

    # 회귀 분석 결과 출력
    print(model.summary())

if __name__ == "__main__":
    sales_analyze()