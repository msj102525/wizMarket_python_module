from matplotlib import font_manager, rc
from app.crud.draw_plot import select_social_data
import matplotlib.pyplot as plt
import pandas as pd
from openpyxl.drawing.image import Image as OpenpyxlImage
import os
from dotenv import load_dotenv


# 산점도 그리기
load_dotenv()

# ROOT_PATH 가져오기
root_path = os.getenv("ROOT_PATH")

font_path = os.path.join(root_path, "app/font/malgun.ttf")
font_name = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font_name)

def social_data():
    data = select_social_data()
    return data

def save_to_excel_with_scatter():
    data = social_data()
    df = pd.DataFrame(data)
    
    with pd.ExcelWriter('C:/workspace/social_data.xlsx', engine='openpyxl') as writer:
        unique_categories = df['BIZ_DETAIL_CATEGORY_NAME'].unique()
        
        for category in unique_categories:
            sheet_name = category.replace("/", "_")
            category_df = df[df['BIZ_DETAIL_CATEGORY_NAME'] == category]
            category_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # 산점도 생성
            plt.figure()
            plt.scatter(category_df.index, category_df['MARKET_SIZE'])
            plt.xlabel('Index')
            plt.ylabel('Market Size')
            plt.title(f'Scatter Plot of Market Size for {category}')
            
            # 산점도를 엑셀 시트에 추가
            fig_path = f'C:/workspace/{sheet_name}_scatter.png'
            plt.savefig(fig_path)
            plt.close()

            # 이미지 삽입
            sheet = writer.sheets[sheet_name]
            img = OpenpyxlImage(fig_path)
            img.anchor = 'H2'
            sheet.add_image(img)
    
    return 'C:/workspace/social_data.xlsx'