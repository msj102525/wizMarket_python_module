from app.crud.local_store_review_menu import (
    test_store_info as crud_test_store_info,
    store_info as crud_store_info,
    update_store_review as crud_update_store_review,
)

import pymysql
from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from datetime import datetime
import os, time
from tqdm import tqdm
import sys
from app.crud.loc_info import *
from datetime import datetime
from PIL import Image
from io import BytesIO
import base64
from concurrent.futures import ThreadPoolExecutor
import pymysql
from tqdm import tqdm



def get_kakao_review():
    start_time = datetime.now()
    print(f"Start Time: {start_time}")

    data = crud_store_info()  # 모든 데이터 가져오기
    connection = get_db_connection()  # 연결 한 번만 생성

    try:
        # 병렬 처리
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for item in data:
                city_name = item['city_name']
                district_name = item['district_name']
                sub_district_name = item['sub_district_name']
                store_name = item['STORE_NAME']
                store_business_number = item['STORE_BUSINESS_NUMBER']

                # 각 작업을 병렬로 실행
                future = executor.submit(
                    crawl_keyword,
                    city_name, district_name, sub_district_name, store_name, store_business_number, connection
                )
                futures.append(future)

            # 모든 작업 완료 대기
            for future in futures:
                future.result()

    finally:
        close_connection(connection)  # 모든 작업이 끝난 후 연결 닫기

    end_time = datetime.now()
    print(f"End Time: {end_time}")
    print(f"Total Time Taken: {end_time - start_time}")




def crawl_keyword(city_name, district_name, sub_district_name, store_name, store_business_number, connection):

    # 글로벌 드라이버 사용
    options = Options()
    options.add_argument("--headless")  # 헤드리스 모드 활성화
    options.add_argument("--disable-gpu")
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920x1080")  # 해상도 설정
    options.add_argument("--start-maximized")

    # WebDriver Manager를 이용해 ChromeDriver 자동 관리
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get("https://map.kakao.com/?nil_profile=title&nil_src=local")

        # id가 search.keyword.query인 input 요소를 찾음
        search_input = driver.find_element(By.ID, "search.keyword.query")
        
        # 전달받은 region_data 값을 입력
        search_input.clear()  # 기존 입력 값이 있을 경우 삭제
        full_name = f"{city_name} {district_name} {sub_district_name} {store_name}"
        search_input.send_keys(full_name)

        # Enter 키를 눌러 검색 실행
        search_input.send_keys(Keys.RETURN)

        # 잠시 대기 (검색 결과 로딩을 기다리기 위해)
        time.sleep(3)

        # id가 info.search.place.list인 ul 태그의 첫 번째 li 태그를 찾음
        search_results = driver.find_element(By.ID, "info.search.place.list")
        first_li = search_results.find_element(By.TAG_NAME, "li")
        
        # li 태그 내의 div 클래스가 info_item인 요소 찾기
        info_item = first_li.find_element(By.CLASS_NAME, "info_item")
        
        # info_item 내의 div 클래스가 contact clickArea인 요소에서 첫 번째 a 태그 클릭
        contact_area = info_item.find_element(By.CLASS_NAME, "contact.clickArea")
        contact_link = contact_area.find_element(By.TAG_NAME, "a")
        moreview_url = contact_link.get_attribute("href")
        driver.get(moreview_url)

        time.sleep(3)

        # span 태그 아래의 모든 a 태그 찾기
        links = driver.find_elements(By.CSS_SELECTOR, "#mArticle > div.cont_essential > div:nth-child(1) > div.place_details > div > div.location_evaluation > a:nth-child(3)")

        if links:
            review = links[0].text
            # 줄바꿈으로 나누기
            review_lines = review.split("\n")

            # review_score와 review_count 분리
            if len(review_lines) > 1:
                try:
                    # "후기 4.0"에서 숫자만 추출하여 float로 변환
                    kakao_review_score = float(review_lines[0].split(" ")[1])  # "4.0" -> 4.0
                    
                    # "(7)"에서 숫자만 추출하여 int로 변환
                    kakao_review_count = int(review_lines[1].strip("()"))  # "(7)" -> 7
                except ValueError:
                    # 변환 실패 시 None 처리
                    kakao_review_score = None
                    kakao_review_count = None
            else:
                kakao_review_score = None
                kakao_review_count = None
        else:
            kakao_review_score = None
            kakao_review_count = None

        # #mArticle > div.cont_menu > ul 요소 찾기
        try:
            menu_list = driver.find_element(By.CSS_SELECTOR, "#mArticle > div.cont_menu > ul")

            # 내부 요소(li 태그들) 가져오기
            menu_items = menu_list.find_elements(By.TAG_NAME, "li")

            # 최대 3개의 메뉴와 가격을 저장
            menu_data = {}
            for index, item in enumerate(menu_items[:3], start=1):  # 최대 3개까지 반복
                menu_details = item.text.split("\n")  # 이름과 가격 분리
                menu_name = menu_details[0] if len(menu_details) > 0 else None

                # 가격 문자열을 정수로 변환
                if len(menu_details) > 1 and menu_details[1]:
                    try:
                        menu_price = int(menu_details[1].replace(",", ""))  # 쉼표 제거 후 int로 변환
                    except ValueError:
                        menu_price = None  # 변환 실패 시 None
                else:
                    menu_price = None

                menu_data[f"menu_{index}"] = menu_name
                menu_data[f"menu_{index}_price"] = menu_price

            # 3개 미만일 경우 남은 항목을 None으로 채우기
            for index in range(len(menu_items) + 1, 4):
                menu_data[f"menu_{index}"] = None
                menu_data[f"menu_{index}_price"] = None

        except Exception:
            # 메뉴 리스트를 찾지 못한 경우 기본 값을 설정
            menu_data = {
                "menu_1": None, "menu_1_price": None,
                "menu_2": None, "menu_2_price": None,
                "menu_3": None, "menu_3_price": None,
            }

        # 기존 데이터
        data = {
            "kakao_review_score": kakao_review_score,
            "kakao_review_count": kakao_review_count,
        }

        # menu_data를 data에 병합
        data.update(menu_data)
        data["store_business_number"] = store_business_number

        # 최종 데이터 출력
        # print(data)
        crud_update_store_review(connection, data)

    finally:
        driver.quit()





if __name__ == "__main__":
    get_kakao_review()