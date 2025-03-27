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
from app.db.connect import get_db_connection, close_connection

from app.crud.loc_info import *
from datetime import datetime


###### 크롤링 #########
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))


def crawl_keyword(region_data, connection):

    # 초기 값 세팅
    reference_id = 3
    year_month = datetime(2024, 12, 1).date()

    # 글로벌 드라이버 사용
    options = Options()
    options.add_argument("--start-fullscreen")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu") 

    # WebDriver Manager를 이용해 ChromeDriver 자동 관리
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        try:
            driver.get("https://bigdata.sbiz.or.kr/#/hotplace/gis")

            # 1️⃣ iframe이 로드될 때까지 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "iframe"))
            )
            
            # 2️⃣ iframe으로 전환
            iframe = driver.find_element(By.ID, "iframe")
            driver.switch_to.frame(iframe)

            # 검색 진행
            search_box = driver.find_element(By.ID, "searchAddress")
            search_box.send_keys(region_data['keyword'])
            search_box.send_keys(Keys.RETURN)
        except Exception as e:
            print(f"검색 에러 : {e}")

        full_keyword = region_data['keyword']

        if "도화2,3동" in full_keyword or "숭의1,3동" in full_keyword or "용현1,4동" in full_keyword:
            full_keyword = full_keyword.replace(",", ".")

        last_keyword = full_keyword.split()[-1]
        time.sleep(10)
        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.ID, "admListAddress"))
        )

        # 'adrsDiv' 아래의 span 태그 중에서 텍스트가 region_data['keyword'] 또는 last_keyword와 일치하는 요소를 찾음
        adrs_ul = driver.find_element(By.ID, "admListAddress")
        time.sleep(0.5)
        button_elements = adrs_ul.find_elements(By.TAG_NAME, "button")
        time.sleep(0.5)

        for button in button_elements:
            button_text = button.text.replace(" ", "")

            if last_keyword in button_text:
                driver.execute_script("arguments[0].click();", button)
                break
        else:
            print("No matching location found in adrsDiv, clicking first li button.")

            try:
                # 첫 번째 li 태그 내부의 button 찾기
                first_li = adrs_ul.find_element(By.TAG_NAME, "li")
                first_button = first_li.find_element(By.TAG_NAME, "button")

                # 첫 번째 버튼 클릭
                driver.execute_script("arguments[0].click();", first_button)
            except Exception as e:
                print("Error selecting first button:", e)
        
        time.sleep(10)

        div_content = None
        
        # 데이터 초기화
        source_data = {
            "city_id": region_data['city_id'],
            "district_id": region_data['district_id'],
            "sub_district_id": region_data['sub_district_id'],
            "shop": None,
            "sales": None,
            "move_pop": None,
            "income": None,
            "spend": None,
            "house": None,
            "work_pop": None,
            "resident": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "y_m": year_month,
            "reference_id": reference_id
        }

        buttons = {
            "StorCnt": "shop",
            "SaleAmt": "sales",
            "PopCnt": "move_pop",
            "WholEarnAmt": "income",
            "WholCnsmpAmt": "spend",
            "HhCnt": "house",
            "WrcpplCnt": "work_pop",
            "WholPpltnCnt": "resident"
        }


        for button_id, key in buttons.items():
            try:
                target_text = last_keyword
                # 버튼 클릭
                menu_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, button_id))
                )
                driver.execute_script("arguments[0].click();", menu_button)
                
                # 버튼 클릭 후 로딩 시간 대기
                time.sleep(10)  # 페이지 로딩 대기

                # `p` 태그와 같은 `div` 내의 `span` 태그 값 가져오기
                span_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f"/html/body/section/div/article/section[1]/div[2]/div[6]/div//p[contains(text(), '{target_text}')]/following-sibling::span"))
                )
                source_data[key] = span_element.text  # 매핑된 키에 값 저장
                # print(f"✅ {key} 값 저장: {data[key]}")

            except Exception as e:
                print(f"❌ {button_id} 처리 중 오류 발생: {e}")

        
        # ✅ shop 값이 없으면 줌아웃 후 다시 시도
        if not source_data["shop"]:  # shop이 None이거나 빈 값이면 실행
            try:
                zoom_out_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "zoomOut"))
                )
                driver.execute_script("arguments[0].click();", zoom_out_button)

                # 줌아웃 후 다시 대기
                time.sleep(5)  # 지도 로딩 대기

                # ✅ 줌아웃 후 다시 데이터 크롤링 시도
                for button_id, key in buttons.items():
                    try:
                        target_text = last_keyword

                        # 버튼 클릭
                        menu_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.ID, button_id))
                        )
                        driver.execute_script("arguments[0].click();", menu_button)

                        # 버튼 클릭 후 로딩 시간 대기
                        time.sleep(10)  # 페이지 로딩 대기

                        # `p` 태그와 같은 `div` 내의 `span` 태그 값 가져오기
                        span_element = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, f"/html/body/section/div/article/section[1]/div[2]/div[6]/div//p[contains(text(), '{target_text}')]/following-sibling::span"))
                        )
                        source_data[key] = span_element.text  # 매핑된 키에 값 저장
                        print(f"✅ {key} 값 다시 저장: {source_data[key]}")

                    except Exception as e:
                        print(f"❌ {button_id} 줌아웃 후 다시 처리 중 오류 발생: {e}")

            except Exception as e:
                print(f"❌ 줌아웃 버튼 클릭 중 오류 발생: {e}")

        div_content = source_data
      
        # 검색 결과에 따라 데이터 인서트
        if div_content:
            data = parse_html(div_content, region_data, reference_id, year_month)
        else:
            data = {
                "city_id": region_data['city_id'],
                "district_id": region_data['district_id'],
                "sub_district_id": region_data['sub_district_id'],
                "shop": None,
                "move_pop": None,
                "sales": None,
                "work_pop": None,
                "income": None,
                "spend": None,
                "house": None,
                "resident": None,
                "created_at": datetime.now(),  
                "updated_at": datetime.now(),  
                "y_m": year_month,
                "reference_id": reference_id
            }

        print(data)

        try:
            with connection.cursor() as cursor:
                # 인서트 함수
                insert_loc_info_data(
                    connection,
                    data
                )
                # 업데이트 함수
                # update_null_loc_info_data(
                #     connection,
                #     data
                # )
                connection.commit()
        except Exception as e:
            print(f"Error inserting or updating data: {e}")
            connection.rollback()
            raise e

    finally:
        driver.quit()


# 데이터 변환 함수
def parse_html(html_content, region_data, reference_id, year_month):
    def clean_value(value, multiplier=1):
        if value:
            # 쉼표와 단위 제거
            value = value.replace(",", "").replace("만원", "").replace("명", "").replace("세대", "").replace("개", "").strip()
            # 숫자 값으로 변환 후 곱하기
            return int(value) * multiplier if value.isdigit() else None
        return None

    data = {
        "city_id": region_data['city_id'],
        "district_id": region_data['district_id'],
        "sub_district_id": region_data['sub_district_id'],
        "shop": clean_value(html_content.get("shop")),
        "move_pop": clean_value(html_content.get("move_pop")),
        "sales": clean_value(html_content.get("sales"), 10000),  # 만원 단위 적용
        "work_pop": clean_value(html_content.get("work_pop")),
        "income": clean_value(html_content.get("income"), 10000),
        "spend": clean_value(html_content.get("spend"), 10000),
        "house": clean_value(html_content.get("house")),
        "resident": clean_value(html_content.get("resident")),
        "created_at": datetime.now(),  
        "updated_at": datetime.now(),  
        "y_m": year_month,
        "reference_id": reference_id
    }
    return data



def process_file_directly(all_region_list):
    connection = get_db_connection()
    try:
        for keyword_data in tqdm(
            all_region_list, desc="Processing keywords from DB"
        ):  crawl_keyword(keyword_data, connection)

    finally:
        close_connection(connection)


def process_keywords_from_db():
    # all_region_list = fetch_null_keywords_from_db()
    # new_region_list = all_region_list[2692:]
    keyword_list = fetch_keywords_from_db()
    # missing_list = find_missing_list()
    # null_list = fetch_test_keywords_from_db()
    # test_list = null_list[:5]

    with ThreadPoolExecutor(max_workers=5) as executor:
        # process_file_directly를 실행하는 스레드를 5개 병렬로 처리
        executor.map(lambda region: process_file_directly([region]), keyword_list)


def find_missing_list():
    # 두 테이블에서 데이터를 가져오기
    loc_info_sub_district_list = fetch_no_sub_district_id()  # loc_info: 딕셔너리 리스트 형식
    all_sub_district_list = fetch_sub_district_id()          # all: 딕셔너리 리스트 형식

    # 딕셔너리 리스트에서 'sub_district_id' 키 추출
    loc_info_keys = {item["sub_district_id"] for item in loc_info_sub_district_list}
    all_keys = {item["sub_district_id"] for item in all_sub_district_list}

    # all에 있지만 loc_info에는 없는 ID 찾기
    missing_keys = all_keys - loc_info_keys

    # 누락된 ID를 이용해 지역 목록 가져오기
    if missing_keys:
        missing_list = fetch_missing_list(list(missing_keys))
        print(missing_list)
        return missing_list


def begin_time():
    time_1 = datetime.now()
    print("Start Time:", time_1)
    return time_1

def finish_time(start_time):
    time_2 = datetime.now()
    taken_time = time_2 - start_time
    print("End Time:", time_2)
    print("Elapsed Time:", taken_time)


if __name__=="__main__":
    start = begin_time()
    process_keywords_from_db()
    finish_time(start)
    # find_missing_list()