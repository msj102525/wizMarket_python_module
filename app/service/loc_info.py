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


###### 크롤링 #########
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from app.db.connect import get_db_connection, close_connection


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
        driver.get("https://sg.sbiz.or.kr/godo/index.sg")

        # 첫 번째 팝업창 제거
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "help_guide"))
        )
        # help_guide div 내에서 guide_check_2 라벨 선택
        help_guide_div = driver.find_element(By.ID, "help_guide")
        guide_label = help_guide_div.find_element(By.CSS_SELECTOR, "label[for='guide_check_2']")
        guide_label.click()

        # help_guide 내 foot div 안의 close 클래스 a 태그 클릭
        close_button = help_guide_div.find_element(By.CSS_SELECTOR, "div.foot a.close")
        close_button.click()

        # 우측 메뉴바 닫기
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "menu"))
        )
        menu_div = driver.find_element(By.ID, "menu")
        mo_btn_close_button = menu_div.find_element(By.CSS_SELECTOR, "div.lay.scrollbarView a.mo_btn_close")
        driver.execute_script("arguments[0].click();", mo_btn_close_button)

        # # 좌측 팝업 닫기
        # WebDriverWait(driver, 30).until(
        #     EC.presence_of_element_located((By.ID, "notice0"))
        # )
        # notice_div = driver.find_element(By.ID, "notice0")
        # notice_close_label = notice_div.find_element(By.ID, "noticeClose0")
        # driver.execute_script("arguments[0].click();", notice_close_label)

        # 우측 팝업 닫기
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "notice1"))
        )
        notice_div = driver.find_element(By.ID, "notice1")
        notice_close_label = notice_div.find_element(By.ID, "noticeClose1")
        driver.execute_script("arguments[0].click();", notice_close_label)

        # 추가 팝업 닫기 1
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "notice2"))
        )
        notice_div = driver.find_element(By.ID, "notice2")
        notice_close_label = notice_div.find_element(By.ID, "noticeClose2")
        driver.execute_script("arguments[0].click();", notice_close_label)

        # 추가 팝업 닫기 2
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "notice3"))
        )
        notice_div = driver.find_element(By.ID, "notice3")
        notice_close_label = notice_div.find_element(By.ID, "noticeClose3")
        driver.execute_script("arguments[0].click();", notice_close_label)

        # 추가 팝업 닫기 3
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "notice0"))
        )
        notice_div = driver.find_element(By.ID, "notice0")
        notice_close_label = notice_div.find_element(By.ID, "noticeClose0")
        driver.execute_script("arguments[0].click();", notice_close_label)

        # 검색 진행
        radio_button = driver.find_element(By.ID, "icon_list2_2")
        driver.execute_script("arguments[0].click();", radio_button)
        search_box = driver.find_element(By.ID, "searchAddress")
        search_box.send_keys(region_data['keyword'])
        search_box.send_keys(Keys.RETURN)

        full_keyword = region_data['keyword']

        if "도화2,3동" in full_keyword or "숭의1,3동" in full_keyword or "용현1,4동" in full_keyword:
            full_keyword = full_keyword.replace(",", ".")

        last_keyword = full_keyword.split()[-1]
        time.sleep(20)
        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.ID, "adrsDiv"))
        )

        # 'adrsDiv' 아래의 span 태그 중에서 텍스트가 region_data['keyword'] 또는 last_keyword와 일치하는 요소를 찾음
        adrs_div = driver.find_element(By.ID, "adrsDiv")
        time.sleep(0.5)
        span_elements = adrs_div.find_elements(By.TAG_NAME, "span")
        time.sleep(0.5)
        for span in span_elements:
            span_text = span.text.replace(" ", "")
            full_keyword_no_space = full_keyword.replace(" ", "")
            if full_keyword_no_space in span_text or last_keyword in span_text:
                driver.execute_script("arguments[0].click();", span)
                break
        else:
            print("No matching location found in adrsDiv")
        
        time.sleep(20)
        
        # 초기 위치에서 div[@class='cell'] 검색 시도
        div_content = None
        
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='cell']"))
            )
            div_elements = driver.find_elements(By.XPATH, "//div[@class='cell']")
            for div_element in div_elements:
                try:
                    span_element = div_element.find_element(By.TAG_NAME, "span")
                    if span_element.text == last_keyword:
                        div_content = div_element.get_attribute("innerHTML")
                        break
                except Exception as e:
                    print(f"Error accessing span element: {e}")

        except Exception as e:
            print(f"Timeout or error waiting for div element: {e}")

        if div_content is None:
            try:
                # 첫 번째 줌 아웃 버튼을 스크롤하여 클릭
                zoom_out_button = driver.find_element(By.CSS_SELECTOR, "#container > div.custom_zoomcontrol > div:nth-child(3) > a:nth-child(2)")
                driver.execute_script("arguments[0].scrollIntoView();", zoom_out_button)
                driver.execute_script("arguments[0].click();", zoom_out_button)
                time.sleep(20)  # 잠깐 대기

                # 다시 검색 시도
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@class='cell']"))
                )
                div_elements = driver.find_elements(By.XPATH, "//div[@class='cell']")
                for div_element in div_elements:
                    try:
                        span_element = div_element.find_element(By.TAG_NAME, "span")
                        if span_element.text == last_keyword:
                            div_content = div_element.get_attribute("innerHTML")
                            break
                    except Exception as e:
                        print(f"Error accessing span element after first zoom out: {e}")

                # div_content가 여전히 None인 경우 줌 인 버튼을 두 번 클릭
                if div_content is None:
                    zoom_in_button = driver.find_element(By.CSS_SELECTOR, "#container > div.custom_zoomcontrol > div:nth-child(3) > a:nth-child(1)")
                    driver.execute_script("arguments[0].scrollIntoView();", zoom_in_button)
                    driver.execute_script("arguments[0].click();", zoom_in_button)
                    time.sleep(20)  # 잠깐 대기
                    driver.execute_script("arguments[0].click();", zoom_in_button)
                    time.sleep(20)  # 잠깐 대기

                    # 다시 검색 시도
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@class='cell']"))
                    )
                    div_elements = driver.find_elements(By.XPATH, "//div[@class='cell']")
                    for div_element in div_elements:
                        try:
                            span_element = div_element.find_element(By.TAG_NAME, "span")
                            if span_element.text == last_keyword:
                                div_content = div_element.get_attribute("innerHTML")
                                break
                        except Exception as e:
                            print(f"Error accessing span element after second zoom in: {e}")

            except Exception as e:
                print(f"Error during zoom adjustments: {e}")


                # div_content가 여전히 None인 경우 줌 아웃 버튼을 두 번 클릭
                if div_content is None:
                    zoom_in_button = driver.find_element(By.CSS_SELECTOR, "#container > div.custom_zoomcontrol > div:nth-child(3) > a:nth-child(2)")
                    driver.execute_script("arguments[0].scrollIntoView();", zoom_in_button)
                    driver.execute_script("arguments[0].click();", zoom_in_button)
                    time.sleep(1)  # 잠깐 대기
                    driver.execute_script("arguments[0].click();", zoom_in_button)
                    time.sleep(1)  # 잠깐 대기

                    # 다시 검색 시도
                    WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@class='cell']"))
                    )
                    div_elements = driver.find_elements(By.XPATH, "//div[@class='cell']")
                    for div_element in div_elements:
                        try:
                            span_element = div_element.find_element(By.TAG_NAME, "span")
                            if span_element.text == last_keyword:
                                div_content = div_element.get_attribute("innerHTML")
                                break
                        except Exception as e:
                            print(f"Error accessing span element after second zoom in: {e}")

            except Exception as e:
                print(f"Error during zoom adjustments: {e}")



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
    from bs4 import BeautifulSoup

    def clean_value(value, multiplier=1):
        if value:
            # 쉼표와 단위 제거
            value = value.replace(",", "").replace("만원", "").replace("명", "").strip()
            # 숫자 값으로 변환 후 곱하기
            return int(value) * multiplier
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    data = {
        "city_id" : region_data['city_id'],
        "district_id" : region_data['district_id'],
        "sub_district_id" : region_data['sub_district_id'],
        "shop": clean_value(soup.find("strong", {"data-name": "business"}).text if soup.find("strong", {"data-name": "business"}) else None),
        "move_pop": clean_value(soup.find("strong", {"data-name": "person"}).text if soup.find("strong", {"data-name": "person"}) else None),
        "sales": clean_value(soup.find("strong", {"data-name": "price"}).text if soup.find("strong", {"data-name": "price"}) else None, 10000),
        "work_pop": clean_value(soup.find("strong", {"data-name": "wrcppl"}).text if soup.find("strong", {"data-name": "wrcppl"}) else None),
        "income": clean_value(soup.find("strong", {"data-name": "earn"}).text if soup.find("strong", {"data-name": "earn"}) else None, 10000),
        "spend": clean_value(soup.find("strong", {"data-name": "cnsmp"}).text if soup.find("strong", {"data-name": "cnsmp"}) else None, 10000),
        "house": clean_value(soup.find("strong", {"data-name": "hhCnt"}).text if soup.find("strong", {"data-name": "hhCnt"}) else None),
        "resident": clean_value(soup.find("strong", {"data-name": "rsdppl"}).text if soup.find("strong", {"data-name": "rsdppl"}) else None),
        "created_at": datetime.now(),  
        "updated_at": datetime.now(),  
        "y_m" : year_month,
        "reference_id" : reference_id
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
    # all_region_list = fetch_keywords_from_db()
    # new_region_list = all_region_list[2692:]
    # keyword_list = fetch_test_keywords_from_db()
    missing_list = find_missing_list()


    with ThreadPoolExecutor(max_workers=10) as executor:
        # process_file_directly를 실행하는 스레드를 5개 병렬로 처리
        executor.map(lambda region: process_file_directly([region]), missing_list)


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
    # start = begin_time()
    # process_keywords_from_db()
    # finish_time(start)
    find_missing_list()