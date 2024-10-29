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
from datetime import datetime
import os, time
from tqdm import tqdm
import sys
from app.crud.loc_info import *


###### 크롤링 #########
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from app.db.connect import get_db_connection, close_connection

# 1. 가지고 있는 지역 id값 모두 조회
def fetch_all_region_id():
    all_region_list = get_all_region_id()
    return all_region_list


def crawl_keyword(region_data, connection, insert_count):
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

        # 페이지 로드 완료 확인 코드 추가
        WebDriverWait(driver, 60).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        radio_button = driver.find_element(By.ID, "icon_list2_2")
        driver.execute_script("arguments[0].click();", radio_button)

        search_box = driver.find_element(By.ID, "searchAddress")
        search_box.send_keys(region_data['keyword'])
        search_box.send_keys(Keys.RETURN)

        last_keyword = region_data['keyword'].split()[-1]

        time.sleep(0.5)

        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='cell']"))
            )
            div_elements = driver.find_elements(By.XPATH, "//div[@class='cell']")
            div_content = None
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
            div_content = None

        if div_content:
            data = parse_html(div_content)
        else:
            data = {
                "shop": None,
                "move_pop": None,
                "sales": None,
                "work_pop": None,
                "income": None,
                "spend": None,
                "house": None,
                "resident": None,
            }
        print(data)
        year_month = datetime(2024, 10, 2).date()
        try:
            with connection.cursor() as cursor:
                insert_record(
                    "loc_info",
                    connection,
                    insert_count,
                    city_id=region_data['city_id'],
                    district_id=region_data['district_id'],
                    sub_district_id=region_data['sub_district_id'],
                    y_m=year_month,
                    **data,
                    created_at = datetime.now(), 
                    updated_at = datetime.now(),
                    reference_id = 3
                )
                connection.commit()
                    
        except Exception as e:
            print(f"Error inserting or updating data: {e}")
            connection.rollback()
            raise e

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        driver.quit()


def parse_html(html_content):
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
        "shop": clean_value(soup.find("strong", {"data-name": "business"}).text if soup.find("strong", {"data-name": "business"}) else None),
        "move_pop": clean_value(soup.find("strong", {"data-name": "person"}).text if soup.find("strong", {"data-name": "person"}) else None),
        "sales": clean_value(soup.find("strong", {"data-name": "price"}).text if soup.find("strong", {"data-name": "price"}) else None, 10000),
        "work_pop": clean_value(soup.find("strong", {"data-name": "wrcppl"}).text if soup.find("strong", {"data-name": "wrcppl"}) else None),
        "income": clean_value(soup.find("strong", {"data-name": "earn"}).text if soup.find("strong", {"data-name": "earn"}) else None, 10000),
        "spend": clean_value(soup.find("strong", {"data-name": "cnsmp"}).text if soup.find("strong", {"data-name": "cnsmp"}) else None, 10000),
        "house": clean_value(soup.find("strong", {"data-name": "hhCnt"}).text if soup.find("strong", {"data-name": "hhCnt"}) else None),
        "resident": clean_value(soup.find("strong", {"data-name": "rsdppl"}).text if soup.find("strong", {"data-name": "rsdppl"}) else None),
    }
    return data



def insert_record(table_name: str, connection, insert_count, city_id, district_id, sub_district_id, **kwargs):
    try:
        with connection.cursor() as cursor:
            # city_id, district_id, sub_district_id는 고정 컬럼
            columns = "city_id, district_id, sub_district_id, " + ", ".join(kwargs.keys())
            placeholders = "%s, %s, %s, " + ", ".join(["%s"] * len(kwargs))
            values = (city_id, district_id, sub_district_id) + tuple(kwargs.values())
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, values)
        connection.commit()
        print(f"{insert_count}번째 insert 성공")
    except pymysql.IntegrityError as e:
        if e.args[0] == 1062:  # Duplicate entry error code
            print(f"Duplicate entry found for {kwargs}. Skipping...")
        else:
            print(f"Error inserting data: {e}")
            connection.rollback()
            raise e
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
        raise e





def process_file_directly(all_region_list):
    connection = get_db_connection()
    insert_count = 0

    try:
        for keyword_data in tqdm(
            all_region_list, desc="Processing keywords from DB"
        ):  # tqdm을 사용하여 진행 상황 표시
            # 각 키워드에 대해 무조건 새로운 데이터를 삽입하거나 갱신
            insert_count += 1

            # keyword_data는 city_id, district_id, sub_district_id, keyword를 포함
            crawl_keyword(keyword_data, connection, insert_count)

    finally:
        close_connection(connection)




def process_keywords_from_db():
    all_region_list = fetch_keywords_from_db()
    new_region_list = all_region_list[2692:]


    with ThreadPoolExecutor(max_workers=5) as executor:
        # process_file_directly를 실행하는 스레드를 5개 병렬로 처리
        executor.map(lambda region: process_file_directly([region]), new_region_list)



def begin_time():
    time_1 = datetime.now()
    print("Start Time:", time_1)
    return time_1

def finish_time(start_time):
    time_2 = datetime.now()
    taken_time = time_2 - start_time
    print("End Time:", time_2)
    print("Elapsed Time:", taken_time)


def test():
    all_region_list = fetch_keywords_from_db()
    new_region_list = all_region_list[2692:]
    print(new_region_list)

if __name__=="__main__":
    start = begin_time()
    process_keywords_from_db()
    finish_time(start)
    # test()