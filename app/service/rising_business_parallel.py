from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import re
from typing import List
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from app.crud.biz_detail_category import (
    get_biz_categories_id_by_biz_detail_category_name,
)
from app.crud.city import get_or_create_city_id
from app.crud.district import get_or_create_district_id
from app.crud.rising_business import insert_rising_business
from app.crud.sub_district import get_or_create_sub_district_id
from app.schemas.rising_business import RisingBusiness, RisingBusinessInsert

from selenium.common.exceptions import (
    UnexpectedAlertPresentException,
    NoAlertPresentException,
    TimeoutException,
)
from selenium.webdriver.common.alert import Alert

NICE_BIZ_MAP_URL = "https://m.nicebizmap.co.kr/analysis/analysisFree"


# 시간 재는 함수
def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(
            f"Execution time for {func.__name__}: {end_time - start_time:.2f} seconds"
        )
        return result

    return wrapper


# Global WebDriver instance
global_driver = None


def setup_global_driver():
    global global_driver
    if global_driver is None:
        options = Options()
        options.add_argument("--start-fullscreen")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # WebDriver Manager를 이용해 ChromeDriver 자동 관리
        service = Service(ChromeDriverManager().install())
        global_driver = webdriver.Chrome(service=service, options=options)

    return global_driver


def click_element(wait, by, value):
    try:
        element = wait.until(EC.element_to_be_clickable((by, value)))
        text = element.text
        element.click()
        time.sleep(2)
        return text
    except TimeoutException:
        # print(
        #     f"TimeoutException occurred for element located by {by} with value {value}. Skipping to next element."
        # )
        return None


def read_element(wait, by, value):
    try:
        element = wait.until(EC.presence_of_element_located((by, value)))
        text = element.text
        time.sleep(0.3)
        return text
    except TimeoutException:
        # print(
        #     f"TimeoutException occurred for element located by {by} with value {value}. Skipping to next element."
        # )
        return None


def convert_to_int_float(value):
    if isinstance(value, str):
        value = re.sub(r"[^\d.]+", "", value)
        return float(value) if "." in value else int(value)
    return value


def handle_unexpected_alert(driver):
    try:
        alert = Alert(driver)
        alert_text = alert.text
        # print(f"Alert detected: {alert_text}")
        alert.accept()
        return True
    except NoAlertPresentException:
        return False


def get_city_count():
    global global_driver
    setup_global_driver()
    try:
        global_driver.get(NICE_BIZ_MAP_URL)
        wait = WebDriverWait(global_driver, 40)
        global_driver.implicitly_wait(10)

        time.sleep(2)

        # 분석 지역
        click_element(
            wait,
            By.CSS_SELECTOR,
            "#pc_sheet04 > div > div.pc_bdy.ticket > div.middle > ul > li > a",
        )

        time.sleep(2)

        city_ul = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul')
            )
        )
        city_ul_li = city_ul.find_elements(By.TAG_NAME, "li")
        # print(f"시/도 갯수: {len(city_ul_li)}")

        get_district_count(len(city_ul_li))
    except Exception as e:
        # print(f"Exception occurred: {e}.")
        return None
    finally:
        try:
            if global_driver:
                global_driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


# def get_district_count(city_count):
def get_district_count(start_idx: int, end_idx: int):
    global global_driver
    setup_global_driver()
    try:
        for city_idx in tqdm(range(start_idx, end_idx), desc="시/도 Progress"):
            try:
                global_driver.get(NICE_BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 40)
                click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

                time.sleep(2)

                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
                )

                time.sleep(2)
                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                district_ul = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul',
                        )
                    )
                )
                district_ul_li = district_ul.find_elements(By.TAG_NAME, "li")
                # print(f"구 갯수: {len(district_ul_li)}")

                get_sub_district_count(city_idx, len(district_ul_li), city_text)

            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(f"Error processing city index {city_idx}: {str(e)}")
                continue
    finally:
        try:
            if global_driver:
                global_driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_sub_district_count(city_idx: int, district_count: int, city_text_ck: str):
    global global_driver
    setup_global_driver()
    try:
        for district_idx in tqdm(range(district_count), f"{city_text_ck} : Progress"):
            try:
                global_driver.get(NICE_BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 40)
                click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

                time.sleep(2)

                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
                )

                time.sleep(2)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2)

                district_ul = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul',
                        )
                    )
                )

                district_ul_li = district_ul.find_elements(By.TAG_NAME, "li")
                # print(f"구 갯수: {len(district_ul_li)}")

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                sub_district_ul = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul',
                        )
                    )
                )
                sub_district_ul_li = sub_district_ul.find_elements(By.TAG_NAME, "li")
                # print(f"동 갯수: {len(sub_district_ul_li)}")

                search_rising_businesses_top5(
                    city_idx, district_idx, len(sub_district_ul_li)
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                print(f"Error processing district index {district_idx}: {str(e)}")
                continue

    finally:
        pass
        # try:
        #     if global_driver:
        #         global_driver.quit()
        # except Exception as quit_error:
        #     print(f"Error closing driver: {str(quit_error)}")


def search_rising_businesses_top5(
    city_idx: int, district_idx: int, sub_district_count: int
):
    global global_driver
    setup_global_driver()
    data_list: List[RisingBusiness] = []

    try:
        for sub_district_idx in range(sub_district_count):
            # start_time = time.time()
            global_driver.get(NICE_BIZ_MAP_URL)
            wait = WebDriverWait(global_driver, 40)
            time.sleep(2)

            click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

            time.sleep(2)

            click_element(
                wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
            )
            time.sleep(2)

            city_text = click_element(
                wait,
                By.XPATH,
                f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
            )
            time.sleep(3)
            district_text = click_element(
                wait,
                By.XPATH,
                f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
            )
            time.sleep(2)
            sub_district_text = click_element(
                wait,
                By.XPATH,
                f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
            )
            time.sleep(2)

            try:
                # 시/구/동 ID 조회 및 생성
                city_id = get_or_create_city_id(city_text)
                if city_id:
                    district_id = get_or_create_district_id(city_id, district_text)
                    if district_id:
                        sub_district_id = get_or_create_sub_district_id(
                            city_id, district_id, sub_district_text
                        )

            except Exception as e:
                print(f"시 구 동 조회 오류 : {e}")

            # 상승 중인 사업 정보 추출
            try:
                rising_ul = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="cardBoxTop5"]'))
                )
                rising_ul_li = rising_ul.find_elements(
                    By.CSS_SELECTOR, "#cardBoxTop5 > li"
                )

                if len(rising_ul_li) > 0:
                    for i, li in enumerate(rising_ul_li):
                        li_text = li.text.strip().split("\n")
                        # print(
                        #     f"city_id: {city_id}, district_id: {district_id}, sub_district_id: {sub_district_id}"
                        # )
                        # print(f"li_text{li_text}, idx:{i}")
                        # print(f"소분류: {li_text[1]}, idx:{i}")

                        if len(li_text) >= 2:
                            try:
                                category_result = (
                                    get_biz_categories_id_by_biz_detail_category_name(
                                        li_text[1]
                                    )
                                )

                                if category_result is None:
                                    # print("Failed to get or create detail category ID")
                                    continue

                                growth_rate = convert_to_int_float(li_text[2])

                                data = RisingBusinessInsert(
                                    city_id=city_id,
                                    district_id=district_id,
                                    sub_district_id=sub_district_id,
                                    biz_main_category_id=category_result[0],
                                    biz_sub_category_id=category_result[1],
                                    biz_detail_category_id=category_result[2],
                                    growth_rate=growth_rate,
                                    sub_district_rank=convert_to_int_float(li_text[0]),
                                )

                                data_list.append(data)

                            except Exception as e:
                                continue
                else:
                    data_list.append(
                        RisingBusinessInsert(
                            city_id=city_id,
                            district_id=district_id,
                            sub_district_id=sub_district_id,
                            biz_main_category_id=2,
                            biz_sub_category_id=2,
                            biz_detail_category_id=3,
                            growth_rate=0.0,
                            sub_district_rank=0,
                        )
                    )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
                data_list.append(
                    RisingBusinessInsert(
                        city_id=city_id,
                        district_id=district_id,
                        sub_district_id=sub_district_id,
                        biz_main_category_id=2,
                        biz_sub_category_id=2,
                        biz_detail_category_id=3,
                        growth_rate=0.0,
                        sub_district_rank=0,
                    )
                )
            except Exception as e:
                continue

            # end_time = time.time()
            # elapsed_time = end_time - start_time
            # print(f"Time taken: {elapsed_time} seconds")
            # print(
            #     f"시/도: {city_text}, 시/군/구: {district_text}, 읍/면/동: {sub_district_text}"
            # )

        insert_rising_business(data_list)

    except Exception as e:
        print(f"Failed to fetch data from {NICE_BIZ_MAP_URL}: {str(e)}")
    finally:
        pass


def execute_task_in_thread(start, end):

    with ThreadPoolExecutor(max_workers=18) as executor:

        futures = [
            executor.submit(get_district_count, start, end),
        ]
        # 17번까지

        for future in futures:
            future.result()


@time_execution
def execute_parallel_tasks():

    ranges = [
        (0, 2),
        (2, 4),
        (4, 6),
        (6, 8),
        (8, 10),
        (10, 12),
        (12, 14),
        (14, 15),
        (15, 16),
        (16, 17),
    ]

    # 멀티프로세싱 사용
    with Pool(processes=len(ranges)) as pool:
        pool.starmap(execute_task_in_thread, ranges)


if __name__ == "__main__":
    execute_parallel_tasks()
    # 컴퓨터 종료 명령어 (운영체제에 따라 다름)
    # if os.name == "nt":  # Windows
    #     os.system("shutdown /s /t 1")
    # else:  # Unix-based (Linux, macOS)
    #     os.system("shutdown -h now")
