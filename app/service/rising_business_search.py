from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import re
from typing import List, Optional
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

from ..crud.city import get_or_create_city as crud_get__all_biz_categories_id_like_biz_detail_category_name

from tqdm import tqdm
from app.crud.biz_detail_category import (
    get__all_biz_categories_id_like_biz_detail_category_name as crud_get__all_biz_categories_id_like_biz_detail_category_name,
    get_biz_categories_id_by_biz_detail_category_name,
)
from app.crud.city import get_or_create_city_id
from app.crud.district import get_district_id, get_or_create_district_id
from app.crud.rising_business import (
    insert_rising_business,
    select_all_rising_business_by_dynamic_query as crud_select_all_rising_business_by_dynamic_query,
    select_all_rising_business_by_region_id,
)
from app.crud.sub_district import get_or_create_sub_district_id, get_sub_district_id_by
from app.schemas.rising_business import (
    RisingBusiness,
    RisingBusinessInsert,
    RisingBusinessOutput,
)

from selenium.common.exceptions import (
    UnexpectedAlertPresentException,
    NoAlertPresentException,
    TimeoutException,
)
from selenium.webdriver.common.alert import Alert

NICE_BIZ_MAP_URL = "https://m.nicebizmap.co.kr/analysis/analysisFree"


def setup_driver():
    driver_path = os.path.join(
        os.path.dirname(__file__), "../", "drivers", "chromedriver.exe"
    )

    options = Options()
    options.add_argument("--start-fullscreen")
    options.add_argument("--no-sandbox")  # 샌드박스 비활성화
    options.add_argument("--disable-dev-shm-usage")  # 리소스 절약

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def click_element(wait, by, value):
    try:
        element = wait.until(EC.element_to_be_clickable((by, value)))
        text = element.text
        element.click()
        time.sleep(0.7)
        return text
    except Exception as e:
        print(
            f"Exception occurred: {e} for element located by {by} with value {value}. Skipping to next element."
        )
        return None


def read_element(wait, by, value):
    try:
        element = wait.until(EC.presence_of_element_located((by, value)))
        text = element.text
        time.sleep(0.5)
        return text
    except Exception as e:
        print(
            f"Exception occurred: {e} for element located by {by} with value {value}. Skipping to next element."
        )
        return None


def convert_to_int_float(value):
    if isinstance(value, str):
        value = re.sub(r"[^\d.]+", "", value)
        return float(value) if "." in value else int(value)
    return value


def clean_data(int_str):
    def clean_value(value):
        if isinstance(value, str):
            value = re.sub(r"[^\d.]+", "", value)
            return float(value) if value else None
        return value

    return {key: clean_value(value) for key, value in int_str.items()}


def handle_unexpected_alert(driver):
    try:
        alert = Alert(driver)
        alert_text = alert.text
        print(f"Alert detected: {alert_text}")
        alert.accept()
        return True
    except NoAlertPresentException:
        return False


def get_city_count():
    driver = setup_driver()
    try:
        driver.get(NICE_BIZ_MAP_URL)
        wait = WebDriverWait(driver, 40)
        driver.implicitly_wait(10)

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
        print(f"시/도 갯수: {len(city_ul_li)}")

        get_district_count(len(city_ul_li))
    except Exception as e:
        print(f"Exception occurred: {e}.")
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_district_count(city_count):
    driver = setup_driver()
    try:
        for city_idx in tqdm(range(city_count), desc="시/도 Progress"):
            try:
                print(f"idx: {city_idx}")
                driver.get(NICE_BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

                time.sleep(1.5)

                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
                )

                time.sleep(1.5)
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
                print(f"구 갯수: {len(district_ul_li)}")

                get_sub_district_count(city_idx, len(district_ul_li), city_text)

            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                print(f"Error processing city index {city_idx}: {str(e)}")
                continue
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_sub_district_count(city_idx: int, district_count: int, city_text_ck: str):
    driver = setup_driver()
    try:
        for district_idx in tqdm(range(district_count), f"{city_text_ck} : Progress"):
            try:
                print(f"idx: {district_idx}")
                driver.get(NICE_BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

                time.sleep(1.5)

                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
                )

                time.sleep(1.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(1.5)

                district_ul = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul',
                        )
                    )
                )

                district_ul_li = district_ul.find_elements(By.TAG_NAME, "li")
                print(f"구 갯수: {len(district_ul_li)}")

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
                print(f"동 갯수: {len(sub_district_ul_li)}")

                print(f"시/도: {city_text}, 시/군/구: {district_text}")

                search_rising_businesses_top5(
                    city_idx, district_idx, len(sub_district_ul_li)
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                print(f"Error processing district index {district_idx}: {str(e)}")
                continue

    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def search_rising_businesses_top5(
    city_idx: int, district_idx: int, sub_district_count: int
):
    driver = setup_driver()
    data_list: List[RisingBusiness] = []

    try:
        for sub_district_idx in range(sub_district_count):
            start_time = time.time()
            driver.get(NICE_BIZ_MAP_URL)
            wait = WebDriverWait(driver, 60)
            time.sleep(1.5)

            click_element(wait, By.XPATH, "/html/body/div[5]/div[2]/ul/li[5]/a")

            time.sleep(1.5)

            click_element(
                wait, By.XPATH, '//*[@id="pc_sheet04"]/div/div[2]/div[2]/ul/li/a'
            )
            time.sleep(1.5)

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
            time.sleep(1.5)
            sub_district_text = click_element(
                wait,
                By.XPATH,
                f'//*[@id="rising"]/div[2]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
            )
            time.sleep(1.5)

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
                                    print("Failed to get or create detail category ID")
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
                                print(
                                    f"읍/면/동: {sub_district_text}, 뜨는 업종 data:{data}"
                                )

                                data_list.append(data)

                            except Exception as e:
                                print(f"카테고리 조회 오류 : {e}")
                                continue
                else:
                    print(f"읍/면/동: {sub_district_text} 데이터 없음")
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
                print(f"읍/면/동: {sub_district_text} 데이터 없음")
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
                print(f"Loading Error : {str(e)}")
                continue

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Time taken: {elapsed_time} seconds")
            print(
                f"시/도: {city_text}, 시/군/구: {district_text}, 읍/면/동: {sub_district_text}"
            )

        insert_rising_business(data_list)

    except Exception as e:
        print(f"Failed to fetch data from {NICE_BIZ_MAP_URL}: {str(e)}")
    finally:
        driver.quit()


if __name__ == "__main__":
    # get_city_count()
    search_rising_businesses_top5(0, 0, 22)
