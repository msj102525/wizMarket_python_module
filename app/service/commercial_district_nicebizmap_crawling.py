from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
import random
import re
import time
import os
from typing import Dict, List
import psutil
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoAlertPresentException,
)
from selenium.webdriver.common.alert import Alert
from tqdm import tqdm

from app.crud.biz_detail_category import get_or_create_biz_detail_category_id
from app.crud.biz_main_category import get_or_create_biz_main_category_id
from app.crud.biz_sub_category import get_or_create_biz_sub_category_id
from app.crud.city import get_or_create_city_id
from app.crud.commercial_district import insert_commercial_district
from app.crud.district import get_or_create_district_id
from app.crud.sub_district import get_or_create_sub_district_id
from app.schemas.commercial_district import CommercialDistrictInsert

from selenium.common.exceptions import (
    UnexpectedAlertPresentException,
    NoAlertPresentException,
    TimeoutException,
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
)


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


BIZ_MAP_URL = "https://m.nicebizmap.co.kr/analysis/analysisFree"
# BIZ_MAP_URL = "https://m.nicebizmap.co.kr/"


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


def click_element(driver, wait, by, value):
    try:
        element = wait.until(EC.element_to_be_clickable((by, value)))
        text = element.text
        element.click()
        time.sleep(1 + random.random())
        return text
    except ElementClickInterceptedException:
        try:
            driver.execute_script("arguments[0].click();", element)
            text = element.text
            return text
        except Exception as e:
            # print(f"JavaScript click failed for element: {value}. Error: {str(e)}")
            return False
    except UnexpectedAlertPresentException:
        handle_unexpected_alert(wait._driver)
    except Exception as e:
        # print(
        #     f"Exception occurred click: {e} for element located by {by} with value {value}. Skipping to next element."
        # )
        return ""


def read_element(wait, by, value):
    try:
        element = wait.until(EC.presence_of_element_located((by, value)))
        text = element.text.strip()
        time.sleep(1 + random.random())
        return text
    except (
        TimeoutException,
        NoSuchElementException,
        StaleElementReferenceException,
    ) as e:
        # print(f"Failed to read element: {by}, {value}. Error: {str(e)}")
        return ""
    except Exception as e:
        # print(f"Unexpected error reading element: {by}, {value}. Error: {str(e)}")
        return ""


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
        # print(f"Alert detected: {alert_text}")
        alert.accept()
        return True
    except NoAlertPresentException:
        return False


def get_district_count(city_idx):
    global global_driver
    setup_global_driver()
    try:
        # for city_idx in tqdm(range(city_count), desc="시/도 Progress"):
        # print(f"idx: {city_idx}")

        global_driver.get(BIZ_MAP_URL)
        wait = WebDriverWait(global_driver, 30)

        time.sleep(1 + random.random())

        # 분석 지역
        click_element(
            global_driver,
            wait,
            By.XPATH,
            '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
        )

        time.sleep(1 + random.random())

        city_text = click_element(
            global_driver,
            wait,
            By.XPATH,
            f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
        )

        district_ul = wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    '//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul',
                )
            )
        )
        district_ul_li = district_ul.find_elements(By.TAG_NAME, "li")
        # print(f"시/군/구 갯수: {len(district_ul_li)}")

        get_sub_district_count(city_idx, len(district_ul_li))
    except Exception as e:
        # print(f"Exception occurred: {e}.")
        return None
    finally:
        if global_driver:
            global_driver.quit()  # 메인 함수에서만 드라이버 종료
            global_driver = None


# def get_sub_district_count(start_idx: int, end_idx: int):
def get_sub_district_count(
    city_idx, district_idx
):  # 시/군/구 직접 전달 반복문 X 시/도 별 조회시 직접 시/군/구 idx 전달
    # def get_sub_district_count(city_idx, district_count):
    global global_driver
    setup_global_driver()
    try:
        # for district_idx in tqdm(
        #     range(district_count), f"{city_idx} : 시/군/구 Progress"
        # ):

        try:
            global_driver.get(BIZ_MAP_URL)
            wait = WebDriverWait(global_driver, 30)
            global_driver.implicitly_wait(10)

            time.sleep(1 + random.random())

            # 분석 지역
            click_element(
                global_driver,
                wait,
                By.XPATH,
                '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
            )

            time.sleep(1 + random.random())

            city_text = click_element(
                global_driver,
                wait,
                By.XPATH,
                f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
            )

            time.sleep(1 + random.random())

            district_text = click_element(
                global_driver,
                wait,
                By.XPATH,
                f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
            )

            sub_district_ul = wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        '//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul',
                    )
                )
            )
            sub_district_ul_li = sub_district_ul.find_elements(By.TAG_NAME, "li")

            get_main_category(city_idx, district_idx, len(sub_district_ul_li))
        except UnexpectedAlertPresentException:
            handle_unexpected_alert(wait._driver)
        except Exception as e:
            print(f"Error processing district_idx {district_idx}: {str(e)}")
            # continue  # 시/도 기준으로 할 때 활성화
        finally:
            pass
    except Exception as e:
        # print(
        #     f"Exception occurred get_sub_district_count(), district_idx {district_idx}: {e}."
        # )
        return None
    # finally: # 시/도 기준으로 할 때 활성화
    #     pass
    finally:
        if global_driver:
            global_driver.quit()  # 메인 함수에서만 드라이버 종료
            global_driver = None


def get_main_category(city_idx, district_idx, sub_district_count):
    global global_driver
    setup_global_driver()
    try:
        for sub_district_idx in tqdm(range(sub_district_count)):
            try:
                global_driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 30)
                global_driver.implicitly_wait(10)

                # 분석 지역
                click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
                )

                time.sleep(1 + random.random())

                city_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                sub_district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                main_category_ul_1 = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[1]',
                        )
                    )
                )

                main_category_ul_1_li = main_category_ul_1.find_elements(
                    By.TAG_NAME, "li"
                )
                # print(f"대분류1 갯수 : {len(main_category_ul_1_li)}")
                m_c_ul = 1

                get_sub_category(
                    city_idx,
                    district_idx,
                    sub_district_idx,
                    len(main_category_ul_1_li),
                    m_c_ul,
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(
                #     f"Exception occurred, 대분류1 반복 err, district_idx:  {district_idx}: {str(e)}"
                # )
                continue
            finally:
                pass

        for sub_district_idx in tqdm(range(sub_district_count)):
            try:
                global_driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 30)
                global_driver.implicitly_wait(10)

                # 분석 지역
                click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
                )

                time.sleep(1 + random.random())

                city_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                sub_district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                main_category_ul_2 = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[3]',
                        )
                    )
                )

                main_category_ul_2_li = main_category_ul_2.find_elements(
                    By.TAG_NAME, "li"
                )

                m_c_ul = 3

                get_sub_category(
                    city_idx,
                    district_idx,
                    sub_district_idx,
                    len(main_category_ul_2_li),
                    m_c_ul,
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(
                #     f"Exception occurred: 대분류2 반복 오류, district_idx: {district_idx}: {str(e)}"
                # )
                continue
            finally:
                pass
    except Exception as e:
        # print(
        #     f"Exception occurred get_main_category(), sub_district: {sub_district_idx} {e}."
        # )
        return None
    finally:
        pass


def get_sub_category(
    city_idx, district_idx, sub_district_idx, main_category_count, m_c_ul
):
    global global_driver
    setup_global_driver()
    try:
        for main_category_idx in range(main_category_count):
            try:
                global_driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 30)
                global_driver.implicitly_wait(10)

                # 분석 지역
                click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
                )

                time.sleep(1 + random.random())

                city_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                sub_district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                main_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]',
                )

                sub_category_li = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#basicReport ul.cate2 > ul")
                    )
                )
                sub_category_ul_li_button = sub_category_li.find_elements(
                    By.CSS_SELECTOR,
                    "#basicReport  ul.cate2 > ul > li > button",
                )
                # print(f"중분류 갯수 : {len(sub_category_ul_li_button)}")

                get_detail_category(
                    city_idx,
                    district_idx,
                    sub_district_idx,
                    main_category_idx,
                    len(sub_category_ul_li_button),
                    m_c_ul,
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(
                #     f"Error processing, main_category_idx: {main_category_idx}: {str(e)}"
                # )
                continue
            finally:
                pass
    except Exception as e:
        # print(
        #     f"Exception occurred, get_sub_category(), main_category_idx: {main_category_idx} {e}."
        # )
        return None
    finally:
        pass


def get_detail_category(
    city_idx,
    district_idx,
    sub_district_idx,
    main_category_idx,
    sub_category_count,
    m_c_ul,
):
    global global_driver
    setup_global_driver()
    try:
        for sub_category_idx in range(0, sub_category_count * 2, 2):

            try:
                global_driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 30)
                global_driver.implicitly_wait(10)

                # 분석 지역
                click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a',
                )

                time.sleep(1 + random.random())

                city_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                sub_district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(1 + random.random())

                main_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]',
                )

                time.sleep(1 + random.random())

                sub_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 1}]',
                )
                # print(f"중분류 : {sub_category_text}")
                detail_category_ul = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#basicReport ul.cate3")
                    )
                )
                detail_category_ul_li_button = detail_category_ul.find_elements(
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 2}]/ul/li',
                )
                # print(f"소분류 갯수 : {len(detail_category_ul_li_button)}")

                search_commercial_district(
                    city_idx,
                    district_idx,
                    sub_district_idx,
                    main_category_idx,
                    sub_category_idx,
                    len(detail_category_ul_li_button),
                    m_c_ul,
                )
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(f"Error processing sub_category_idx {sub_category_idx}: {str(e)}")
                continue
            finally:
                pass
    except Exception as e:
        # print(
        #     f"Exception occurred get_detail_category(), sub_category_idx: {sub_category_idx} {e}."
        # )
        return None
    finally:
        pass


def search_commercial_district(
    city_idx,
    district_idx,
    sub_district_idx,
    main_category_idx,
    sub_category_idx,
    detail_category_count,
    m_c_ul,
):
    global global_driver
    setup_global_driver()
    try:
        for detail_category_idx in range(detail_category_count):

            try:
                start_time = time.time()
                # print(
                #     f"Execution started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
                # )

                global_driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(global_driver, 30)
                global_driver.implicitly_wait(10)

                # 분석 지역
                click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]',
                )

                time.sleep(1 + random.random())

                city_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]',
                )

                time.sleep(1 + random.random())

                district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]',
                )

                time.sleep(1 + random.random())

                sub_district_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]',
                )

                time.sleep(1 + random.random())

                main_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]',
                )

                time.sleep(1 + random.random())

                sub_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 1}]',
                )

                time.sleep(1 + random.random())

                detail_category_text = click_element(
                    global_driver,
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 2}]/ul/li[{detail_category_idx + 1}]',
                )

                time.sleep(1 + random.random())

                try:
                    city_id = get_or_create_city_id(city_text)
                    if city_id is None:
                        # print("Failed to get or create main city ID")
                        continue
                    # print(f"시/도: {city_text}, {city_id}")

                    district_id = get_or_create_district_id(city_id, district_text)
                    if district_id is None:
                        # print("Failed to get or create district ID")
                        continue
                    # print(f"시/군/구: {district_text}, {district_id}")

                    sub_district_id = get_or_create_sub_district_id(
                        city_id, district_id, sub_district_text
                    )
                    if sub_district_id is None:
                        # print("Failed to get or create sub_district ID")
                        continue
                    # print(f"읍/면/동: {sub_district_text}, {sub_district_id}")

                except Exception as e:
                    # print(f"시 구 동 조회 오류 : {e}")
                    continue

                ###########################

                try:
                    main_category_id = get_or_create_biz_main_category_id(
                        main_category_text
                    )
                    if main_category_id is None:
                        # print("Failed to get or create main category ID")
                        continue

                    sub_category_id = get_or_create_biz_sub_category_id(
                        main_category_id, sub_category_text
                    )
                    if sub_category_id is None:
                        # print("Failed to get or create sub-category ID")
                        continue

                    if detail_category_text:
                        detail_category_text = detail_category_text.replace(
                            "(확장 분석)", ""
                        ).strip()

                    detail_category_id = get_or_create_biz_detail_category_id(
                        sub_category_id, detail_category_text
                    )
                    # if detail_category_id is None:
                    #     print("Failed to get or create detail category ID")

                except Exception as e:
                    # print(f"카테고리 조회 오류 : {e}")
                    continue

                if detail_category_text:
                    # print(detail_category_text)

                    time.sleep(1 + random.random())

                    # '//*[@id="report1"]' 요소가 나타날 때까지 기다리기
                    try:
                        # 상권분석 보기
                        click_element(
                            global_driver, wait, By.XPATH, '//*[@id="pcBasicReport"]'
                        )

                        wait.until(
                            EC.presence_of_element_located(
                                (By.XPATH, '//*[@id="report1"]/div/div[3]/div/div')
                            )
                        )
                    except TimeoutException:
                        # print(f"Element not found: //*[@id='report1'] 없거나 안뜸")
                        continue

                    wait = WebDriverWait(global_driver, 3)

                    # 분석 텍스트 보기 없애기
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[4]/div[1]/div/div/div/div[1]/div[2]/label',
                    )

                    time.sleep(1 + random.random())

                    # 표 전제보기
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[4]/div[1]/div/div/div/div[1]/div[1]/label',
                    )

                    time.sleep(1 + random.random())

                    # 밀집도 클릭
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[2]',
                    )

                    time.sleep(1 + random.random())

                    # 전국 해당 업종수 밀집도 데이터
                    national_density = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s2"]/div[2]/div[2]/div/div[2]/table/tbody/tr[3]/td[2]',
                    )

                    time.sleep(1 + random.random())

                    # 해당 시/도, 해당 업종수 밀집도 데이터
                    city_density = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s2"]/div[2]/div[2]/div/div[2]/table/tbody/tr[3]/td[3]',
                    )

                    # 해당 시/군/구, 해당 업종수 밀집도 데이터
                    district_density = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s2"]/div[2]/div[2]/div/div[2]/table/tbody/tr[3]/td[4]',
                    )

                    # 해당 읍/면/동, 해당 업종수 밀집도 데이터
                    sub_district_density = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s2"]/div[2]/div[2]/div/div[2]/table/tbody/tr[3]/td[5]',
                    )

                    # 시장규모 클릭
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[3]',
                    )

                    time.sleep(1 + random.random())

                    # 해당지역 업종 총 시장규모(원) 제일 최신
                    market_size = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s3"]/div[2]/div[2]/div[2]/table/tbody/tr/td[7]',
                    )

                    # 결제단가 클릭
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[6]',
                    )

                    time.sleep(1 + random.random())

                    # 해당지역 업종 결제단가(원)
                    average_payment = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s6"]/div[2]/div[2]/div[2]/table/tbody/tr[2]/td[7]',
                    )

                    # 해당지역 업종 이용건수(건)
                    usage_count = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s6"]/div[2]/div[2]/div[2]/table/tbody/tr[1]/td[7]',
                    )

                    # 비용/수익통계 대분류:1 음식일때만
                    if main_category_id == 1:
                        # 비용/수익통계 클릭
                        click_element(
                            global_driver,
                            wait,
                            By.XPATH,
                            '//*[@id="report1"]/div/div[3]/div/ul/li[7]',
                        )

                        time.sleep(1 + random.random())

                        # 해당지역 업종 점포당 매출규모(원)
                        average_sales = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[1]/p[2]/b',
                        )

                        # 해당지역 영업비용(원)
                        operating_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[2]/p[2]/b',
                        )

                        # 식재료비(원)
                        food_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[1]/p[2]',
                        )

                        # 고용인 인건비(원)
                        employee_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[2]/p[2]',
                        )

                        # 임차료(원)
                        rental_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[3]/p[2]',
                        )

                        # 세금(원)
                        tax_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[4]/p[2]',
                        )

                        # 가족 종사자 인건비(원)
                        family_employee_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[5]/p[2]',
                        )

                        # 대표자 인건비(원)
                        ceo_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[6]/p[2]',
                        )

                        # 기타 인건비(원)
                        etc_cost = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[3]/ul/li[7]/p[2]',
                        )

                        # 해당지역 평균 영업이익(원)
                        average_profit = read_element(
                            wait,
                            By.XPATH,
                            '//*[@id="receipt1"]/div/div[2]/ul/li[4]/p[2]/b',
                        )
                    else:
                        average_sales = 0
                        operating_cost = 0
                        food_cost = 0
                        employee_cost = 0
                        rental_cost = 0
                        tax_cost = 0
                        family_employee_cost = 0
                        ceo_cost = 0
                        etc_cost = 0
                        average_profit = 0

                    # 매출 비중 클릭
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[8]',
                    )

                    time.sleep(1 + random.random())

                    # 해당지역 업종 매출 요일별 (월요일 %)
                    avg_profit_per_mon = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[2]',
                    )

                    # 해당지역 업종 매출 요일별 (화요일 %)
                    avg_profit_per_tue = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[3]',
                    )

                    # 해당지역 업종 매출 요일별 (수요일 %)
                    avg_profit_per_wed = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[4]',
                    )

                    # 해당지역 업종 매출 요일별 (목요일 %)
                    avg_profit_per_thu = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[5]',
                    )

                    # 해당지역 업종 매출 요일별 (금요일 %)
                    avg_profit_per_fri = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[6]',
                    )

                    # 해당지역 업종 매출 요일별 (토요일 %)
                    avg_profit_per_sat = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[7]',
                    )

                    # 해당지역 업종 매출 요일별 (일요일 %)
                    avg_profit_per_sun = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[1]/tbody/tr/td[8]',
                    )

                    # 해당지역 업종 매출 시간별 (06 ~ 09  %)
                    avg_profit_per_06_09 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[2]',
                    )

                    # 해당지역 업종 매출 시간별 (09 ~ 12  %)
                    avg_profit_per_09_12 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[3]',
                    )

                    # 해당지역 업종 매출 시간별 (12 ~ 15  %)
                    avg_profit_per_12_15 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[4]',
                    )

                    # 해당지역 업종 매출 시간별 (15 ~ 18  %)
                    avg_profit_per_15_18 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[5]',
                    )

                    # 해당지역 업종 매출 시간별 (18 ~ 21  %)
                    avg_profit_per_18_21 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[6]',
                    )

                    # 해당지역 업종 매출 시간별 (21 ~ 24  %)
                    avg_profit_per_21_24 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[7]',
                    )

                    # 해당지역 업종 매출 시간별 (24 ~ 06  %)
                    avg_profit_per_24_06 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s8"]/div[2]/div[2]/div[2]/table[2]/tbody/tr[2]/td[8]',
                    )

                    # 고객 비중 클릭
                    click_element(
                        global_driver,
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[9]',
                    )

                    time.sleep(1 + random.random())

                    # 남 20대
                    avg_client_per_m_20 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[1]/td[2]',
                    )

                    # 남 30대
                    avg_client_per_m_30 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[1]/td[3]',
                    )

                    # 남 40대
                    avg_client_per_m_40 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[1]/td[4]',
                    )

                    # 남 50대
                    avg_client_per_m_50 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[1]/td[5]',
                    )

                    # 남 60대 이상
                    avg_client_per_m_60 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[1]/td[6]',
                    )

                    # 여 20대
                    avg_client_per_f_20 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[2]/td[2]',
                    )

                    # 여 30대
                    avg_client_per_f_30 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[2]/td[3]',
                    )

                    # 여 40대
                    avg_client_per_f_40 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[2]/td[4]',
                    )

                    # 여 50대
                    avg_client_per_f_50 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[2]/td[5]',
                    )

                    # 여 60대 이상
                    avg_client_per_f_60 = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s9"]/div[2]/div[2]/div[2]/table[1]/tbody/tr[2]/td[6]',
                    )

                    top5_menu_elements = []

                    # 뜨는 메뉴 대분류: 음식일때만
                    if main_category_id == 1:
                        try:
                            # 주요 메뉴/뜨는 메뉴 클릭
                            click_element(
                                global_driver,
                                wait,
                                By.XPATH,
                                '//*[@id="report1"]/div/div[3]/div/ul/li[11]',
                            )

                            time.sleep(1 + random.random())

                            # 뜨는 메뉴 클릭
                            click_element(
                                global_driver,
                                wait,
                                By.XPATH,
                                '//*[@id="s11"]/div[2]/div[2]/div/div[1]/ul/li[2]/button',
                            )

                            time.sleep(1 + random.random())

                            wait.until(
                                EC.presence_of_element_located(
                                    (
                                        By.XPATH,
                                        '//*[@id="s11"]/div[2]/div[2]',
                                    )
                                )
                            )

                            for i in range(5):
                                try:
                                    element = wait.until(
                                        EC.presence_of_element_located(
                                            (
                                                By.XPATH,
                                                f'//*[@id="popular_graph"]/div/div[2]/div[2]/table/tbody/tr[{i + 1}]/td[3]',
                                            )
                                        )
                                    )
                                    top5_menu_elements.append(element.text)
                                except:
                                    top5_menu_elements.append(None)
                        except:
                            top5_menu_elements = [None] * 5
                    else:
                        top5_menu_elements = [None] * 5

                    top_menu_1 = top5_menu_elements[0]
                    top_menu_2 = top5_menu_elements[1]
                    top_menu_3 = top5_menu_elements[2]
                    top_menu_4 = top5_menu_elements[3]
                    top_menu_5 = top5_menu_elements[4]

                    data: CommercialDistrictInsert = {
                        "city_id": city_id,
                        "district_id": district_id,
                        "sub_district_id": sub_district_id,
                        ###
                        "biz_main_category_id": main_category_id,
                        "biz_sub_category_id": sub_category_id,
                        "biz_detail_category_id": detail_category_id,
                        ###
                        "national_density": convert_to_int_float(national_density)
                        or 0.0,
                        "city_density": convert_to_int_float(city_density) or 0.0,
                        "district_density": convert_to_int_float(district_density)
                        or 0.0,
                        "sub_district_density": convert_to_int_float(
                            sub_district_density
                        )
                        or 0.0,
                        ###
                        "market_size": convert_to_int_float(market_size) * 10000 or 0, # 원데이터로 저장
                        ###
                        "average_payment": convert_to_int_float(average_payment) or 0,
                        "usage_count": convert_to_int_float(usage_count) or 0,
                        ###
                        "average_sales": convert_to_int_float(average_sales) * 10000 or 0, # 원데이터로 저장
                        "operating_cost": convert_to_int_float(operating_cost) or 0,
                        "food_cost": convert_to_int_float(food_cost) or 0,
                        "employee_cost": convert_to_int_float(employee_cost) or 0,
                        "rental_cost": convert_to_int_float(rental_cost) or 0,
                        "tax_cost": convert_to_int_float(tax_cost) or 0,
                        "family_employee_cost": convert_to_int_float(
                            family_employee_cost
                        )
                        or 0,
                        "ceo_cost": convert_to_int_float(ceo_cost) or 0,
                        "etc_cost": convert_to_int_float(etc_cost) or 0,
                        "average_profit": convert_to_int_float(average_profit) or 0,
                        ###
                        "avg_profit_per_mon": convert_to_int_float(avg_profit_per_mon)
                        or 0.0,
                        "avg_profit_per_tue": convert_to_int_float(avg_profit_per_tue)
                        or 0.0,
                        "avg_profit_per_wed": convert_to_int_float(avg_profit_per_wed)
                        or 0.0,
                        "avg_profit_per_thu": convert_to_int_float(avg_profit_per_thu)
                        or 0.0,
                        "avg_profit_per_fri": convert_to_int_float(avg_profit_per_fri)
                        or 0.0,
                        "avg_profit_per_sat": convert_to_int_float(avg_profit_per_sat)
                        or 0.0,
                        "avg_profit_per_sun": convert_to_int_float(avg_profit_per_sun)
                        or 0.0,
                        ###
                        "avg_profit_per_06_09": convert_to_int_float(
                            avg_profit_per_06_09
                        )
                        or 0.0,
                        "avg_profit_per_09_12": convert_to_int_float(
                            avg_profit_per_09_12
                        )
                        or 0.0,
                        "avg_profit_per_12_15": convert_to_int_float(
                            avg_profit_per_12_15
                        )
                        or 0.0,
                        "avg_profit_per_15_18": convert_to_int_float(
                            avg_profit_per_15_18
                        )
                        or 0.0,
                        "avg_profit_per_18_21": convert_to_int_float(
                            avg_profit_per_18_21
                        )
                        or 0.0,
                        "avg_profit_per_21_24": convert_to_int_float(
                            avg_profit_per_21_24
                        )
                        or 0.0,
                        "avg_profit_per_24_06": convert_to_int_float(
                            avg_profit_per_24_06
                        )
                        or 0.0,
                        ###
                        "avg_client_per_m_20": convert_to_int_float(avg_client_per_m_20)
                        or 0.0,
                        "avg_client_per_m_30": convert_to_int_float(avg_client_per_m_30)
                        or 0.0,
                        "avg_client_per_m_40": convert_to_int_float(avg_client_per_m_40)
                        or 0.0,
                        "avg_client_per_m_50": convert_to_int_float(avg_client_per_m_50)
                        or 0.0,
                        "avg_client_per_m_60": convert_to_int_float(avg_client_per_m_60)
                        or 0.0,
                        "avg_client_per_f_20": convert_to_int_float(avg_client_per_f_20)
                        or 0.0,
                        "avg_client_per_f_30": convert_to_int_float(avg_client_per_f_30)
                        or 0.0,
                        "avg_client_per_f_40": convert_to_int_float(avg_client_per_f_40)
                        or 0.0,
                        "avg_client_per_f_50": convert_to_int_float(avg_client_per_f_50)
                        or 0.0,
                        "avg_client_per_f_60": convert_to_int_float(avg_client_per_f_60)
                        or 0.0,
                        ###
                        "top_menu_1": top_menu_1,
                        "top_menu_2": top_menu_2,
                        "top_menu_3": top_menu_3,
                        "top_menu_4": top_menu_4,
                        "top_menu_5": top_menu_5,
                        "y_m": None,
                    }

                    # print(data)

                    insert_commercial_district(data)

                    end_time = time.time()

                    print(
                        f"Execution finished at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
                    )

                    print(f"Total execution time: {end_time - start_time} seconds")

                else:
                    print(
                        f"NO DATA : {city_text}, {district_text}, {sub_district_text}, {main_category_text}, {sub_category_text} : index {detail_category_idx}."
                    )
                    # continue
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                # print(
                #     f"Error processing {city_text}, {district_text}, {sub_district_text}, {main_category_text}, {sub_category_text} : index {detail_category_idx}. {str(e)}"
                # )
                continue
    except Exception as e:
        # print(
        #     f"Exception occurred search_commercial_district(), detail_category_idx: {detail_category_idx} {e}."
        # )
        return None
    finally:
        pass


# def calculate_optimal_workers():
#     # CPU 코어 수 확인
#     cpu_cores = cpu_count()

#     # 시스템 메모리 사용량 확인
#     memory = psutil.virtual_memory()
#     memory_usage_percent = memory.percent

#     # CPU 사용량 확인
#     cpu_usage_percent = psutil.cpu_percent()

#     # 기본 워커 수 설정
#     default_workers = cpu_cores * 2  # CPU 코어 수의 2배를 기본값으로

#     # CPU와 메모리 사용량에 따른 조정
#     if cpu_usage_percent > 80 or memory_usage_percent > 80:
#         # 시스템 부하가 높을 경우 워커 수 감소
#         optimal_workers = max(1, default_workers // 2)
#     elif cpu_usage_percent < 30 and memory_usage_percent < 50:
#         # 시스템 여유가 많을 경우 워커 수 증가
#         optimal_workers = default_workers * 2
#     else:
#         optimal_workers = default_workers

#     # 최소 1개, 최대 32개로 제한
#     return max(1, min(optimal_workers, 32))


def execute_task_in_thread(value, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            # executor.submit(get_sub_district_count, 시/도 Index, value),
            # executor.submit(get_sub_district_count, 0, value),
            # executor.submit(get_district_count, value),
        ]
        for future in futures:
            future.result()


@time_execution
def execute_parallel_tasks():

    values = []

    # 시/도 기준으로 실행 /  전국 17 시/도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

    # 시/군/구 기준으로 실행
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]  # 서울특별시
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15]  # 부산광역시
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8]  # 대구광역시
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]  # 인천광역시
    # values = [0, 1, 2, 3, 4]  # 광주광역시
    # values = [0, 1, 2, 3, 4]  # 대전광역시
    # values = [0, 1, 2, 3, 4]  # 울산광역시
    # values = [0]  # 세종특별자치시
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]  # 경기도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  # 충청북도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14]  # 충청남도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17, 18, 19, 20, 21]  # 전라남도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17, 18, 19, 20, 21]  # 경상북도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17]  # 경상남도
    # values = [0, 1]  # 제주측별자치도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13 ,14, 15, 16, 17]  # 강원특별자치도
    # values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12 ,13]  # 전북특별자치도

    # 시/도 의 시/군/구 갯수 -1
    values = values

    # max_workers = calculate_optimal_workers()
    # print(f"Optimal number of workers: {max_workers}")
    # max_workers = min(max_workers, len(values))
    # print(f"최종 max_workers: {max_workers}")

    max_workers = len(values) + 2
    print(f"최종 max_workers: {max_workers}")

    with Pool(processes=max_workers) as pool:
        pool.starmap(execute_task_in_thread, [(value, max_workers) for value in values])


if __name__ == "__main__":
    execute_parallel_tasks()
    print(f"상권분석 END")

    # 컴퓨터 종료 명령어 (운영체제에 따라 다름)
    # if os.name == "nt":  # Windows
    #     os.system("shutdown /s /t 1")
    # else:  # Unix-based (Linux, macOS)
    #     os.system("shutdown -h now")
