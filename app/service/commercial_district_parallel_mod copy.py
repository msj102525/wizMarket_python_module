from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import re
import time
import os
from typing import Dict, List

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
)

# BIZ_MAP_URL = "https://m.nicebizmap.co.kr/analysis/analysisFree"
BIZ_MAP_URL = "https://m.nicebizmap.co.kr/"


def setup_driver():
    driver_path = os.path.join(
        os.path.dirname(__file__), "../", "drivers", "chromedriver.exe"
    )

    options = Options()
    options.add_argument("--start-fullscreen")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

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
    except UnexpectedAlertPresentException:
        handle_unexpected_alert(wait._driver)
    except Exception as e:
        print(
            f"Exception occurred click: {e} for element located by {by} with value {value}. Skipping to next element."
        )


def read_element(wait, by, value):
    try:
        element = wait.until(EC.presence_of_element_located((by, value)))
        text = element.text
        time.sleep(0.5)
        return text
    except Exception as e:
        print(
            f"Exception occurred read: {e} for element located by {by} with value {value}. Skipping to next element."
        )


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


# def get_sub_district_count(city_idx, district_count):
def get_sub_district_count(start_idx: int, end_idx: int):
    driver = setup_driver()
    try:
        for district_idx in tqdm(
            range(start_idx, end_idx), f"{start_idx} : 시/군/구 Progress"
        ):
            try:

                print(f"idx: {district_idx}")
                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)
                # 서울
                city_idx = 0

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                time.sleep(2.5)

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
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
                # print(f"읍/면/동 갯수: {len(sub_district_ul_li)}")

                # print(f"시/도 : {city_text}, 시/군/구 : {district_text}")

                get_main_category(city_idx, district_idx, len(sub_district_ul_li))
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                print(f"Error processing district_idx {district_idx}: {str(e)}")
                continue
            finally:
                driver.quit()
                driver = setup_driver()
    except Exception as e:
        print(
            f"Exception occurred get_sub_district_count(), district_idx {district_idx}: {e}."
        )
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_main_category(city_idx, district_idx, sub_district_count):
    driver = setup_driver()
    try:
        for sub_district_idx in tqdm(range(sub_district_count), "읍/면/동 Progress"):
            try:
                print(f"idx: {district_idx}")
                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(2.5)

                sub_district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(2.5)

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
                print(
                    f"Exception occurred, 대분류1 반복 err, district_idx:  {district_idx}: {str(e)}"
                )
                continue
            finally:
                driver.quit()
                driver = setup_driver()

        for sub_district_idx in tqdm(range(sub_district_count), "읍/면/동 Progress"):
            try:

                print(f"idx: {district_idx}")
                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(2.5)

                sub_district_text = click_element(
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
                # print(f"대분류2 갯수 : {len(main_category_ul_2_li)}")

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
                print(
                    f"Exception occurred: 대분류2 반복 오류, district_idx: {district_idx}: {str(e)}"
                )
                continue
            finally:
                driver.quit()
                driver = setup_driver()
    except Exception as e:
        print(
            f"Exception occurred get_main_category(), sub_district: {sub_district_idx} {e}."
        )
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_sub_category(
    city_idx, district_idx, sub_district_idx, main_category_count, m_c_ul
):
    driver = setup_driver()
    try:
        for main_category_idx in range(main_category_count):
            try:
                # print(f"idx: {district_idx}")
                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(2.5)

                sub_district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(2.5)

                main_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]/button',
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
                print(
                    f"Error processing, main_category_idx: {main_category_idx}: {str(e)}"
                )
                continue
            finally:
                driver.quit()
                driver = setup_driver()

    except Exception as e:
        print(
            f"Exception occurred, get_sub_category(), main_category_idx: {main_category_idx} {e}."
        )
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def get_detail_category(
    city_idx,
    district_idx,
    sub_district_idx,
    main_category_idx,
    sub_category_count,
    m_c_ul,
):
    driver = setup_driver()
    try:
        for sub_category_idx in range(0, sub_category_count * 2, 2):
            try:
                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(2.5)

                sub_district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(2.5)

                main_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]/button',
                )

                time.sleep(2.5)

                sub_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 1}]/button',
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
                print(f"Error processing sub_category_idx {sub_category_idx}: {str(e)}")
                continue
            finally:
                driver.quit()
                driver = setup_driver()
    except Exception as e:
        print(
            f"Exception occurred get_detail_category(), sub_category_idx: {sub_category_idx} {e}."
        )
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def search_commercial_district(
    city_idx,
    district_idx,
    sub_district_idx,
    main_category_idx,
    sub_category_idx,
    detail_category_count,
    m_c_ul,
):
    driver = setup_driver()

    try:
        for detail_category_idx in range(detail_category_count):
            try:

                start_time = time.time()
                print(
                    f"Execution started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
                )

                driver.get(BIZ_MAP_URL)
                wait = WebDriverWait(driver, 60)
                driver.implicitly_wait(10)

                time.sleep(2.5)

                click_element(wait, By.XPATH, '//*[@id="gnb1"]/li[1]/a')

                # 분석 지역
                click_element(
                    wait, By.XPATH, '//*[@id="pc_sheet01"]/div/div[2]/div[2]/ul/li[1]/a'
                )

                time.sleep(2.5)

                city_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{city_idx + 1}]/a',
                )

                time.sleep(2.5)

                district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{district_idx + 1}]/a',
                )

                time.sleep(2.5)

                sub_district_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[4]/div[2]/div[2]/div/div[2]/ul/li[{sub_district_idx + 1}]/a',
                )

                time.sleep(2.5)

                main_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul}]/li[{main_category_idx + 1}]/button',
                )

                time.sleep(2.5)

                sub_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 1}]/button',
                )

                time.sleep(2.5)

                detail_category_text = click_element(
                    wait,
                    By.XPATH,
                    f'//*[@id="basicReport"]/div[5]/div[3]/div[2]/div/ul[{m_c_ul + 1}]/ul/li[{sub_category_idx + 2}]/ul/li[{detail_category_idx + 1}]/button',
                )

                time.sleep(2.5)

                try:
                    city_id = get_or_create_city_id(city_text)
                    if city_id:
                        district_id = get_or_create_district_id(city_id, district_text)
                        if city_id & district_id:
                            sub_district_id = get_or_create_sub_district_id(
                                city_id, district_id, sub_district_text
                            )
                except Exception as e:
                    print(f"시 구 동 조회 오류 : {e}")

                ###########################

                try:
                    main_category_id = get_or_create_biz_main_category_id(
                        main_category_text
                    )
                    if main_category_id:
                        sub_category_id = get_or_create_biz_sub_category_id(
                            main_category_id, sub_category_text
                        )
                        if main_category_id & sub_category_id:

                            detail_category_text = detail_category_text.replace(
                                "(확장 분석)", ""
                            ).strip()

                            detail_category_id = get_or_create_biz_detail_category_id(
                                sub_category_id, detail_category_text
                            )
                except Exception as e:
                    print(f"시 구 동 조회 오류 : {e}")

                if detail_category_text:
                    print(detail_category_text)

                    # detail_category_text = detail_category_text.replace(
                    #     "(확장 분석)", ""
                    # ).strip()

                    #########################################

                    # 상권분석 보기
                    click_element(wait, By.XPATH, '//*[@id="pcBasicReport"]')

                    time.sleep(2.5)

                    # '//*[@id="report1"]' 요소가 나타날 때까지 기다리기
                    wait.until(
                        EC.presence_of_element_located(
                            (
                                By.XPATH,
                                '//*[@id="report1"]/div/div[3]/div/div',
                            )
                        )
                    )

                    time.sleep(2.5)

                    # 분석 텍스트 보기 없애기
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[4]/div[1]/div/div/div/div[1]/div[2]/label',
                    )

                    time.sleep(2.5)

                    # 표 전제보기
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[4]/div[1]/div/div/div/div[1]/div[1]/label',
                    )

                    time.sleep(2.5)

                    # 밀집도 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[2]/a',
                    )

                    time.sleep(2.5)

                    # 전국 해당 업종수 밀집도 데이터
                    national_density = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s2"]/div[2]/div[2]/div/div[2]/table/tbody/tr[3]/td[2]',
                    )

                    time.sleep(2.5)

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
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[3]/a',
                    )

                    time.sleep(2.5)

                    # 해당지역 업종 총 시장규모(원) 제일 최신
                    market_size = read_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s3"]/div[2]/div[2]/div[2]/table/tbody/tr/td[7]',
                    )

                    # 결제단가 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[6]/a',
                    )

                    time.sleep(2.5)

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

                    # 비용/수익통계 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[7]/a',
                    )

                    time.sleep(2.5)

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

                    # 매출 비중 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[8]/a',
                    )

                    time.sleep(2.5)

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
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[9]/a',
                    )

                    time.sleep(2.5)

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

                    # 주요 메뉴/뜨는 메뉴 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="report1"]/div/div[3]/div/ul/li[11]/a',
                    )

                    time.sleep(2.5)

                    # 뜨는 메뉴 클릭
                    click_element(
                        wait,
                        By.XPATH,
                        '//*[@id="s11"]/div[2]/div[2]/div/div[1]/ul/li[2]/button',
                    )

                    time.sleep(2.5)

                    try:
                        top5_menu_elements = []

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
                        "national_density": convert_to_int_float(national_density),
                        "city_density": convert_to_int_float(city_density),
                        "district_density": convert_to_int_float(district_density),
                        "sub_district_density": convert_to_int_float(
                            sub_district_density
                        ),
                        ###
                        "market_size": convert_to_int_float(market_size),
                        ###
                        "average_payment": convert_to_int_float(average_payment),
                        "usage_count": convert_to_int_float(usage_count),
                        ###
                        "average_sales": convert_to_int_float(average_sales),
                        "operating_cost": convert_to_int_float(operating_cost),
                        "food_cost": convert_to_int_float(food_cost),
                        "employee_cost": convert_to_int_float(employee_cost),
                        "rental_cost": convert_to_int_float(rental_cost),
                        "tax_cost": convert_to_int_float(tax_cost),
                        "family_employee_cost": convert_to_int_float(
                            family_employee_cost
                        ),
                        "ceo_cost": convert_to_int_float(ceo_cost),
                        "etc_cost": convert_to_int_float(etc_cost),
                        "average_profit": convert_to_int_float(average_profit),
                        ###
                        "avg_profit_per_mon": convert_to_int_float(avg_profit_per_mon),
                        "avg_profit_per_tue": convert_to_int_float(avg_profit_per_tue),
                        "avg_profit_per_wed": convert_to_int_float(avg_profit_per_wed),
                        "avg_profit_per_thu": convert_to_int_float(avg_profit_per_thu),
                        "avg_profit_per_fri": convert_to_int_float(avg_profit_per_fri),
                        "avg_profit_per_sat": convert_to_int_float(avg_profit_per_sat),
                        "avg_profit_per_sun": convert_to_int_float(avg_profit_per_sun),
                        ###
                        "avg_profit_per_06_09": convert_to_int_float(
                            avg_profit_per_06_09
                        ),
                        "avg_profit_per_09_12": convert_to_int_float(
                            avg_profit_per_09_12
                        ),
                        "avg_profit_per_12_15": convert_to_int_float(
                            avg_profit_per_12_15
                        ),
                        "avg_profit_per_15_18": convert_to_int_float(
                            avg_profit_per_15_18
                        ),
                        "avg_profit_per_18_21": convert_to_int_float(
                            avg_profit_per_18_21
                        ),
                        "avg_profit_per_21_24": convert_to_int_float(
                            avg_profit_per_21_24
                        ),
                        "avg_profit_per_24_06": convert_to_int_float(
                            avg_profit_per_24_06
                        ),
                        ###
                        "avg_client_per_m_20": convert_to_int_float(
                            avg_client_per_m_20
                        ),
                        "avg_client_per_m_30": convert_to_int_float(
                            avg_client_per_m_30
                        ),
                        "avg_client_per_m_40": convert_to_int_float(
                            avg_client_per_m_40
                        ),
                        "avg_client_per_m_50": convert_to_int_float(
                            avg_client_per_m_50
                        ),
                        "avg_client_per_m_60": convert_to_int_float(
                            avg_client_per_m_60
                        ),
                        "avg_client_per_f_20": convert_to_int_float(
                            avg_client_per_f_20
                        ),
                        "avg_client_per_f_30": convert_to_int_float(
                            avg_client_per_f_30
                        ),
                        "avg_client_per_f_40": convert_to_int_float(
                            avg_client_per_f_40
                        ),
                        "avg_client_per_f_50": convert_to_int_float(
                            avg_client_per_f_50
                        ),
                        "avg_client_per_f_60": convert_to_int_float(
                            avg_client_per_f_60
                        ),
                        ###
                        "top_menu_1": top_menu_1,
                        "top_menu_2": top_menu_2,
                        "top_menu_3": top_menu_3,
                        "top_menu_4": top_menu_4,
                        "top_menu_5": top_menu_5,
                    }

                    print(data)

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
                    continue
            except UnexpectedAlertPresentException:
                handle_unexpected_alert(wait._driver)
            except Exception as e:
                print(
                    f"Error processing {city_text}, {district_text}, {sub_district_text}, {main_category_text}, {sub_category_text} : index {detail_category_idx}. {str(e)}"
                )
                continue
            finally:
                driver.quit()
                driver = setup_driver()
    except Exception as e:
        print(
            f"Exception occurred search_commercial_district(), detail_category_idx: {detail_category_idx} {e}."
        )
        return None
    finally:
        try:
            if driver:
                driver.quit()
        except Exception as quit_error:
            print(f"Error closing driver: {str(quit_error)}")


def execute_task_in_thread(start, end):
    # with ThreadPoolExecutor(max_workers=16) as executor:
    #     futures = [
    #         executor.submit(get_sub_district_count, start, end),
    #     ]
    #     for future in futures:
    #         future.result()
    get_sub_district_count(start, end)


def execute_parallel_tasks():
    start_time = time.time()
    print(
        f"Total execution started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
    )

    ranges = [
        (0, 3),
        (3, 6),
        (6, 9),
        (9, 12),
        (12, 15),
        (15, 18),
        (18, 21),
        (21, 25),
    ]

    with Pool(processes=len(ranges)) as pool:
        pool.starmap(execute_task_in_thread, ranges)

    end_time = time.time()
    print(
        f"Total execution finished at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
    )
    print(f"Total execution time: {end_time - start_time} seconds")


if __name__ == "__main__":
    execute_parallel_tasks()
    print(f"상권분석 END")
