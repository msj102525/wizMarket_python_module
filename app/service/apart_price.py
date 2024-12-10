from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
import datetime
import time
from app.db.connect import *
from app.crud.apart_price import select_all_region, update_loc_info_apart_price
import re

def crawl_keyword():
    start_time = datetime.datetime.now()  # 시작 시간 기록
    print(f"크롤링 시작 시간: {start_time}")
    region_list = select_all_region()
    connection = get_db_connection()
    """
    메인 크롤링 함수
    """
    # 글로벌 드라이버 설정
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--start-maximized")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # 웹사이트 접속
        driver.get("https://sgis.kostat.go.kr/view/house/houseAnalysisMap")
        time.sleep(3)

        # 닫기 버튼 클릭
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "body > div.openguide-wrap > div.openlayer.openguide > div > div.check-today > button"))
        ).click()

        # 상세 분석 버튼 클릭
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#nav > ul > li:nth-child(2) > button"))
        ).click()

        # 시/도 선택 영역 클릭
        span_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,
                                            "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sido > span"))
        )
        ActionChains(driver).move_to_element(span_element).click().perform()
        time.sleep(1)
        # 시/도 목록 가져오기
        city_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR,
                                                "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sido.active > ul > li"))
        )
        init_flag = True
        # 시/도 반복
        for city_index, city_element in enumerate(city_elements[4:], start=1):  # 첫 번째 항목(전체) 제외
            city_name = city_element.text.strip()

            # 시/도 선택
            city_element.click()
            time.sleep(1)
            # 시/군/구 선택 영역 클릭
            span_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sgg > span"))
            )
            ActionChains(driver).move_to_element(span_element).click().perform()
            time.sleep(1)
            # 시/군/구 목록 가져오기
            district_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR,
                                                    "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sgg.active > ul > li"))
            )
            time.sleep(1)
            # 시/군/구 반복
            for district_index, district_element in enumerate(district_elements[1:], start=1):  # 첫 번째 항목(전체) 제외
                district_name = district_element.text.strip()
                if district_name == "옹진군":
                    continue
                # 시/군/구 선택
                district_element.click()
                time.sleep(1)
                # 데이터 수집 작업 수행
                process_region(driver, city_name, district_name, region_list, connection, init_flag)
                init_flag = False
                # 시/군/구가 마지막인 경우 시/도 선택 화면으로 돌아가기
                if district_index == len(district_elements) - 1:  # 마지막 시/군/구 확인
                    break  # 시/군/구 반복 종료

                # 다시 시/군/구 선택 화면으로 돌아가기
                span_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR,
                                                    "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sgg > span"))
                )
                ActionChains(driver).move_to_element(span_element).click().perform()
                time.sleep(1)
            # 시/도 선택 화면으로 돌아가기
            span_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "#detailLayer > div.nav-layer__box.scroll > div > div.area-select-wrap > div.area-select.sido > span"))
            )
            ActionChains(driver).move_to_element(span_element).click().perform()
            time.sleep(1)
    finally:
        driver.quit()
        end_time = datetime.datetime.now()
        print(f"크롤링 종료 시간: {end_time}")
        elapsed_time = end_time - start_time  # 실행 시간 계산
        print(f"총 실행 시간: {elapsed_time}")



def process_region(driver, city, district, region_list, connection, init_flag):
    """
    데이터 수집 작업
    """
    try:
        if init_flag:
            # 주택 선택 (첫 실행에만 수행)
            span_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "#detailLayer > div.nav-layer__box.scroll > div > div.choice-all-wrap > ul > li.detail-house-btn > i"))
            )
            ActionChains(driver).move_to_element(span_element).click().perform()
            time.sleep(1)
            # 면적당 아파트 선택
            span_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "#main > div.openlayer.popup-index1.house-box.rs-popup > div > div.popup-container.scroll > ul > li:nth-child(8) > a"))
            )
            ActionChains(driver).move_to_element(span_element).click().perform()
            time.sleep(1)
            # 높음 선택
            span_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "#main > div.openlayer.popup-index1.house-box.rs-popup > div > div.popup-container.scroll > ul > li.active > a > div > div.Sortby.dis-f > div > label:nth-child(1) > span"))
            )
            ActionChains(driver).move_to_element(span_element).click().perform()
            time.sleep(1)
        # 분석 선택
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "#detailLayer > div.button-wrap > button.btn.assay.active"))
        ).click()

        # 잠깐 대기
        time.sleep(1)

        # 첫번째 지역 선택 
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 
                                        "#recommendArea > div > div > div.basic.active > div.local-list > ul > li:nth-child(1)"))
        ).click()
        time.sleep(1)
        # 보드 펼치기
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 
                                        "#main > div.data-board.middle > div.data-board__title > button"))
        ).click()

        # 잠깐 대기
        time.sleep(1)

        try:
            # div 영역 가져오기
            div_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "area-column-chart-1")))
            time.sleep(1)
            # SVG 태그 찾기
            svg_elements = div_element.find_elements(By.TAG_NAME, "svg")
            time.sleep(1)

            if svg_elements:
                # 첫 번째 SVG 태그 내 g 태그 찾기
                g_elements = svg_elements[0].find_elements(By.CLASS_NAME, "highcharts-series-group")
                time.sleep(0.5)
                if g_elements:
                    # 첫 번째 g 태그에서 모든 path 태그 찾기
                    all_paths = g_elements[0].find_elements(By.TAG_NAME, "path")
                    time.sleep(0.5)
                    for index, path in enumerate(all_paths):
                        try:
                            # 각 path 태그 위로 마우스 이동
                            ActionChains(driver).move_to_element(path).perform()

                            # div class="highcharts-label highcharts-tooltip highcharts-color-0" 찾기
                            inner_div = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div > div.highcharts-label.highcharts-tooltip.highcharts-color-0"))
                            )

                            # inner_div 아래 span 태그를 포함하는 첫 번째 div 찾기
                            child_div_1 = inner_div.find_element(By.CSS_SELECTOR, "span > div:nth-child(1)")

                            sub_district_name = child_div_1.find_element(By.CSS_SELECTOR,"span").text
                            # print(sub_district_name)

                            # inner_div 아래 span 태그를 포함하는 두 번째 div 찾기
                            child_div_2 = inner_div.find_element(By.CSS_SELECTOR, "span > div:nth-child(2)")
                            price_text = child_div_2.find_element(By.CSS_SELECTOR, "span").text
                            price_number = re.search(r"[\d,\.]+", price_text).group()
                            price_number = float(price_number.replace(",", "")) * 1000 * 3.3 # 숫자 처리 및 * 1000
                            price_number_int = int(price_number) 
                            price = price_number_int
                            # print(price)
                            time.sleep(0.5)
                            # DB에 있는 지역과 비교
                            insert_data(region_list, city, district, sub_district_name, price, connection)

                        except Exception as e:
                            print(f"path 태그 {index + 1} 처리 중 오류 발생: {e}")
                else:
                    print("g 태그 리스트가 비어 있습니다.")
            else:
                print("SVG 태그가 없습니다.")

        except Exception as e:
            print(f"오류 발생: {e}")


        # 닫기 버튼
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "#main > div.movelayer.pop-data-board.scroll.ui-draggable.ui-draggable-handle > div > div.popup-header > button"))
        ).click()
        time.sleep(1.5)
        # 뒤로가기 버튼
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "#recommendArea > div > div > div.title-h2 > button"))
        ).click()
        time.sleep(1.5)
        # 확인 버튼
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "#main > div.openlayer.popup-notice01 > div > div.btn-area > button.btn.check"))
        ).click()

        time.sleep(3)

        # 데이터 수집 로직

    except Exception as e:
        print(f"{city} - {district} 데이터 수집 중 오류 발생: {e}")


def insert_data(region_list, city, district, sub_district_name, apart_price, connection):
    """
    지역 정보와 가격 데이터를 삽입하는 함수
    """
    if district == '세종시':
        district = '세종특별자치시'
    
    district = district.split()[0]
    
    sub_district_name = sub_district_name.replace('·', '.')

    if sub_district_name == "도화2.3동":
        sub_district_name = "도화2,3동"
    
    if sub_district_name == "숭의1.3동":
        sub_district_name = "숭의1,3동"
    
    if sub_district_name == "용현1.4동":
        sub_district_name = "용현1,4동"

    try:
        # 지역 일치 항목 찾기
        matching_region = next(
            (region for region in region_list 
             if region['city_name'] == city 
             and region['district_name'] == district 
             and region['sub_district_name'] == sub_district_name),
            None
        )

        if matching_region:
            # 일치하는 지역 정보가 있으면 ID 값으로 삽입
            city_id = matching_region['city_id']
            district_id = matching_region['district_id']
            sub_district_id = matching_region['sub_district_id']

            # print(f"삽입할 데이터: city_id={city_id}, district_id={district_id}, sub_district_id={sub_district_id}, apart_price={apart_price}")
            data = {
                "city_id" : city_id,
                "district_id" : district_id,
                "sub_district_id" : sub_district_id,
                "apart_price" : apart_price
            }
            # 데이터베이스 삽입 로직 추가
            # print(data)
            update_loc_info_apart_price(connection, data)

        else:
            print(f"일치하는 지역을 찾을 수 없습니다: {city}, {district}, {sub_district_name}")

    except Exception as e:
        print(f"데이터 삽입 중 오류 발생: {e}")



if __name__ == "__main__":
    crawl_keyword()
