from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyperclip
import pyautogui
import time
from PIL import ImageGrab, Image
import os
import cv2

def con_to_naver_map():
    # 크롬 옵션 설정
    chrome_options = Options()
    # chrome_options.add_argument("--start-maximized") 
    # chrome_options.add_argument("--headless=new")  # 브라우저 창 숨기기
    chrome_options.add_argument("--window-size=1920,1080") 
    chrome_options.add_argument("--enable-clipboard-write")

    # 크롬 드라이버 초기화
    chrome_service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    # driver.set_window_position(3000, 3000)

    # 자동 로그인할 웹사이트 열기
    driver.get('https://map.naver.com/p/entry/place/1815776005?lng=126.9035629&lat=37.526477&placePath=%2Fhome&searchType=place&c=15.00,0,0,0,dh')

    # 로딩 대기
    time.sleep(2)

    # iframe 전환
    WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, 'entryIframe')))
    time.sleep(2)

    review_field = None

    try:
        # 리뷰 버튼 요소 대기
        review_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[4]/div/div/div/div/a[4]"))
        )
        review_field.click()
        time.sleep(1)  # 클릭 후 로딩 대기

        register_count_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div[1]/div/div/div[1]/em"))
        )
        register_count = register_count_element.text
        
        register_pop_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div[1]/div/div/div[1]/span"))
        )
        register_pop = register_pop_element.text


        # 리뷰 데이터 수집
        reviews = []
        while True:
            try:
                # 현재 페이지의 리뷰 수집
                review_elements = driver.find_elements(By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div[3]/div[1]/ul/li")
                for review_element in review_elements:
                    review_text = review_element.text
                    reviews.append(review_text)

                # "더보기" 버튼 확인
                more_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div[3]/div[2]/div/a/span"))
                )

                # "더보기" 버튼의 텍스트 확인
                if more_button.text != "더보기":
                    break

                # "더보기" 버튼 클릭
                driver.find_element(By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div[3]/div[2]/div/a").click()
                time.sleep(2)  # 클릭 후 로딩 대기

            except Exception as e:
                print(f"Error occurred during review fetching: {e}")
                break
        
        output_path = '로다커피 방문자 리뷰.txt'
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(f"Register Count: {register_count}\n")
            file.write(f"Register Pop: {register_pop}\n")
            file.write("\n방문자 Reviews:\n")
            for idx, review in enumerate(reviews, start=1):
                file.write(f"\nReview {idx}: \n{review}\n")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        driver.quit()
    
    # try:
    #     chrome_options = Options()
    #     # chrome_options.add_argument("--start-maximized") 
    #     # chrome_options.add_argument("--headless=new")  # 브라우저 창 숨기기
    #     chrome_options.add_argument("--window-size=1920,1080") 
    #     chrome_options.add_argument("--enable-clipboard-write")

    #     # 크롬 드라이버 초기화
    #     chrome_service = Service(ChromeDriverManager().install())
    #     driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    #     # driver.set_window_position(3000, 3000)

    #     # 자동 로그인할 웹사이트 열기
    #     driver.get('https://map.naver.com/p/entry/place/18608205?lng=126.9065498&lat=37.5281964&placePath=%2Fhome&entry=plt&searchType=place&c=15.00,0,0,0,dh')

    #     # 로딩 대기
    #     time.sleep(2)

    #     # iframe 전환
    #     WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, 'entryIframe')))
    #     time.sleep(2)

    #     # 리뷰 버튼 요소 대기
    #     review_field = WebDriverWait(driver, 10).until(
    #         EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[4]/div/div/div/div/a[4]"))
    #     )
    #     review_field.click()
    #     time.sleep(1)  # 클릭 후 로딩 대기

    #     register_count_element = WebDriverWait(driver, 10).until(
    #         EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div/h2/div[1]/em"))
    #     )
    #     register_count = register_count_element.text
        

    #     blog_field = WebDriverWait(driver, 10).until(
    #         EC.presence_of_element_located((By.XPATH, "//*[@id='_subtab_view']/div/a[2]"))
    #     )
    #     blog_field.click()
    #     time.sleep(1)  # 클릭 후 로딩 대기

    #     blog_titles = []
    #     blog_reviews = []
    #     while True:
    #         try:
    #             # 현재 페이지의 블로그 데이터 수집
    #             blog_title_elements = driver.find_elements(By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div/div[1]/ul/li/a/div[3]")
    #             blog_review_elements = driver.find_elements(By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div/div[1]/ul/li/a/div[4]/span")

    #             for title_element, review_element in zip(blog_title_elements, blog_review_elements):
    #                 blog_titles.append(title_element.text)
    #                 blog_reviews.append(review_element.text)

    #             # "더보기" 버튼 확인
    #             more_button = WebDriverWait(driver, 5).until(
    #                 EC.presence_of_element_located((By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div/div[2]/div/a/span"))
    #             )

    #             # "더보기" 버튼의 텍스트 확인
    #             if more_button.text != "더보기":
    #                 break

    #             # "더보기" 버튼 클릭
    #             driver.find_element(By.XPATH, "//*[@id='app-root']/div/div/div/div[6]/div[3]/div/div[2]/div/a").click()
    #             time.sleep(2)  # 클릭 후 로딩 대기

    #         except Exception as e:
    #             print(f"Error occurred during blog fetching: {e}")
    #             break


    #     output_path = '목포회집 블로그.txt'

    #     with open(output_path, 'w', encoding='utf-8') as file:
    #         # 블로그 데이터 저장
    #         file.write("\nBlog Titles and Reviews:\n")
    #         for title, review in zip(blog_titles, blog_reviews):
    #             file.write(f"Title: {title} \nReview: {review}\n\n")

        
    #     print(f"Data saved to {output_path}")


    # except Exception as e:
    #     print(f"Error occurred: {e}")
    # finally:
    #     driver.quit()

if __name__=="__main__":
    con_to_naver_map()