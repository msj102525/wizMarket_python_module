import os
from openai import OpenAI
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from app.crud.generate_image_text import select_generage_image_text
import uuid
from dotenv import load_dotenv
from datetime import datetime

########### AI 그림 및 대답 생성 ############

# OpenAI API 키 설정
api_key = os.getenv("GPT_KEY")
client = OpenAI(api_key=api_key)
# .env 파일 로드
load_dotenv()

today = datetime.today().strftime("%Y%m%d")

def create_text():
    data = select_generage_image_text()
    data_dict = data[0]

    # 각 값을 변수에 할당
    road_name = data_dict['ROAD_NAME']
    resident_j_score = data_dict['LOC_INFO_RESIDENT_J_SCORE']
    work_pop_j_score = data_dict['LOC_INFO_WORK_POP_J_SCORE']
    shop_j_score = data_dict['LOC_INFO_SHOP_J_SCORE']
    income_j_score = data_dict['LOC_INFO_INCOME_J_SCORE']
    mz_population_j_score = data_dict['LOC_INFO_MZ_POPULATION_J_SCORE']
    average_spend_j_score = data_dict['LOC_INFO_AVERAGE_SPEND_J_SCORE']
    average_sales_j_score = data_dict['LOC_INFO_AVERAGE_SALES_J_SCORE']
    house_j_score = data_dict['LOC_INFO_HOUSE_J_SCORE']

    gpt_content = """
        당신은 전문 조언자입니다. 
        가게 홍보 포스터에 사용할 문구를 작성해주세요
    """    

    content = f"""
        다음과 같은 입지 정보를 가진 한국식 프라이드 치킨 가게 운영 전략을 통해
        홍보 포스터에 사용할 문구를 작성해주세요.
        score 는 각 행정 구역별 10점 만점 기준으로 상대 평가입니다.

        주소 : {road_name}
        거주 인구 : {resident_j_score}
        세대 수 : {house_j_score}
        직장 인구 : {work_pop_j_score}
        매장 수 : {shop_j_score}
        수입 : {income_j_score}
        소비 : {average_spend_j_score}
        매출 : {average_sales_j_score}
        mz 세대 인구 : {mz_population_j_score}

        여러 문장이라면 구분자 ; 세미콜론을 사용해 주세요.
        모든 문장은 30자를 넘지 않게 해주세요
    """

    client = OpenAI(api_key=os.getenv("GPT_KEY"))

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": gpt_content},
            {"role": "user", "content": content},
        ],
    )
    report = completion.choices[0].message.content

    return report


# gpt ai 달리 사용 - url 생성
""" 이미지 생성 """
def generate_image(size="1024x1024"):
    prompt = '''
        돼지고기집 홍보용 포스터를 만들어줘
    '''
    try:
        response = client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size=size,
            n=1
        )
        image_url = response.data[0].url
        print(f"url : {image_url}")
        return image_url

    except Exception as e:
        print(f"이미지 생성 중 오류 발생: {e}")
        return None



# 허깅 페이스 디퓨저 모델 사용
def generate_diffusion():
    prompt = "Korean-style fried chicken restaurant"
    token = os.getenv("FACE_KEY")

    API_URL = "https://api-inference.huggingface.co/models/stabilityai/stable-diffusion-3.5-large"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    data = {
        "inputs": prompt
    }

    # API 요청 보내기
    response = requests.post(API_URL, headers=headers, json=data)

    if response.status_code == 200:
        # 생성된 이미지를 메모리로 불러옴
        image = Image.open(BytesIO(response.content))
        # 결과 이미지 저장 경로 설정 (ROOT_PATH 및 UUID 사용)
        root_path = os.getenv("ROOT_PATH")
        output_path = os.path.join(root_path, "app/image/generate_by_ai/diffusion", f"generated_diffusion_{today}_{uuid.uuid4()}.png")
        
        # 디렉토리가 없을 경우 생성
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 이미지 저장
        image.save(output_path)
        return output_path
    else:
        print(f"Failed to generate image: {response.status_code}, {response.json()}")





# 영화 포스터 모델
def generate_movie_poster():
    prompt = "Korean-style fried chicken restaurant"
    token = os.getenv("FACE_KEY")

    API_URL = "https://api-inference.huggingface.co/models/alex1105/movie-posters-v2"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    data = {
        "inputs": prompt
    }
    
    # API 요청 보내기
    response = requests.post(API_URL, headers=headers, json=data)
    # 응답 처리
    if response.status_code == 200:
        image = Image.open(BytesIO(response.content))
        root_path = os.getenv("ROOT_PATH")
        output_path = os.path.join(root_path, "app/image/generate_by_ai/movie_poster", f"generated_movie_poster_{today}_{uuid.uuid4()}.png")
        image.save(output_path)

        return output_path
    else:
        print(f"Failed to generate image: {response.status_code}, {response.json()}")


# 영화 포스터 + 텍스트
def add_text_to_image():
    # 이미지와 텍스트 기본 설정
    image_path = generate_movie_poster()
    text = create_text()

    image = Image.open(image_path)
    draw = ImageDraw.Draw(image)
    root_path = os.getenv("ROOT_PATH")
    font_path = os.path.join(root_path, "app", "font", "yang.otf") 
    font_size = 40
    font = ImageFont.truetype(font_path, font_size)

    # gpt 홍보 문구 + 허깅페이스 영화 포스터 결합
    # 이미지 크기 계산
    img_width, img_height = image.size
    
    # 텍스트를 세미콜론으로 구분하여 줄 나누기
    lines = text.split(';')  # 세미콜론으로 텍스트를 나누어 각 문장을 줄로 처리
    
    # 줄 높이 계산 (getbbox 사용)
    line_height = font.getbbox("A")[3] + 10  # 줄 간격을 약간 추가

    # 텍스트 위치 설정
    
    text_y = img_height - (line_height * len(lines)) - 40  # 하단에서 여백 확보

    # 여러 줄로 텍스트 추가
    for line in lines:
        line = line.strip()  # 각 문장 앞뒤 공백 제거
        text_width = font.getbbox(line)[2]  # 현재 줄의 텍스트 너비 계산
        text_x = (img_width - text_width) // 2
        draw.text((text_x, text_y), line, font=font, fill="white")
        text_y += line_height  # 다음 줄로 이동

    # 결과 이미지 저장 및 보기
    root_path = os.getenv("ROOT_PATH")
    output_path = os.path.join(root_path, "app/image/generate_by_ai/combine", f"generated_combine_{today}_{uuid.uuid4()}.png")
    image.save(output_path)
    image.show()



def stability_api():
    api_secret = os.getenv("STABILITY_API_SECRET")
    response = requests.post(
        f"https://api.stability.ai/v2beta/stable-image/generate/core",
        headers={
            "authorization": f"Bearer {api_secret}",
            "accept": "image/*"
        },
        files={"none": ''},
        data={
            "prompt": "Lighthouse on a cliff overlooking the ocean",
            "output_format": "png",
        },
    )

    if response.status_code == 200:
        with open("./lighthouse.png", 'wb') as file:
            file.write(response.content)
    else:
        raise Exception(str(response.json()))

if __name__ == "__main__":
    stability_api()