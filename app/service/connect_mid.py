import requests
from dotenv import load_dotenv
import os
import webbrowser
from PIL import Image


# .env 파일에서 환경 변수 로드
load_dotenv()

# 환경 변수 가져오기
USE_API_TOKEN = os.getenv("USE_API_TOKEN")
DIS_USE_TOKEN = os.getenv("DIS_USE_TOKEN")
DIS_SER_ID = os.getenv("DIS_SER_ID")
DIS_CHA_ID = os.getenv("DIS_CHA_ID")

def con_mid_image():
    apiUrl = "https://api.useapi.net/v2/jobs/imagine"
    token = USE_API_TOKEN
    prompt = "Astronaut holding a lightsaber and racing on a cute cat"
    discord = DIS_USE_TOKEN
    server = DIS_SER_ID
    channel = DIS_CHA_ID
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    body = {
        "prompt": f"{prompt}",
        "discord": f"{discord}",
        "server": f"{server}",
        "channel": f"{channel}"
    }
    response = requests.post(apiUrl, headers=headers, json=body)
    print(response.json()["jobid"])
    return response.json()["jobid"]


def get_mid_image():
    token = USE_API_TOKEN
    jobid = "job:20241218021941022-user:1308-channel:1316232391072944131-bot:midjourney"
    apiUrl = f"https://api.useapi.net/v2/jobs/?jobid={jobid}"
    headers = {
        "Content-Type": "application/json", 
        "Authorization" : f"Bearer {token}"
    }
    response = requests.get(apiUrl, headers=headers)
    # print(response, response.json())
    data = response.json()
    link = data["attachments"]
    print(link)
    url = link[0]['proxy_url']
    webbrowser.open(url)
    # if response.status_code == 200:
    #     data = response.json()
    #     # print(data)
    #     # 'attachments' 리스트에서 'proxy_url' 가져오기
    #     attachments = data.get('attachments')
    #     if attachments and len(attachments) > 0:
    #         proxy_url = attachments[0].get('proxy_url')
    #         if proxy_url:
    #             print("Opening URL:", proxy_url)
    #             webbrowser.open(proxy_url)  # 브라우저에서 열기
    #             return proxy_url
    #         else:
    #             print("proxy_url not found in attachments[0].")
    #     else:
    #         print("No attachments found in response.")
    # else:
    #     print("Error:", response.status_code, response.text)


def download():
    url = "https://media.discordapp.net/attachments/1316232391072944131/1318764698442797068/jangsemin_Astronaut_holding_a_lightsaber_and_racing_on_a_cute_c_095193c2-a28a-498f-8b96-4feda280a2b7.png?ex=676382d5&is=67623155&hm=305c7dc43122ab2dc4aaa2f9b08c511c68ce7d8271dd79778e852562130f1954&"
    image_response = requests.get(url)
    if image_response.status_code == 200:
            with open("downloaded_image.png", "wb") as file:  # 파일 저장
                file.write(image_response.content)
            print("Image saved as 'downloaded_image.png'")
    else:
        print("Failed to download image:", image_response.status_code)

def divide():
    # 이미지 불러오기
    image_path = "downloaded_image.png"  # 업로드된 이미지 경로
    output_dir = "C:/workspace/python/wizMarket_python_module/app/image"  # 분할된 이미지 저장 경로

    # 이미지 열기
    image = Image.open(image_path)
    width, height = image.size

    # 4분할 좌표 설정 (좌측 상단, 우측 상단, 좌측 하단, 우측 하단)
    coords = [
        (0, 0, width // 2, height // 2),                # 좌측 상단
        (width // 2, 0, width, height // 2),            # 우측 상단
        (0, height // 2, width // 2, height),           # 좌측 하단
        (width // 2, height // 2, width, height),       # 우측 하단
    ]

    # 4분할 이미지 저장
    for i, (left, top, right, bottom) in enumerate(coords):
        cropped_image = image.crop((left, top, right, bottom))
        output_path = f"{output_dir}image_part_{i+1}.png"
        cropped_image.save(output_path)
        print(f"Saved: {output_path}")



def send_slash_command():

    token = DIS_USE_TOKEN
    prompt = "Astronaut holding a lightsaber and racing on a cute cat"
    guild_id = DIS_SER_ID
    channel_id = DIS_CHA_ID
    application_id = '381363681419853834'


    url = "https://discord.com/api/v10/interactions"
    headers = {
        "Authorization": f"{token}",
        "Content-Type": "application/json",
    }
    body = {
        "type": 2,  # Type 2: Slash Command
        "application_id": application_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "data": {
            "name": "imagine",
            "options": [
                {
                    "name": "prompt",
                    "value": prompt,
                    "type": 3  # Type 3: STRING
                }
            ],
        },
    }
    response = requests.post(url, headers=headers, json=body)
    print(response, response.json())
    return response.json()

if __name__=="__main__":
    # job_id =  con_mid_image()
    # get_mid_image()
    # download()
    # divide()
    send_slash_command()