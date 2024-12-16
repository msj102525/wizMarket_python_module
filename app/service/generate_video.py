import requests
import os
from dotenv import load_dotenv
import time

load_dotenv()

api_secret = os.getenv("STABILITY_API_SECRET")


def ge_id():
    response = requests.post(
        f"https://api.stability.ai/v2beta/image-to-video",
        headers={
            "authorization": f"Bearer {api_secret}"
        },
        files={
            "image": open("C:/Users/jyes_semin/Desktop/Data/image/pizza.png", "rb")
        },
        data={
            "seed": 0,
            "cfg_scale": 1.8,
            "motion_bucket_id": 127
        },
    )

    response_json = response.json()
    print("Response Status Code:", response.status_code)
    if 'errors' in response_json:
            print("Errors:", response_json['errors'])
    generation_id = response.json().get('id')
    print("Generation ID:", generation_id)
    return generation_id


def get_video():
    generation_id  = ge_id()
    time.sleep(15)
    response = requests.request(
        "GET",
        f"https://api.stability.ai/v2beta/image-to-video/result/{generation_id}",
        headers={
            'accept': "video/*",  # Use 'application/json' to receive base64 encoded JSON
            'authorization': f"Bearer {api_secret}"
        },
    )

    if response.status_code == 202:
        print("Generation in-progress, try again in 10 seconds.")
    elif response.status_code == 200:
        print("Generation complete!")
        with open("video.mp4", 'wb') as file:
            file.write(response.content)
    else:
        raise Exception(str(response.json()))
    

if __name__ == "__main__":
    get_video()