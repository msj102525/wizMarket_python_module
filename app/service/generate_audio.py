import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("FACE_KEY")

API_URL = "https://api-inference.huggingface.co/models/facebook/musicgen-small"
headers = {"Authorization": f"Bearer {api_key}"}  # f-string으로 API 키 전달

def query(payload):
	response = requests.post(API_URL, headers=headers, json=payload)
	return response.content

audio_bytes = query({
	"inputs": "a catchy beat for a podcast intro",
})
# You can access the audio with IPython.display for example
from IPython.display import Audio
Audio(audio_bytes)
with open("output.mp3", "wb") as f:
    f.write(audio_bytes)