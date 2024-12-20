# Import everything needed to edit video clips
from moviepy import *
import base64
from openai import OpenAI
from PIL import Image, ImageDraw, ImageFont
import io

def combine_video_audio():
# 입력 파일 경로
    video_path = "C:/Users/jyes_semin/Desktop/Data/video.mp4"
    audio_path = "C:/Users/jyes_semin/Desktop/Data/audio.mp3"
    output_path = "C:/Users/jyes_semin/Desktop/Data/video_with_audio.mp4"

    # 영상 로드
    video = VideoFileClip(video_path)

    # 오디오 로드 및 자르기
    audio = AudioFileClip(audio_path).subclipped(0, video.duration)  # 영상 길이에 맞게 오디오 자르기

    # 자른 오디오를 영상에 추가
    video_with_audio = video.with_audio(audio)

    # 결과 저장
    video_with_audio.write_videofile(output_path, codec="libx264", audio_codec="aac")
    

def add_text_to_video():
    # Load the video clip
    clip = VideoFileClip("C:/Users/jyes_semin/Desktop/Data/test_generate/video_with_audio.mp4")

    font = "C:/workspace/python/wizMarket_python_module/app/font/batang.ttf"

    # Create a text clip
    txt_clip = TextClip(
        font=font,
        text="The Blender Foundation and\nPeach Project presents",
        font_size=50,
        color="#fff",
        text_align="center",
    )

    # Set the duration of the text clip to match the video clip
    txt_clip = txt_clip.with_duration(clip.duration)

    # Center the text
    txt_clip = txt_clip.with_position('center')

    # Composite the text clip onto the video clip
    result = CompositeVideoClip([clip, txt_clip])

    # Export the result to a file
    result.write_videofile("myvideo_with_text.mp4")



if __name__ == "__main__":
    # combine_video_audio()
    add_text_to_video()
