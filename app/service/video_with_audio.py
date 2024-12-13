# Import everything needed to edit video clips
from moviepy import *
import base64
from openai import OpenAI



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


if __name__ == "__main__":
    combine_video_audio()