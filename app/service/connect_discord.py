import datetime
import requests

discord_url = "https://discord.com/api/webhooks/1316234071164194816/P0kaAaiYjsTxBXt6sHAf9G4X0Ok6NUPWTocty-5xmWMA1EuBgtbKgZNlUJv8SIuslKQ6"
#디스코드 채널로 메세지 전송
def discord_send_message(text):
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(text)}"}
    requests.post(discord_url, data=message)
    print(message)

if __name__ == "__main__":
    discord_send_message("테스트입니다.")

