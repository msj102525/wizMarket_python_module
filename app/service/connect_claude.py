import anthropic
import os

api_key = os.getenv("CLAUDE_KEY")
client = anthropic.Anthropic(api_key=api_key)

def get_response_from_claude(question):
    result_text = ""
    
    response = client.messages.create(
        model="claude-3-opus-20240229",
        max_tokens=1000,
        temperature=0.0,
        system="Respond only in Korean.",
        messages=[{"role": "user", "content": question}]
    )
    
    if not response.content or not isinstance(response.content, list):
        result_text = "No response or unexpected response format."
    else:
        response_texts = [block.text for block in response.content if hasattr(block, 'text')]
        result_text = " ".join(response_texts)
 
    return result_text


if __name__=="__main__":
    question = """
    '다음과 같은 내용을 바탕으로 온라인 광고 콘텐츠를 제작하려고 합니다. 
    잘 어울리는 광고 문구를 생성해주세요.
    - 현재 날짜, 날씨, 시간, 계절 등의 상황에 어울릴 것
    - 40자 이상 작성할 것
    - 특수기호, 이모티콘은 제외할 것
    - 광고 채널 : 문자메시지 형태로 작성할 것
    - 주제 : 매장 소개 형태로 작성할 것
                
    매장명 : 인더키친 몽뜨레셰프 청담본점
    주소 : 서울 강남구 도산대로 435
    업종 : 패밀리레스토랑
    날짜 : 2024-12-11 (수요일) 17:39
    날씨 : 맑음, 5.91℃
    매출이 가장 높은 시간대 : 21~24시
    매출이 가장 높은 남성 연령대 : 남자 40대
    매출이 가장 높은 여성 연령대 : 여자 30대
    주제 세부 정보 : 

    """
    response = get_response_from_claude(question)
    print(response)