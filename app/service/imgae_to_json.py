import os
import json

# 이미지 파일 제이슨 파일로 만들기
# 이미지 파일이 있는 상위 폴더 경로
base_dir = r"C:\Users\jyes_semin\Desktop\Data\download\food\kfood.zip"

# JSON 데이터 리스트 생성
data = []

# 폴더 내 모든 이미지 파일에 대해 처리
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.lower().endswith(('.png', '.jpg', '.jpeg')):
            # 음식명을 폴더 이름으로 설정 (예: '갈비구이')
            label = os.path.basename(root)
            # 이미지 파일의 전체 경로
            image_path = os.path.join(root, file)
            # JSON 데이터로 추가
            data.append({"image_path": image_path, "label": label})

# JSON 파일로 저장
json_path = os.path.join(base_dir, "kfood_data.json")
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"JSON 파일이 생성되었습니다: {json_path}")
