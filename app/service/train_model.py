import torch
from datasets import load_dataset
from diffusers import StableDiffusionPipeline, UNet2DConditionModel
from transformers import CLIPTextModel, CLIPTokenizer
from torch.optim import AdamW
from torch.utils.data import DataLoader
from torchvision import transforms
from PIL import Image, ImageFile
from tqdm import tqdm  # tqdm을 임포트합니다

# 손상된 이미지를 불러올 수 있도록 설정
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Hugging Face API 토큰
token = "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"

# 1. 데이터셋 로드 및 전처리 함수 정의
data_file = "C:/workspace/python/wizMarket_test_ai/app/data/kfood_data.json"
dataset = load_dataset("json", data_files=data_file)["train"]

# 이미지 전처리 함수
def preprocess(example):
    image = Image.open(example["image_path"]).convert("RGB")
    transform = transforms.Compose([
        transforms.Resize((512, 512)),
        transforms.ToTensor(),
        transforms.Normalize([0.5], [0.5])
    ])
    example["image"] = transform(image)
    example["caption"] = example["label"]
    return example

dataset = dataset.map(preprocess)
dataset.set_format(type="torch", columns=["image", "caption"])
dataloader = DataLoader(dataset, batch_size=1, shuffle=True)

# 2. Stable Diffusion 모델 로드 (float32 적용)
model_id = "CompVis/stable-diffusion-v1-4"
pipe = StableDiffusionPipeline.from_pretrained(model_id, use_auth_token=token, torch_dtype=torch.float32)

# 3. LoRA를 적용할 UNet 설정 (float32로 변경)
unet = pipe.unet.to("cpu").float()
text_encoder = CLIPTextModel.from_pretrained(model_id, subfolder="text_encoder").to("cpu").float()
tokenizer = CLIPTokenizer.from_pretrained(model_id, subfolder="tokenizer")

# 4. 최적화 설정
optimizer = AdamW(unet.parameters(), lr=1e-5)

# 5. 학습 루프 설정
num_epochs = 1  # 반복 횟수 설정

for epoch in range(num_epochs):
    print(f"Epoch {epoch+1}/{num_epochs} started...")
    # tqdm을 사용하여 배치 진행 상황을 표시
    for batch in tqdm(dataloader, desc=f"Training epoch {epoch+1}/{num_epochs}"):
        # 이미지와 캡션을 가져와서 전처리 (float32 적용)
        images = batch["image"].to("cpu").float()
        captions = batch["caption"]

        # 텍스트 인코딩
        text_inputs = tokenizer(captions, padding="max_length", truncation=True, return_tensors="pt").to("cpu")
        encoder_hidden_states = text_encoder(text_inputs.input_ids).last_hidden_state.float()

        # 랜덤 타임스텝 설정
        timestep = torch.randint(0, 1000, (images.size(0),), device="cpu").long()

        # 이미지 텐서를 4채널로 변환 (RGBA)
        images = torch.cat([images, torch.randn(images.size(0), 1, 512, 512, device=images.device).float()], dim=1)

        # 랜덤 노이즈 생성 (float32)
        noise = torch.randn_like(images).float()

        # 노이즈 예측
        noise_pred = unet(images + noise, timestep=timestep, encoder_hidden_states=encoder_hidden_states).sample

        # 손실 계산 및 역전파
        loss = torch.nn.functional.mse_loss(noise_pred, noise)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

        # 현재 손실 출력
        tqdm.write(f"Loss: {loss.item()}")

# 6. 모델 저장
pipe.save_pretrained("./fine_tuned_stable_diffusion")
print("모델이 성공적으로 저장되었습니다.")
