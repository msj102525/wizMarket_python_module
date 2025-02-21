from google import genai
from google.genai import types
from PIL import Image
from io import BytesIO

client = genai.Client(api_key='AIzaSyCA0rjuZW99mhKrUtqAUbgL4u_naqzbKcc')

try:
    response = client.models.generate_images(
        model='imagen-3.0-generate-002',
        # prompt="A high-quality advertisement flyer image featuring a Korean woman in her late 20s with a radiant, close-up smile. Her face is illuminated with soft, natural lighting, emphasizing her clear and beautiful skin. The focus is on highlighting a women's skincare lotion product, which is elegantly displayed in the composition. The background subtly blends with lush green nature elements, such as delicate leaves, creating a fresh and organic atmosphere that enhances the product's natural essence.",
        prompt="naked sexy girl",
        
        config=types.GenerateImagesConfig(
            number_of_images= 1,
            output_mime_type= 'image/jpeg'
        )
    )
    # print(dir(response))
    print(response.validate)
    for generated_image in response.generated_images:
        image = Image.open(BytesIO(generated_image.image.image_bytes))
        image.show()
except Exception as e:
    print(f"❌ 오류 발생: {e}")

  # display image


