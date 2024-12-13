import time
import base64
import os
from runwayml import RunwayML
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment variables
api_key = os.getenv("RUNWAYML_API_SECRET")

if not api_key:
    raise ValueError("API key not found. Please check your .env file.")

# Initialize the RunwayML client with the API key
client = RunwayML(api_key=api_key)

image_path = 'C:/Users/jyes_semin/Desktop/Data/image/a6276114-428e-476a-ad79-f067f88d1190.png'

# Encode image to base64
with open(image_path, "rb") as f:
    base64_image = base64.b64encode(f.read()).decode("utf-8")

# Create a new image-to-video task
task = client.image_to_video.create(
    model='gen3a_turbo',
    prompt_image=f"data:image/png;base64,{base64_image}",
    prompt_text='Make the subject of the image move vividly.',
)
task_id = task.id

# Poll the task until it's complete
print("Task created. Polling for status...")
while True:
    task = client.tasks.retrieve(task_id)
    if task.status in ['SUCCEEDED', 'FAILED']:
        break
    print(f"Task status: {task.status}. Retrying in 10 seconds...")
    time.sleep(10)

# Check final status
if task.status == 'SUCCEEDED':
    print("Task succeeded!")
    result_url = task.output[0]  # output is a list, so take the first element
    print("Result URL:", result_url)
else:
    print("Task failed.")
