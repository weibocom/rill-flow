from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request
from pydantic import BaseModel
from scipy.io.wavfile import write as write_wav
from transformers import AutoProcessor, BarkModel
from bark import SAMPLE_RATE
import requests
import uvicorn
import json
import os

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=20)
data_dir = os.getenv('WORK_DIR', "/tmp")


class Item(BaseModel):
    text: str


@app.on_event("startup")
async def on_startup():
    load_model()


@app.post("/bark/generate")
def bark_generator_async(execution_id: str, name: str, item: Item, request: Request):
    executor.submit(transformers_generate, execution_id, name, item.text, request.headers.get("X-Callback-Url"))
    return {"result": "success"}


def load_model():
    model_path = "./bark/models"
    global bark_model, processor, voice_preset
    bark_model = BarkModel.from_pretrained(model_path)
    processor = AutoProcessor.from_pretrained("suno/bark")
    voice_preset = "v2/en_speaker_9"


def transformers_generate(execution_id: str, name: str, text: str, callback_url: str):
    inputs = processor(text, voice_preset=voice_preset)
    audio_array = bark_model.generate(**inputs)
    audio_array = audio_array.cpu().numpy().squeeze()

    output_directory = data_dir + "/" + execution_id + "/wav"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file_path = output_directory + "/" + name + ".wav"
    write_wav(output_file_path, SAMPLE_RATE, audio_array)

    callback_body = {
        "audio_path": output_file_path
    }
    callback(callback_url, callback_body)


def callback(callback_url, callback_body):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(callback_body)
    response = requests.post(callback_url, headers=headers, data=payload)
    print(response)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9001)
