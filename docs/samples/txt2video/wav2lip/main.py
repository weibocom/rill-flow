import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import requests
import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=20)
data_dir = os.getenv('WORK_DIR', "/tmp")


class Item(BaseModel):
    audio_path: str


@app.post("/wav2lip/generate")
def generate_digital_human_async(execution_id: str, name: str, item: Item, request: Request):
    executor.submit(generate_digital_human, execution_id, name, item.audio_path, request.headers.get("X-Callback-Url"))
    return {"result": "success"}


def generate_digital_human(execution_id: str, name: str, audio_path: str, callback_url: str):
    mp4_path = data_dir + "/" + execution_id + "/mp4"
    if not os.path.exists(mp4_path):
        os.makedirs(mp4_path)
    out_file = os.path.join(mp4_path, f"{name}.mp4")

    subprocess.run(['python', 'inference.py',
                    '--checkpoint_path', 'face_detection/detection/sfd/wav2lip_gan.pth',
                    '--face', 'MonaLisa.jpg',
                    '--audio', audio_path,
                    '--outfile', out_file])

    callback_body = {
        "video_path": out_file
    }
    callback(callback_url, callback_body)


def callback(callback_url, callback_body):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(callback_body)
    response = requests.post(callback_url, headers=headers, data=payload)
    print(response)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9002)
