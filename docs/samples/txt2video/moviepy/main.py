import json
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests
import uvicorn
from fastapi import FastAPI, Request
from moviepy.editor import *
from pydantic import BaseModel

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=20)
data_dir = os.getenv('WORK_DIR', "/tmp")


class Subtitles(BaseModel):
    text: str
    video_path: str


class Concatenate(BaseModel):
    segments: List[str]


@app.post("/moviepy/subtitles")
def subtitles_async(execution_id: str, name: str, item: Subtitles, request: Request):
    executor.submit(subtitles, execution_id, name, item.video_path, item.text, request.headers.get("X-Callback-Url"))
    return {"result": "success"}


@app.post("/moviepy/concatenate")
def concatenate_async(execution_id: str, item: Concatenate, request: Request):
    executor.submit(concatenate, execution_id, item.segments, request.headers.get("X-Callback-Url"))
    return {"result": "success"}


def subtitles(execution_id: str, name: str, video_path: str, text: str, callback_url: str):
    video_clip = VideoFileClip(video_path)
    subtitle_clip = TextClip(text, font='Arial', fontsize=25, color='white', method='caption', size=video_clip.size, align='South')
    subtitle_clip = subtitle_clip.set_duration(video_clip.duration)
    video_with_subtitles = CompositeVideoClip([video_clip, subtitle_clip])

    subtitles_video_path = data_dir + "/" + execution_id + "/subtitles"
    if not os.path.exists(subtitles_video_path):
        os.makedirs(subtitles_video_path)
    video_subtitles_path = os.path.join(subtitles_video_path, f"{name}.mp4")
    video_with_subtitles.write_videofile(video_subtitles_path)

    callback_body = {
        "segment": video_subtitles_path
    }
    callback(callback_url, callback_body)


def concatenate(execution_id: str, segments: List, callback_url: str):
    clips = [VideoFileClip(segment) for segment in sorted(segments)]
    final_clip = concatenate_videoclips(clips)

    video_path = data_dir + "/" + execution_id
    if not os.path.exists(video_path):
        os.makedirs(video_path)
    video_clip_path = video_path + "/result.mp4"
    final_clip.write_videofile(video_clip_path)

    callback_body = {
        "result_video_path": video_clip_path
    }
    callback(callback_url, callback_body)


def callback(callback_url, callback_body):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(callback_body)
    response = requests.post(callback_url, headers=headers, data=payload)
    print(response)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9003)
