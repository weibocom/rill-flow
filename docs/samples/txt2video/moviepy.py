# main.py
class Item(BaseModel):
    segments: List[str]


@app.post("/moviepy/generate")
def concatenate_video_async(execution_id: str, item: Item, request: Request):
    executor.submit(concatenate_video, execution_id, item.segments, request.headers.get("X-Callback-Url"))
    return {"result": "success"}


def concatenate_video(execution_id: str, segments: List, callback_url: str):
    # merge video
    clips = [VideoFileClip(segment) for segment in sorted(segments)]
    final_clip = concatenate_videoclips(clips)
    video_path = data_dir + "/" + execution_id
    if not os.path.exists(video_path):
        os.makedirs(video_path)
    video_clip_path = video_path + "/merge_video_output.mp4"
    final_clip.write_videofile(video_clip_path)

    # add subtitles
    video = VideoFileClip(video_clip_path)
    generator = lambda txt: TextClip(txt, font='Arial', fontsize=52, color='white')
    subs = SubtitlesClip('./subtitles.srt', generator)
    subtitles = SubtitlesClip(subs, generator)
    video_with_subtitles = CompositeVideoClip([video, subtitles.set_position(lambda t: ('center', 2000 + t))])
    video_subtitles_path = video_path + "/subtitles_output.mp4"
    video_with_subtitles.write_videofile(video_subtitles_path)

    callback_body = {
        "video_path": video_path
    }
    callback(callback_url, callback_body)


def callback(callback_url, callback_body):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(callback_body)
    requests.post(callback_url, headers=headers, data=payload)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9003)