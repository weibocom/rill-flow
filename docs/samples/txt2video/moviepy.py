# main.py
@app.post("/moviepy/generate")
def concatenate_video_async(execution_id: str, name: str, request: dict):
    executor.submit(concatenate_video, execution_id, name, request)
    return {"result": "success"}


def concatenate_video(execution_id: str, name: str, request: dict):
    clips = []
    for segment in sorted(request["segments"]):
        clip = VideoFileClip(segment)
        clips.append(clip)

    # merge video
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
    video_with_subtitles = CompositeVideoClip([video, subtitles.set_position(lambda t: ('center', 2000+t))])
    video_subtitles_path = video_path + "/subtitles_output.mp4"
    video_with_subtitles.write_videofile(video_subtitles_path)

    finish_task(execution_id, name, video_subtitles_path)


def finish_task(execution_id, name, video_path):
    headers = {"Content-Type": "application/json"}
    RILL_FLOW_HOST = os.environ.get(
        "PROXY_RILL_FLOW_DOMAIN", "127.0.0.1"
    )
    RILL_FLOW_PORT = os.environ.get(
        "PROXY_RILL_FLOW_PORT", "8080"
    )
    url = f"http://{RILL_FLOW_HOST}:{RILL_FLOW_PORT}/flow/finish.json"
    params = {'execution_id': execution_id, 'task_name': name}
    data = {
        "video_path": video_path
    }
    requests.post(url, data=json.dumps(data), params=params, headers=headers)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9003)