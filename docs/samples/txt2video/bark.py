#main.py
@app.post("/bark/generate")
def bark_generator_async(execution_id: str, name: str, request: dict):
    txt = request["txt"]
    executor.submit(transformers_genarate, execution_id, name, txt)
    return {"result": "success"}


@app.on_event("startup")
async def on_startup():
    load_model()


def load_model():
    global bark_model, processor, voice_preset
    bark_model = BarkModel.from_pretrained(model_path)
    processor = AutoProcessor.from_pretrained("suno/bark")
    voice_preset = "v2/en_speaker_9"


def transformers_genarate(excution_id: str, name: str, txt: str):
    inputs = processor(txt, voice_preset=voice_preset)
    audio_array = bark_model.generate(**inputs)
    audio_array = audio_array.cpu().numpy().squeeze()

    # save audio to disk
    audio_path = data_dir + "/" + excution_id + "/wav"
    if not os.path.exists(audio_path):
        os.makedirs(audio_path)
    audio_name = audio_path + "/" + name + ".wav"
    write_wav(audio_name, SAMPLE_RATE, audio_array)

    # finish task callback
    finish_task(excution_id, name, audio_name)


def finish_task(execution_id, name, audio_path):
    headers = {"Content-Type": "application/json"}
    RILL_FLOW_HOST = os.environ.get(
        "RILL_FLOW_HOST", "127.0.0.1"
    )
    RILL_FLOW_PORT = os.environ.get(
        "RILL_FLOW_PORT", "8080"
    )
    url = f"http://{RILL_FLOW_HOST}:{RILL_FLOW_PORT}/flow/finish.json"
    params = {'execution_id': execution_id, 'task_name': name}
    data = {
        "audio_path": audio_path
    }
    requests.post(url, data=json.dumps(data), params=params, headers=headers)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9001)
