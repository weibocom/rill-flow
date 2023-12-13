# main.py
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

    command = ['python', 'inference.py',
               '--checkpoint_path', 'face_detection/detection/sfd/wav2lip.pth',
               '--face', 'MonaLisa.jpg',
               '--audio', audio_path,
               '--outfile', out_file]
    run_subprocess(command)

    callback_body = {
        "segment": out_file
    }
    callback(callback_url, callback_body)


def run_subprocess(command, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            subprocess.run(command, check=True)
            return True
        except subprocess.CalledProcessError:
            retries += 1
    print(f"Subprocess failed after {max_retries} retries.")
    return False


def callback(callback_url, callback_body):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(callback_body)
    requests.post(callback_url, headers=headers, data=payload)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9002)