# main.py
@app.post("/wav2lip/generate")
def generate_digital_human_async(execution_id: str, name: str, request: dict):
    executor.submit(generate_digital_human, execution_id, name, request)
    return {"result": "success"}


def generate_digital_human(excution_id: str, name: str, request: dict):
    audio_path = request["audio_path"]
    mp4_path = data_dir + "/" + excution_id + "/mp4"
    if not os.path.exists(mp4_path):
        os.makedirs(mp4_path)
    out_file = os.path.join(mp4_path, f"{name}.mp4")
    subprocess.run(['python', 'inference.py',
                    '--checkpoint_path', 'face_detection/detection/sfd/wav2lip.pth',
                    '--face', 'MonaLisa.jpg',
                    '--audio', audio_path,
                    '--outfile', out_file])
    finish_task(excution_id, name, out_file)


def finish_task(execution_id, name, out_file):
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
        "segment": out_file
    }
    requests.post(url, data=json.dumps(data), params=params, headers=headers)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9002)