# coding:utf-8
import json
import os
import subprocess

import requests
from fastapi import FastAPI, Request
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import uuid

file_directory = "/data1/executor/src/"

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=8)


@app.post('/executor.json')
async def executor_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)

        mode = request.headers.get("X-Mode")
        callbackUrl = request.headers.get("X-Callback-Url")
        if mode is not None and mode == "async" and callbackUrl is not None:
            # 函数异步执行
            executor.submit(async_processor, body_raw, request)
            result = {"result_type": "SUCCESS"}
            return result
        else:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(executor, processor, body_raw, request)
            logging.info("executor response:%s", result)
            return result
    except Exception as e:
        logging.error("executor processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "Executor processing failed"}


def async_processor(request_body: dict, request: Request):
    result_type = "SUCCESS"
    try:
        executor_response = processor(request_body, request)
    except Exception as e:
        executor_response = "executor processor is failed"
        result_type = "FAILED"
        logging.error("executor processor is failed. request body:%s", request_body, e)
    finally:
        executor_response["result_type"] = result_type
        callback(executor_response, request.headers.get("X-Callback-Url"))


def processor(request_body: dict, request: Request):
    if len(request_body) == 0:
        raise Exception("request body is empty")
    if request.headers.get("language_type") is not None and request.headers.get("language_type") == "shell":
        return processor_shell(request_body)
    else:
        request_body["executor_tag"] = "executor"
        return request_body


def callback(executor_response, callback_url):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(executor_response)

    try:
        response = requests.post(callback_url, headers=headers, data=payload)
        logging.info("callback rill flow is success. callback url:%s, callback body:%s, response:%s", callback_url,
                     payload, response)
    except Exception as e:
        logging.error("callback rill flow is failed. callback url:%s, callback body:%s", callback_url, payload, e)


def processor_shell(request_body: dict):
    file_name = file_directory + str(uuid.uuid1()) + ".sh"
    try:
        write_file(file_name, request_body)
        result = subprocess.run(["/bin/bash", file_name],
                                capture_output=True,
                                text=True)
        if result.returncode == 0:
            return {"result": result.stdout}
        else:
            return {"result": result.stderr}
    except Exception as e:
        logging.error("processor shell is failed. request_body:%s", request_body, e)
        raise Exception(e)
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)


def write_file(file_name, request_body: dict):
    file = open(file_name, "w")
    file.write(request_body["data"])
    file.flush()
    file.close()


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=8000)
