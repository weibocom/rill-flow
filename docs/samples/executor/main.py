# coding:utf-8
import asyncio
import json

import requests
from fastapi import FastAPI, Request
import uvicorn
from concurrent.futures import ThreadPoolExecutor
import logging

file_directory = "/data1/executor/src/"

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=8)


@app.post('/greet.json')
async def greet_api(request: Request):
    body_raw = {}
    try:
        name = request.query_params.get("user")
        if name is None:
            return {"result_type": "FAILED", "error_msg": "params user is null"}

        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        greetWord = body_raw[name]
        return {"result_type", "SUCCESS", "result", greetWord}
    except Exception as e:
        logging.error("greet processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "greet processing failed"}


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
            return {"result_type": "SUCCESS"}
        else:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(executor, processor, body_raw, request)
            logging.info("executor response:%s", result)
            return result
    except Exception as e:
        logging.error("executor processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "Executor processing failed"}


@app.post('/callback.json')
async def callback_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        return {"callback_result": body_raw["executor_result"] * 10}
    except Exception as e:
        logging.error("callback processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "callback processing failed"}


@app.post('/calculate.json')
async def calculate_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        if len(body_raw) == 0:
            return {"result_type": "FAILED", "error_msg": "body is empty"}
        total = 0
        for i in body_raw["callback_result_list"]:
            total += i
        return {"sum": total}
    except Exception as e:
        logging.error("calculate processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "calculate processing failed"}


@app.post('/sub_callback.json')
async def sub_callback_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        if len(body_raw) == 0:
            return {"result_type": "FAILED", "error_msg": "body is empty"}

        return {"callback_result": body_raw["sum"] * 10}
    except Exception as e:
        logging.error("sub_callback processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "sub_callback processing failed"}


@app.post('/choice.json')
async def choice_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        if len(body_raw) == 0:
            return {"result_type": "FAILED", "error_msg": "body is empty"}
        total = 0
        for i in range(body_raw["input_num"]):
            total += i * i
        return {"sum": total}
    except Exception as e:
        logging.error("choice processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "choice processing failed"}


@app.post('/print.json')
async def print_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        if len(body_raw) == 0:
            return {"result_type": "FAILED", "error_msg": "body is empty"}
        return {"result_type", "SUCCESS", "print_result", body_raw["sum"]}
    except Exception as e:
        logging.error("print processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "print processing failed"}


@app.post("/gpt_output_processor.json")
async def gpt_output_processor(request: Request):
    try:
        body_raw = await request.json()

        if "result" not in body_raw:
            return {"result_type", "FAILED", "result", "chatgpt output is empty!!"}

        chatgpt_response = body_raw["result"]
        if not chatgpt_response or "error" in chatgpt_response:
            return {"result_type": "FAILED", "result": chatgpt_response.get("error", "chatgpt output is empty!!")}

        choices_array = chatgpt_response.get("choices", [])

        if not choices_array:
            return {"result_type": "FAILED", "result": "chatgpt response is invalid!!"}

        concatenated_content = " ".join(choice.get("message", {}).get("content", "") for choice in choices_array)

        return {"result_type": "SUCCESS", "result": concatenated_content}
    except Exception as e:
        logging.error("gpt output processor is failed", e)
        return {"result_type": "FAILED", "result": "gpt output processor is failed"}


def async_processor(request_body: dict, request: Request):
    result_type = "SUCCESS"
    try:
        executor_response = processor(request_body, request)
    except Exception as e:
        executor_response = "executor processor is failed"
        result_type = "FAILED"
        logging.error("executor processor is failed. request body:%s", request_body, e)
    executor_response["result_type"] = result_type
    callback(executor_response, request.headers.get("X-Callback-Url"))


def processor(request_body: dict, request: Request):
    if len(request_body) == 0:
        raise Exception("request body is empty")

    return {"executor_result": request_body["segment_item"] * 10}


def callback(executor_response, callback_url):
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(executor_response)

    try:
        response = requests.post(callback_url, headers=headers, data=payload)
        logging.info("callback rill flow is success. callback url:%s, callback body:%s, response:%s", callback_url,
                     payload, response)
    except Exception as e:
        logging.error("callback rill flow is failed. callback url:%s, callback body:%s", callback_url, payload, e)


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=8000)
