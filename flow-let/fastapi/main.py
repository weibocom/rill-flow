import asyncio
import logging
import os
import sys
import json
import requests
from fastapi import FastAPI, Request
import uvicorn
from concurrent.futures import ThreadPoolExecutor

upstream_url = os.getenv("upstream_url")
if upstream_url is None:
    sys.exit("env: upstream_url is required")

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=40)
header_black_list = ["x-mode", "x-callback-url"]

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.INFO)


@app.api_route("/{path:path}", methods=["get", "post"])
async def executor_proxy(request: Request, path: str = "/"):
    body = None
    if request.method == "POST":
        body = await request.json()

    mode = request.headers.get("X-Mode", default="sync")
    callbackUrl = request.headers.get("X-Callback-Url", default=None)
    logging.info("mode:{}, path:{},callbackUrl:{}, params:{}".format(mode, path, callbackUrl, request.query_params))

    if mode == "async" and callbackUrl is not None:
        executor.submit(async_dispatcher, request, body)
        return {"result_type": "SUCCESS"}
    else:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, dispatcher, request, body)
        return result


def async_dispatcher(request: Request, request_body: dict = None):
    result_type = "SUCCESS"
    executor_response = None
    try:
        executor_response = dispatcher(request, request_body)
    except Exception as e:
        executor_response = "executor processor is failed"
        result_type = "FAILED"
        logging.error("executor processor is failed. request body:%s", request_body, e)
        logging.exception("async_dispatcher is failed.", e)
    finally:
        executor_response["result_type"] = result_type
        callback(executor_response, request.headers.get("X-Callback-Url"))


def dispatcher(request: Request, request_body: dict = None):
    uri = request.url.path
    url = f"{upstream_url}{uri}"
    logging.info("url: %s", url)
    try:
        resp = requests.request(request.method, url, json=request_body, headers=copy_headers(request),
                                params=request.query_params)
        return resp.json()
    except Exception as e:
        logging.exception("dispatcher request url is failed. url:{}", url)
        raise e


def callback(executor_response, callback_url):
    headers = {"Content-Type": "application/json"}

    try:
        payload = json.dumps(executor_response)
        response = requests.post(callback_url, headers=headers, data=payload)
        logging.info("callback rill flow is success. callback url:%s, callback body:%s, response:%s", callback_url,
                     payload, response)
    except Exception as e:
        logging.error("callback rill flow is failed. callback url:%s, callback body:%s", callback_url, payload, e)


def copy_headers(request: Request):
    headers = {}
    for key in request.headers.keys():
        if key in header_black_list:
            continue
        headers[key] = request.headers[key]
    return headers


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=8002, log_level='info')
