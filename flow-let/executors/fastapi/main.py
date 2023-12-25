# coding:utf-8
from fastapi import FastAPI, Request
import uvicorn
import logging

file_directory = "/data1/executor/src/"

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

app = FastAPI()


@app.post('/execut.json')
async def executor_api(request: Request):
    body_raw = {}
    try:
        body_raw = await request.json()
        logging.info("header:%s, request body:%s", request.headers.items(), body_raw)
        body_raw["executor_tag"] = "executor"
        return body_raw
    except Exception as e:
        logging.error("executor processing failed. request body:%s", body_raw, e)
        return {"result_type": "FAILED", "error_msg": "Executor processing failed"}


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=8000)
