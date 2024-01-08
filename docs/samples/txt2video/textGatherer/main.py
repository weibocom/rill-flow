from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request
from pydantic import BaseModel
import uvicorn
import subprocess


app = FastAPI()
executor = ThreadPoolExecutor(max_workers=20)


class Item(BaseModel):
    paragraph_limit: int = 2


@app.post("/scrape")
def start_scrapy_async(request: Request, item: Item):
    callback_url = request.headers.get("X-Callback-Url")
    executor.submit(start_scrapy, item, callback_url)
    return {"result": "success"}


def start_scrapy(item: Item, callback_url: str):
    paragraph_limit = item.paragraph_limit
    subprocess.run(['scrapy', 'crawl', 'rill_flow',
                    '-a', 'paragraph_limit=' + str(paragraph_limit),
                    '-a', 'callback_url=' + callback_url])


if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=9000)
