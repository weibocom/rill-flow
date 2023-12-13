# main.py
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


# spiders/rill-flow.py
class RillFlowSpider(scrapy.Spider):
    name = "rill_flow"

    def start_requests(self):
        urls = [
            "https://rill-flow.github.io/en/docs/intro"
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        paragraph = response.css('p::text').getall()
        paragraph_list = paragraph[:int(self.paragraph_limit)]
        callback_body = {
            "text_array": paragraph_list
        }
        self.callback(self.callback_url, callback_body)


    def callback(self, callback_url, callback_body):
        headers = {"Content-Type": "application/json"}
        payload = json.dumps(callback_body)
        requests.post(callback_url, headers=headers, data=payload)