# main.py
@app.post("/scrape")
def start_scrapy_async(execution_id: str, name: str, request: dict):
    executor.submit(start_scrapy, execution_id, name, request)
    return {"result": "success"}


def start_scrapy(execution_id: str, name: str, request: dict):
    paragraph_limit = request["paragraph_limit"]
    subprocess.run(['scrapy', 'crawl', 'rill_flow',
                    '-a', 'execution_id=' + execution_id,
                    '-a', 'name=' + name,
                    '-a', 'paragraph_limit=' + str(paragraph_limit)])


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
        execution_id = self.execution_id
        name = self.name
        paragraph_limit = self.paragraph_limit
        paragraph = response.css('p::text').getall()
        if (len(paragraph) < int(paragraph_limit)):
            paragraph_list = paragraph
        else:
            paragraph_list = paragraph[:int(paragraph_limit)]
        self.finish_task(execution_id, name, paragraph_list)


    def finish_task(self, execution_id: str, name: str, paragraph_list: dict):
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
            "txt_array": paragraph_list
        }
        requests.post(url, data=json.dumps(data), params=params, headers=headers)