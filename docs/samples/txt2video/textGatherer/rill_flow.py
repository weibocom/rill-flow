import json
import requests
import scrapy


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
        response = requests.post(callback_url, headers=headers, data=payload)
        print(response)

