FROM python:3.9.6

ADD . /code
WORKDIR /code

RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple
RUN pip install -r requirements.txt
RUN pip install transformers
RUN pip install .

CMD ["python", "main.py"]
