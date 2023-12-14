FROM python:3.10
ADD .. /code
WORKDIR /code

RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple
RUN pip install -r requirements.txt

CMD ["python", "main.py"]