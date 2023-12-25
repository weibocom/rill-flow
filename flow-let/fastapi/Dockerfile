FROM python:3

ENV TZ=PRC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir -p /data1/executor/src

WORKDIR /data1/executor/src
COPY main.py /data1/executor/src/
COPY requirements.txt /data1/executor/src/

RUN mkdir -p /data1/executor/logs && pip install -i https://mirrors.aliyun.com/pypi/simple/ -r  /data1/executor/src/requirements.txt

ENV PYTHONPATH='/data1/executor/src/:$PYTHONPATH'

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]