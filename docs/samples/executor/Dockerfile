FROM python:3

ENV TZ=PRC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir -p /data1/executor/src

# 启动目录
WORKDIR /data1/executor/src

ENV PYTHONPATH='/data1/executor/src/:$PYTHONPATH'
COPY main.py /data1/executor/src/
COPY requirements.txt /data1/executor/src/

RUN mkdir -p /data1/executor/logs && pip install -i https://mirrors.aliyun.com/pypi/simple/ -r  /data1/executor/src/requirements.txt

# 启动应用
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]