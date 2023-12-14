FROM continuumio/miniconda3
RUN conda create --name moviepy310 python=3.10
RUN echo "conda activate moviepy310" >> ~/.bashrc
ENV PATH /opt/conda/envs/moviepy310/bin:$PATH

ADD . /code
WORKDIR /code

RUN sed -i 's/deb.debian.org/mirrors.tencent.com/g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y imagemagick && apt-get install -y vim
COPY policy.xml /etc/ImageMagick-6/policy.xml

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
