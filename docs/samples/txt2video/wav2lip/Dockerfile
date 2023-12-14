FROM continuumio/miniconda3
RUN conda create --name wav2lip39 python=3.9
RUN echo "conda activate wav2lip39" >> ~/.bashrc
ENV PATH /opt/conda/envs/wav2lip39/bin:$PATH

ADD . /code
WORKDIR /code

RUN mkdir -p /root/.cache/torch/hub/checkpoints/

RUN sed -i 's/deb.debian.org/mirrors.tencent.com/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y libgl1-mesa-glx
RUN apt-get install -y libsndfile1
RUN ldconfig
RUN apt-get install -y ffmpeg

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
