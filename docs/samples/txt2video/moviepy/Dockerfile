FROM continuumio/miniconda3
RUN conda create --name moviepy python=3.10
RUN echo "conda activate moviepy" >> ~/.bashrc
ENV PATH /opt/conda/envs/moviepy/bin:$PATH

# install dependency
RUN sed -i 's/deb.debian.org/mirrors.tencent.com/g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y imagemagick vim

# create project
WORKDIR /media-processor
COPY main.py main.py
COPY subtitles.srt subtitles.srt
COPY requirements.txt requirements.txt

# install packages
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r requirements.txt

# modify ImageMagick policy file so that Textclips work correctly.
RUN sed -i 's/none/read,write/g' /etc/ImageMagick-6/policy.xml

CMD ["python", "main.py"]
