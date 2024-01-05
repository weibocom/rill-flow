FROM continuumio/miniconda3
RUN conda create --name moviepy python=3.10
RUN echo "conda activate moviepy" >> ~/.bashrc
ENV PATH /opt/conda/envs/moviepy/bin:$PATH

RUN apt-get update && apt-get install -y imagemagick
RUN sed -i 's/none/read,write/g' /etc/ImageMagick-6/policy.xml

WORKDIR /media-processor
COPY main.py main.py requirements.txt ./
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
