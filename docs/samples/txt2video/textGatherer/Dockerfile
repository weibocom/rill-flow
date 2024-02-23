FROM continuumio/miniconda3
RUN conda create --name scrapy python=3.10
RUN echo "conda activate scrapy" >> ~/.bashrc
ENV PATH /opt/conda/envs/scrapy/bin:$PATH

RUN pip install scrapy
RUN scrapy startproject TextGatherer
WORKDIR /TextGatherer
COPY rill_flow.py TextGatherer/spiders/rill_flow.py
COPY main.py main.py

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
