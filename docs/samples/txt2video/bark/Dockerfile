FROM continuumio/miniconda3
RUN conda create --name bark python=3.9
RUN echo "conda activate bark" >> ~/.bashrc
ENV PATH /opt/conda/envs/bark/bin:$PATH

RUN git clone https://github.com/suno-ai/bark.git
WORKDIR bark/
RUN mkdir -p bark/models && for i in 'config.json' 'generation_config.json' 'pytorch_model.bin' 'speaker_embeddings_path.json' 'special_tokens_map.json' 'tokenizer.json' 'tokenizer_config.json' 'vocab.txt' ;do wget -O bark/models/$i "https://huggingface.co/suno/bark/resolve/main/$i" ;done

COPY main.py requirements.txt ./
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
