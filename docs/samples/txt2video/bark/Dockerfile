FROM continuumio/miniconda3
RUN conda create --name bark python=3.9
RUN echo "conda activate bark" >> ~/.bashrc
ENV PATH /opt/conda/envs/bark/bin:$PATH

# create project
RUN git clone https://github.com/suno-ai/bark.git
WORKDIR bark/
COPY main.py main.py

# download models
RUN mkdir -p bark/models
RUN wget -O bark/models/config.json https://huggingface.co/suno/bark/resolve/main/config.json
RUN wget -O bark/models/generation_config.json https://huggingface.co/suno/bark/resolve/main/generation_config.json
RUN wget -O bark/models/pytorch_model.bin https://huggingface.co/suno/bark/resolve/main/pytorch_model.bin
RUN wget -O bark/models/speaker_embeddings_path.json https://huggingface.co/suno/bark/resolve/main/speaker_embeddings_path.json
RUN wget -O bark/models/special_tokens_map.json https://huggingface.co/suno/bark/resolve/main/special_tokens_map.json
RUN wget -O bark/models/tokenizer.json https://huggingface.co/suno/bark/resolve/main/tokenizer.json
RUN wget -O bark/models/tokenizer_config.json https://huggingface.co/suno/bark/resolve/main/tokenizer_config.json
RUN wget -O bark/models/vocab.txt https://huggingface.co/suno/bark/resolve/main/vocab.txt

# install packages
COPY requirements.txt requirements.txt
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
