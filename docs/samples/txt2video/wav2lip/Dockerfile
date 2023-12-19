FROM continuumio/miniconda3
RUN conda create --name wav2lipEnv python=3.9
RUN echo "conda activate wav2lipEnv" >> ~/.bashrc
ENV PATH /opt/conda/envs/wav2lipEnv/bin:$PATH

# install dependency
RUN sed -i 's/deb.debian.org/mirrors.tencent.com/g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y libgl1-mesa-glx libsndfile1 ffmpeg

# create project
RUN git clone https://github.com/Rudrabha/Wav2Lip.git
WORKDIR Wav2Lip/
COPY main.py main.py
COPY MonaLisa.jpg MonaLisa.jpg

# download models
RUN mkdir -p /root/.cache/torch/hub/checkpoints/
RUN wget -O face_detection/detection/sfd/wav2lip_gan.pth https://huggingface.co/camenduru/Wav2Lip/resolve/main/checkpoints/wav2lip_gan.pth
RUN wget -O /root/.cache/torch/hub/checkpoints/s3fd-619a316812.pth https://huggingface.co/camenduru/Wav2Lip/resolve/main/checkpoints/s3fd-619a316812.pth

# install packages
COPY requirements.txt requirements.txt
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
