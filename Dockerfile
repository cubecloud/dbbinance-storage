FROM ubuntu:22.04

# ARGs for env name
ARG conda_env=dbupdater
ARG env_file=${conda_env}.yml

LABEL main=dbupdater

RUN apt-get update && apt-get install -y \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /miniconda.sh && \
    chmod +x /miniconda.sh && \
    /miniconda.sh -b -p /opt/conda && \
    rm /miniconda.sh

ENV PATH="/opt/conda/bin:${PATH}"

# File with env name.yml
COPY ${env_file} /
COPY start.py /

# Create conda env with Python 3.9 cos we have 3.10 by default in ubuntu 22.04
RUN conda env create -y -f /${env_file} && \
    conda clean --all && \
    echo "source activate $conda_env" > ~/.bashrc

# Variables for env
ENV CONDA_DEFAULT_ENV=${conda_env}
ENV PATH="/opt/conda/envs/${conda_env}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

# Start updater
CMD ["python", "start.py"]
