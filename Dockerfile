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

RUN conda config --add channels conda-forge && \
    conda config --set channel_priority strict && \
    conda update --all --override-channels -c conda-forge --yes && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r && \
    conda install mamba --yes

RUN cd / && git clone  https://$TOKEN@github.com/cubecloud/dbbinance-storage.git && \
    cd /dbbinance-storage && \
    git checkout main

WORKDIR /dbbinance-storage

# Create conda env with Python 3.9 cos we have 3.10 by default in ubuntu 22.04
RUN conda env create -y -f ${env_file} && \
    conda clean --all && \
    echo "source activate $conda_env" > ~/.bashrc

# Variables for env
ENV CONDA_DEFAULT_ENV=${conda_env}
ENV PATH="/opt/conda/envs/${conda_env}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

# Start updater
CMD ["bash", "-c", "./start.sh"]
