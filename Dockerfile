FROM python:3.8-slim

ENV PYTHONUNBUFFERED 1
ENV SRC_DIR /tmp/src
ENV EXTENSION_DIR /opt/spaceone
ENV PYTHONPATH "${PYTHONPATH}:/opt"

RUN apt-get update \
  && apt-get install -y wget build-essential

COPY pkg/pip_requirements.txt pip_requirements.txt
COPY templates/opt/cloudforet ${EXTENSION_DIR}

RUN pip install -r pip_requirements.txt

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}
RUN python3 setup.py install && \
    rm -rf /tmp/*
