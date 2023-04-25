FROM python:3.8-slim

ENV PYTHONUNBUFFERED 1
ENV SRC_DIR /tmp/src
ENV EXTENSION_DIR /opt/cloudforet
ENV PYTHONPATH "${PYTHONPATH}:/opt"

RUN apt-get update \
  && apt-get install -y wget build-essential

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.1 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

COPY pkg/pip_requirements.txt pip_requirements.txt
COPY templates/opt/cloudforet ${EXTENSION_DIR}

ARG CACHEBUST=1
RUN pip install -r pip_requirements.txt

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}
RUN python3 setup.py install && \
    rm -rf /tmp/*