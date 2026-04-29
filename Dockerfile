# syntax=docker/dockerfile:1.7

# Pinned base image (Debian 12 / bookworm) with Python preinstalled.
# We force linux/amd64 because upstream sldl only ships a 64-bit Linux x86 binary;
# on arm64 hosts (Apple Silicon, RPi5...) Docker will run it through QEMU.
FROM --platform=linux/amd64 python:3.13.1-slim-bookworm

ARG SLDL_VERSION=2.6.0
ARG SLDL_URL=https://github.com/fiso64/sldl/releases/download/v${SLDL_VERSION}/sldl_linux-x64.zip

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TERM=xterm-256color \
    FORCE_COLOR=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
        libicu72 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "${SLDL_URL}" -o /tmp/sldl.zip && \
    unzip -j /tmp/sldl.zip 'sldl' -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/sldl && \
    rm /tmp/sldl.zip && \
    sldl --help >/dev/null

RUN pip install --no-cache-dir 'rich==13.9.4'

RUN mkdir -p /data /downloads

COPY entrypoint.py /usr/local/bin/gargantua
RUN chmod +x /usr/local/bin/gargantua

WORKDIR /downloads
VOLUME ["/data", "/downloads"]

ENTRYPOINT ["gargantua"]
