FROM ubuntu:14.04

MAINTAINER Yongtao You <yongtao.you@cloudwiz.cn>

# Install necessary packages
RUN apt-get clean && apt-get update -y && apt-get install -y \
    gcc \
    git \
    make \
    openssh-client \
    wget \
    zlib1g-dev

# Clone tcollector repo from github.com
RUN mkdir -p /root/src
WORKDIR /root/src
RUN git clone https://github.com/ytyou/agent.git
