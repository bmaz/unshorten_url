FROM python:3.7-alpine

ENV http_proxy=http://firewall.ina.fr:81/
ENV https_proxy=http://firewall.ina.fr:81/

WORKDIR /usr/src/app
ADD ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt