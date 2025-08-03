FROM python:3

RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY jars/mysql-connector-j-8.3.0.jar /opt/spark/jars/
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

