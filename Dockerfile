FROM python:3.8.10 AS dependencies

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app"

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

RUN export JAVA_HOME

RUN curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | gzip -d > cs && \
    chmod +x cs && \
    ./cs install scala:2.12.15

RUN wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.2.1-bin-hadoop3.2.tgz && \
    mv spark-3.2.1-bin-hadoop3.2 /usr/local/spark

ENV SPARK_HOME /usr/local/spark

RUN export SPARK_HOME

ENV PATH="/usr/local/spark/bin:${PATH}"

COPY . .

CMD ["tail", "-f", "/dev/null"]