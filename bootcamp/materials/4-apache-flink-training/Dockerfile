FROM --platform=linux/amd64 flink:1.20.0

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.11 from source
# it currently only supports Python 3.11 in PyFlink officially.
RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
tar -xvf Python-3.11.9.tgz && \
cd Python-3.11.9 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.11.9.tgz && rm -rf Python-3.11.9 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink
COPY requirements.txt .
RUN python -m pip install --upgrade pip; \
    pip3 install -r requirements.txt  --no-cache-dir;

# Image already contains Java 11. Set its HOME
RUN apt-get clean;

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk


# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-python/1.20.0/flink-python-1.20.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar;
WORKDIR /opt/flink
