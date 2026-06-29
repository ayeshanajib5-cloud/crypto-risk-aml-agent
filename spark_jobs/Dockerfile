FROM apache/spark:3.5.1-python3

USER root

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Download Kafka connector JARs for Spark
RUN mkdir -p /opt/spark/jars &&     curl -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar     https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar &&     curl -o /opt/spark/jars/kafka-clients-3.4.1.jar     https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar &&     curl -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar     https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar &&     curl -o /opt/spark/jars/commons-pool2-2.11.1.jar     https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN mkdir -p /app
COPY vwap_zscore.py /app/vwap_zscore.py
