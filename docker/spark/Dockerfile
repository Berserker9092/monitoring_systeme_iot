FROM spark:3.5.7-java17-python3

USER root

WORKDIR /opt/spark/work-dir

COPY src/processors/spark_streaming_processor.py /opt/spark/work-dir/

ENV PYTHONUNBUFFERED=1

# spark-submit télécharge automatiquement les packages Kafka + Elasticsearch
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0", \
     "--master", "local[*]", \
     "/opt/spark/work-dir/spark_streaming_processor.py"]
