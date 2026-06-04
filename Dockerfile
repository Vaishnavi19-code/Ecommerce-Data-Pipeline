# FROM apache/airflow:2.9.1
# USER airflow
# RUN pip install pyspark
FROM apache/airflow:2.9.1

USER root

# install Java (REQUIRED for PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jdk

# set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# install pyspark
RUN pip install pyspark