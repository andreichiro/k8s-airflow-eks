# Use the official Apache Airflow image as the base
FROM apache/airflow:2.7.3


# Install the additional Python packages
RUN pip install \
    apache-airflow-providers-amazon \
    scikit-learn \
    pandas \
    pyspark \
    polars \
    apache-airflow[amazon] \
    apache-airflow-providers-apache-spark

USER root

RUN apt-get update && apt-get install -y procps
RUN sudo apt-get update
### not working!!! RUN sudo apt-get install openjdk-17-jdk

USER airflow
