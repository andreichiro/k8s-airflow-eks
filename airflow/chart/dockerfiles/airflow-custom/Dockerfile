#Use the official Apache Airflow image as the base
FROM apache/airflow:2.7.3

#Install the additional Python packages
RUN pip install \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake[common.sql] \
    apache-airflow-providers-dbt-cloud[http] \
    apache-airflow-providers-apache-spark \
    scikit-learn \
    pandas \
    polars \
    apache-airflow[amazon] \
    pyiceberg[s3fs,hive,glue,duckdb] \
    snowflake-connector-python \
    dbt-cloud-cli

USER root

RUN apt-get update && apt-get install -y procps
RUN sudo apt-get update

USER airflow