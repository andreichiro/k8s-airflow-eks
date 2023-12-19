#Use the official Apache Airflow image as the base
FROM apache/airflow:2.8.0

USER root

RUN apt-get update && apt-get install -y procps
RUN sudo apt-get update
RUN apt-get install -y libmariadb-dev

USER airflow
#Install the additional Python packages
RUN pip install --upgrade pip \
    apache-airflow[amazon] \    
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake[common.sql] \
    apache-airflow-providers-dbt-cloud[http] \
    apache-airflow-providers-apache-spark \
    scikit-learn \
    pandas \
    polars \
    pyiceberg[s3fs,hive,glue,duckdb,pyarrow] \
    snowflake-connector-python \
    duckdb \
    apache-airflow[snowflake] \    
    gspread \
    apache-airflow[google]
