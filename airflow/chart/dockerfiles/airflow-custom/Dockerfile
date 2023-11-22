# Use the official Apache Airflow image as the base
FROM apache/airflow:2.7.3

# Install the additional Python packages
RUN pip install \
    apache-airflow-providers-amazon \
    scikit-learn \
    pandas \
    pyspark \
    polars \
    apache-airflow[amazon]