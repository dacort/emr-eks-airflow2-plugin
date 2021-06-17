# Stage 0 - Build an emr_containers pip-installable package
FROM python:3.8-slim-buster
WORKDIR /app
COPY emr_containers /app/emr_containers/
COPY setup.py /app/

# This should make emr_containers-0.0.0-py3-none-any.whl
RUN python setup.py bdist_wheel

# -----------------------------------------
# Stage 1 - Install emr_containers onto Airflow
FROM apache/airflow:2.0.1-python3.8

COPY --from=0 /app/dist/emr_containers-0.0.0-py3-none-any.whl /
RUN pip install /emr_containers-0.0.0-py3-none-any.whl
