# EMR on EKS Airflow v2 Plugin

_NOTICE_ This is an experimental plugin that is under active development until it can be merged with [airflow proper](https://github.com/apache/airflow).

It's only been used with self-managed Airflow 2.0 I originally deployed this using a custom Airflow build (see [Run EMR on EKS jobs on Apache Airflow](https://www.youtube.com/watch?v=lTGguM1_1z0?t=300s)).

## Requirements

- Python >= 3.8

## Installing

Airflow 2.0 [no longer supports](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) importing plugins via `airflow.{operators,sensors,hooks}.<plugin_name`, so extensions need to be imported as regular Python modules.

As I'm intending to get this merged into the AWS providers package in Airflow and not a pip-installable package, there are a couple ways of doing this:

1. Run `python setup.py` and `pip install` the resulting wheel file on your Airflow installation

        python setup.py bdist_wheel

2. If you're running a containerized version of Airflow, the `Dockerfile` builds a custom container based off Airflow 2.1.0

        docker build -t airflow2-emr-eks .
        docker tag airflow2-emr-eks ghcr.io/OWNER/airflow-emr-eks:2.1.0
        docker push ghcr.io/OWNER/airflow-emr-eks:2.1.0

Airflow 2.0 on Amazon WMAA is not yet supported.

## Usage

See [`airflow2_emr_eks.py`](https://github.com/dacort/airflow-example-dags/blob/main/dags/airflow2_emr_eks.py) for an example Airflow 2.0 DAG.

That example requires a new Connection in Airflow with the connection ID of `emr_eks` and the following "extra" config:

```json
{
    "virtual_cluster_id": "abcdefghijklmno0123456789",
    "job_role_arn": "arn:aws:iam::123456789012:role/emr_eks_default_role"
}
```