# EMR on EKS Airflow v2 Plugin

💁 This plugin is now officially incoporated into official Airflow Amazon Provider as of August 2021.

You can find instructions on using it on the [Amazon EMR on EKS Operators documentation](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr_eks.html) page.

**!!** Please note that **Airflow 2.1.x** or greater is required. If you want to use EMR on EKS on Amazon MWAA, you can follow the [instructions below](#installing-on-mwaa).

It's been tested with self-managed Airflow 2.0 (see [Run EMR on EKS jobs on Apache Airflow](https://www.youtube.com/watch?v=lTGguM1_1z0?t=300s)) and Airflow 2.0 on MWAA.

## Requirements

- Python >= 3.8

## Installing on MWAA

If you're running Airflow 2.0 on Amazon WMAA, create a zip file with the plugin and an entrypoint. Then [follow the instructions](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-import-plugins.html#configuring-dag-plugins-upload).

        zip -j plugins.zip mwaa/mwaa_plugin.py
        cd emr_containers
        zip -r ../plugins.zip .
        
Alternatively, you can create a `requirements.txt` file that points to this repository and contains the following line:

    emr-containers @ https://github.com/dacort/emr-eks-airflow2-plugin/archive/main.zip
    apache-airflow[amazon]==2.0.2

## Usage

See [`airflow2_emr_eks.py`](https://github.com/dacort/airflow-example-dags/blob/main/dags/airflow2_emr_eks.py) for an example Airflow 2.0 DAG.

That example requires a new Connection in Airflow with the connection ID of `emr_eks` and the following "extra" config:

```json
{
    "virtual_cluster_id": "abcdefghijklmno0123456789",
    "job_role_arn": "arn:aws:iam::123456789012:role/emr_eks_default_role"
}
```

## (Deprecated) Installing

Airflow 2.0 [no longer supports](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) importing plugins via `airflow.{operators,sensors,hooks}.<plugin_name`, so extensions need to be imported as regular Python modules.

As I'm intending to get this merged into the AWS providers package in Airflow and not a pip-installable package, there are a couple ways of doing this:

1. Run `python setup.py` and `pip install` the resulting wheel file on your Airflow installation

        python setup.py bdist_wheel

2. If you're running a containerized version of Airflow, the `Dockerfile` builds a custom container based off Airflow 2.1.0

        docker build -t airflow2-emr-eks .
        docker tag airflow2-emr-eks ghcr.io/OWNER/airflow-emr-eks:2.1.0
        docker push ghcr.io/OWNER/airflow-emr-eks:2.1.0
