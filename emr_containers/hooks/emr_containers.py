from time import sleep
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EMRContainerHook(AwsBaseHook):
    """
    Interact with AWS EMR Virtual Cluster to run, poll jobs and return job status
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on EMR
    :type sleep_time: int
    :param virtual_cluster_id: Cluster ID of the EMR on EKS virtual cluster
    :type virtual_cluster_id: str
    """

    INTERMEDIATE_STATES = (
        "PENDING",
        "SUBMITTED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCEL_PENDING",
    )
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
        self, *args: Any, sleep_time: int = 30, virtual_cluster_id: str, **kwargs: Any
    ) -> None:
        super().__init__(client_type="emr-containers", *args, **kwargs)  # type: ignore
        self.sleep_time = sleep_time
        self.virtual_cluster_id = virtual_cluster_id

    def submit_job(
        self,
        name: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: Optional[dict] = None,
        client_request_token: Optional[str] = None,
    ) -> str:
        """
        Submit a job to the EMR Containers API and and return the job ID.
        A job run is a unit of work, such as a Spark jar, PySpark script,
        or SparkSQL query, that you submit to Amazon EMR on EKS.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.start_job_run

        :param name: The name of the job run.
        :type name: str
        :param execution_role_arn: The IAM role ARN associated with the job run.
        :type execution_role_arn: str
        :param release_label: The Amazon EMR release version to use for the job run.
        :type release_label: str
        :param job_driver: Job configuration details, e.g. the sparkSubmitJobDriver.
        :type job_driver: dict
        :param configuration_overrides: The configuration overrides for the job run,
            specifically either application configuration or monitoring configuration.
        :type configuration_overrides: dict
        :param client_request_token: The client idempotency token of the job run request.
            Use this if you want to specify a unique ID to prevent two jobs from getting started.
        :type client_request_token: str
        :return: Job ID
        """
        params = {
            "name": name,
            "virtualClusterId": self.virtual_cluster_id,
            "executionRoleArn": execution_role_arn,
            "releaseLabel": release_label,
            "jobDriver": job_driver,
            "configurationOverrides": configuration_overrides or {},
        }
        if client_request_token:
            params["ClientRequestToken"] = client_request_token

        response = self.get_conn().start_job_run(**params)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException("Start Job Run failed: %s" % response)
        else:
            self.log.info(
                "Start Job Run success - Job Id %s and virtual cluster id %s",
                response["id"],
                response["virtualClusterId"],
            )
            return response["id"]

    def check_query_status(self, job_id: str) -> Optional[str]:
        """
        Fetch the status of submitted job run. Returns None or one of valid query states.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.describe_job_run
        :param job_id: Id of submitted job run
        :type job_id: str
        :return: str
        """
        try:
            response = self.get_conn().describe_job_run(
                virtualClusterId=self.virtual_cluster_id,
                id=job_id,
            )
            return response["jobRun"]["state"]
        except self.get_conn().exceptions.ResourceNotFoundException:
            raise AirflowException(
                "Job ID %s not found on Virtual Cluster %s"
                % (job_id, self.virtual_cluster_id)
            )
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Exception while getting query state %s", ex)
            return None

    def poll_query_status(
        self, job_id: str, max_tries: Optional[int] = None
    ) -> Optional[str]:
        """
        Poll the status of submitted job run until query state reaches final state.
        Returns one of the final states.
        :param job_id: Id of submitted job run
        :type job_id: str
        :param max_tries: Number of times to poll for query state before function exits
        :type max_tries: int
        :return: str
        """
        try_number = 1
        final_query_state = (
            None  # Query state when query reaches final state or max_tries reached
        )

        # TODO: Make this logic a little bit more robust.
        # Currently this polls until the state is *not* one of the INTERMEDIATE_STATES
        # While that should work in most cases...it might not. :)
        while True:
            query_state = self.check_query_status(job_id)
            if query_state is None:
                self.log.info(f"Try {try_number}: Invalid query state. Retrying again")
            elif query_state in self.INTERMEDIATE_STATES:
                self.log.info(
                    f"Try {try_number}: Query is still in an intermediate state - {query_state}"
                )
            else:
                self.log.info(
                    f"Try {try_number}: Query execution completed. Final state is {query_state}"
                )
                final_query_state = query_state
                break
            if max_tries and try_number >= max_tries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(self.sleep_time)
        return final_query_state

    def stop_query(self, job_id: str) -> Dict:
        """
        Cancel the submitted job_run
        :param job_id: Id of submitted job_run
        :type job_id: str
        :return: dict
        """
        return self.get_conn().cancel_job_run(
            virtualClusterId=self.virtual_cluster_id,
            id=job_id,
        )
