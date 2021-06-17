from typing import Any, Dict, Optional
from uuid import uuid4

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.models import BaseOperator
from emr_containers.hooks.emr_containers import EMRContainerHook
from airflow.utils.decorators import apply_defaults


class EMRContainerOperator(BaseOperator):
    """
    An operator that submits jobs to EMR on EKS virtual clusters.
    :param name: The name of the job run.
    :type name: str
    :param virtual_cluster_id: The EMR on EKS virtual cluster ID
    :type virtual_cluster_id: str
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
        If no token is provided, a UUIDv4 token will be generated for you.
    :type client_request_token: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :type aws_conn_id: str
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on EMR
    :type sleep_time: int
    :param max_tries: Maximum number of times to wait for the job run to finish.
        Defaults to None, which will poll until the job is *not* in a pending, submitted, or running state.
    :type max_tries: int
    """

    template_fields = ["name", "virtual_cluster_id",
                       "execution_role_arn", "release_label", "job_driver"]
    ui_color = "#f9c915"

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        name: str,
        virtual_cluster_id: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: Optional[dict] = None,
        client_request_token: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        sleep_time: int = 30,
        max_tries: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.virtual_cluster_id = virtual_cluster_id
        self.execution_role_arn = execution_role_arn
        self.release_label = release_label
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides or {}
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.sleep_time = sleep_time
        self.max_tries = max_tries
        self.job_id = None

    @cached_property
    def hook(self) -> EMRContainerHook:
        """Create and return an EMRContainerHook."""
        return EMRContainerHook(
            self.aws_conn_id,
            sleep_time=self.sleep_time,
            virtual_cluster_id=self.virtual_cluster_id,
        )

    def execute(self, context: dict) -> Optional[str]:
        """Run job on EMR Containers"""

        self.job_id = self.hook.submit_job(
            self.name,
            self.execution_role_arn,
            self.release_label,
            self.job_driver,
            self.configuration_overrides,
        )
        query_status = self.hook.poll_query_status(self.job_id, self.max_tries)

        if query_status in EMRContainerHook.FAILURE_STATES:
            # self.hook.get_state_change_reason(self.query_execution_id)
            error_message = "BEEP BOOP"
            raise Exception(
                "Final state of EMR Containers job is {}, query_execution_id is {}. Error: {}".format(
                    query_status, self.job_id, error_message
                )
            )
        elif not query_status or query_status in EMRContainerHook.INTERMEDIATE_STATES:
            raise Exception(
                "Final state of EMR Containers job is {}. "
                "Max tries of poll status exceeded, query_execution_id is {}.".format(
                    query_status, self.job_id
                )
            )

        return self.job_id

    def on_kill(self) -> None:
        """Cancel the submitted job run"""
        if self.job_id:
            self.log.info("Stopping job run with jobId - %s", self.job_id)
            response = self.hook.stop_query(self.job_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error("Exception while cancelling query: %s", ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error(
                        "Unable to request query cancel on EMR. Exiting")
                else:
                    self.log.info(
                        "Polling EMR for query with id %s to reach final state",
                        self.job_id,
                    )
                    self.hook.poll_query_status(self.job_id)
