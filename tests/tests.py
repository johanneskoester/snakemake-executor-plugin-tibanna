import tempfile
from typing import Optional
import snakemake.common.tests
from snakemake_interface_executor_plugins import ExecutorSettingsBase

from snakemake_executor_plugin_tibanna import ExecutorSettings


BUCKET_NAME = "snakemake-testing-%s-bucket" % next(tempfile._get_candidate_names())


class TestWorkflows(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "tibanna"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instatiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            sfn=...,  # TODO add test sfn
        )

    def get_assume_shared_fs(self) -> bool:
        return False

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.RemoteExecutionSettings:
        return snakemake.settings.RemoteExecutionSettings(
            seconds_between_status_checks=10,
            envvars=self.get_envvars(),
            # TODO remove once we have switched to stable snakemake for dev
            container_image="snakemake/snakemake:latest",
        )
