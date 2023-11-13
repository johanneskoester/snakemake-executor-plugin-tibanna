__author__ = "Soohyun Lee, Johannes Köster"
__copyright__ = "Copyright 2023, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from dataclasses import dataclass, field
import math
import os
import re
from typing import AsyncGenerator, List, Generator, Optional
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins import ExecutorSettingsBase, CommonSettings
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError  # noqa

from tibanna.core import API
from tibanna import ec2_utils, core as tibanna_core


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    sfn: Optional[str] = field(
        default=None,
        metadata={
            "help": "Name of Tibanna Unicorn step function (e.g. "
            "tibanna_unicorn_monty). "
            "This works as serverless scheduler/resource allocator and must be "
            "deployed first using tibanna cli. (e.g. tibanna deploy_unicorn "
            "--usergroup=monty --buckets=bucketname)",
            # Optionally specify that setting is required when the executor is in use.
            "required": True,
        },
    )
    spot_instance: bool = field(
        default=False,
        metadata={
            "help": "Use spot instance for AWS cloud execution. ",
            "category": "tibanna_aux_config",
        },
    )
    subnet: Optional[str] = field(
        default=None,
        metadata={
            "help": "Subnet ID for AWS cloud execution. ",
            "category": "tibanna_aux_config",
        },
    )
    security_group: Optional[str] = field(
        default=None,
        metadata={
            "help": "Security group ID for AWS cloud execution. ",
            "category": "tibanna_aux_config",
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.snakefile = self.workflow.main_snakefile
        self.envvars = {e: os.environ[e] for e in self.workflow.envvars}
        if self.envvars:
            self.logger.debug("envvars = %s" % str(self.envvars))
        self.tibanna_sfn = self.workflow.executor_settings.sfn

        self.precommand = self.workflow.executor_settings.precommand or ""

        self.quiet = self.workflow.output_settings.quiet

        self.container_image = self.workflow.remote_execution_settings.container_image

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        # If required, make sure to pass the job's id to the job_info object, as keyword
        # argument 'external_job_id'.

        # submit job here, and obtain job ids from the backend
        tibanna_input = self.make_tibanna_input(job)
        jobid = tibanna_input["jobid"]
        exec_info = API().run_workflow(
            tibanna_input,
            sfn=self.tibanna_sfn,
            verbose=not self.quiet,
            jobid=jobid,
            open_browser=False,
            sleep=0,
        )
        exec_arn = exec_info.get("_tibanna", {}).get("exec_arn", "")
        jobname = tibanna_input["config"]["run_name"]
        jobid = tibanna_input["jobid"]

        job_info = SubmittedJobInfo(
            job=job,
            external_jobid=jobid,
            aux={"exec_arn": exec_arn, "jobname": jobname},
        )
        self.report_job_submission(job_info)

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # If you provided it above, each will have its external_jobid set according
        # to the information you provided at submission time.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(active_job).
        # For jobs that have errored, you have to call
        # self.report_job_error(active_job).
        # This will also take care of providing a proper error message.
        # Usually there is no need to perform additional logging here.
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        #
        # To modify the time until the next call of this method,
        # you can set self.next_sleep_seconds here.
        for j in active_jobs:
            # use self.status_rate_limiter to avoid too many API calls.
            async with self.status_rate_limiter:
                if j.exec_arn:
                    status = API().check_status(j.exec_arn)
                else:
                    status = "FAILED_AT_SUBMISSION"
            if not self.quiet or status != "RUNNING":
                self.logger.debug(f"job {j.jobname}: {status}")
            if status == "RUNNING":
                yield j
            elif status == "SUCCEEDED":
                self.report_job_success(j)
            else:
                self.report_job_error(j)

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.

        for j in active_jobs:
            self.logger.info(f"killing job {j.jobname}")
            while True:
                try:
                    res = API().kill(j.exec_arn)
                    if not self.quiet:
                        print(res)
                    break
                except KeyboardInterrupt:
                    pass

    def split_filename(self, filename, checkdir=None):
        f = os.path.abspath(filename)
        if checkdir:
            checkdir = checkdir.rstrip("/")
            if f.startswith(checkdir):
                fname = re.sub(f"^{checkdir}/", "", f)
                fdir = checkdir
            else:
                direrrmsg = (
                    "All source files including Snakefile, "
                    + "conda env files, and rule script files "
                    + "must be in the same working directory: {} vs {}"
                )
                raise WorkflowError(direrrmsg.format(checkdir, f))
        else:
            fdir, fname = os.path.split(f)
        return fname, fdir

    def remove_prefix(self, s):
        return re.sub(f"^{self.s3_bucket}/{self.s3_subdir}/", "", s)

    def get_snakefile(self):
        return os.path.basename(self.snakefile)

    def add_command(self, job: JobExecutorInterface, tibanna_args, tibanna_config):
        # format command
        command = self.format_job_exec(job)

        if self.precommand:
            command = self.precommand + "; " + command
        self.logger.debug("command = " + str(command))
        tibanna_args.command = command

    def adjust_filepath(self, f):
        if not hasattr(f, "remote_object"):
            rel = self.remove_prefix(f)  # log/benchmark
        elif (
            hasattr(f.remote_object, "provider") and f.remote_object.provider.is_default
        ):
            rel = self.remove_prefix(f)
        else:
            rel = f
        return rel

    def make_tibanna_input(self, job: JobExecutorInterface):
        # mem & cpu
        mem = job.resources["mem_mb"] / 1024 if "mem_mb" in job.resources.keys() else 1
        cpu = job.threads

        # jobid, grouping, run_name
        jobid = tibanna_core.create_jobid()
        if job.is_group():
            run_name = f"snakemake-job-{str(jobid)}-group-{str(job.groupid)}"
        else:
            run_name = f"snakemake-job-{str(jobid)}-rule-{str(job.rule)}"

        # tibanna input
        tibanna_config = {
            "run_name": run_name,
            "mem": mem,
            "cpu": cpu,
            "ebs_size": math.ceil(job.resources["disk_mb"] / 1024),
            "log_bucket": self.s3_bucket,
        }

        tibanna_config.update(
            self.workflow.executor_settings.get_items_by_category("tibanna_aux_config")
        )

        tibanna_args = ec2_utils.Args(
            language="snakemake",
            container_image=self.container_image,
            input_env=self.envvars,
        )
        self.add_command(job, tibanna_args, tibanna_config)
        tibanna_input = {
            "jobid": jobid,
            "config": tibanna_config,
            "args": tibanna_args.as_dict(),
        }
        return tibanna_input
