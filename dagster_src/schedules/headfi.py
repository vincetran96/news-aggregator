from dagster import ScheduleDefinition

from dagster_src.jobs.headfi import headfi_pipeline_job

headfi_pipeline__daily = ScheduleDefinition(
    job=headfi_pipeline_job,
    cron_schedule="0 6 * * *",
)
