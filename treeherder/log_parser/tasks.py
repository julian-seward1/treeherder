import logging

from celery import (chord,
                    task)
from django.conf import settings
from django.core.management import call_command

from treeherder.autoclassify.tasks import autoclassify
from treeherder.log_parser.utils import (expand_log_url,
                                         extract_json_log_artifacts,
                                         extract_text_log_artifacts,
                                         is_parsed,
                                         post_log_artifacts)
from treeherder.model.derived import JobsModel

logger = logging.getLogger(__name__)


def parser_task(f):
    def inner(project, job_guid, job_log_url):
        job_log_url = expand_log_url(project, job_guid, job_log_url)

        if is_parsed(job_log_url):
            return

        return f(project, job_guid, job_log_url)

    inner.__name__ = f.__name__
    inner.__doc__ = f.__doc__

    return inner


@task(name='log-parser', max_retries=10)
@parser_task
def parse_log(project, job_guid, job_log_url):
    """
    Call ArtifactBuilderCollection on the given job.
    """
    post_log_artifacts(project,
                       job_guid,
                       job_log_url,
                       parse_log,
                       extract_text_log_artifacts,
                       )


def parse_job_logs(project, job_guid, *signatures):
    if not signatures:
        return
    logger.debug("Scheduling log parsing for job %s" % job_guid)
    callback = after_logs_parsed.si(project, job_guid)
    callback.set(routing_key="after_logs_parsed")
    return chord(signatures)(callback)


@task(name='after-logs-parsed', max_retries=10)
def after_logs_parsed(project, job_guid):
    logger.info("after_logs_parsed for job %s" % job_guid)

    with JobsModel(project) as jm:
        jobs = jm.get_job_ids_by_guid([job_guid])
        print jobs
        job = jobs[job_guid]
        log_urls = jm.get_job_log_url_list([job["id"]])
        types = set(item["name"] for item in log_urls)

    signatures = []

    if "buildbot_text" in types and "errorsummary_json" in types:
        crossreference_task = crossreference_error_lines.s(project, job_guid)
        crossreference_task.set(routing_key="crossreference_error_lines")
        signatures = [crossreference_task]

        if settings.AUTOCLASSIFY_JOBS:
            classify_task = autoclassify.s(project, job_guid)
            classify_task.set(routing_key="autoclassify")
            signatures.append(classify_task)
    else:
        signatures = []

    callback = log_processing_complete.si(project, job_guid)
    callback.set(routing_key="log_processing_complete")

    return chord(signatures)(callback)


@task(name='log-parser-json', max_retries=10)
@parser_task
def parse_json_log(project, job_guid, job_log_url):
    """
    Apply the Structured Log Fault Formatter to the structured log for a job.
    """
    # The parse-json-log task has suddenly started taking 80x longer that it used to,
    # which is causing a backlog in normal log parsing tasks too. The output of this
    # task is not being used yet, so skip parsing until this is resolved.
    # See bug 1152681.
    return

    # don't parse a log if it's already been parsed
    if is_parsed(project, job_guid, job_log_url):
        return

    post_log_artifacts(project,
                       job_guid,
                       job_log_url,
                       parse_json_log,
                       extract_json_log_artifacts,
                       )
    logger.debug('parse_json_log complete for job %s' % job_guid)


@task(name='store-error-summary', max_retries=10)
@parser_task
def store_error_summary(project, job_guid, job_log_url):
    """This task is a wrapper for the store_error_summary command."""
    try:
        logger.debug('Running store_error_summary for job %s' % job_guid)
        call_command('store_error_summary', project, job_guid, job_log_url['url'])
    except Exception, e:
        print e
        store_error_summary.retry(exc=e, countdown=(1 + store_error_summary.request.retries) * 60)
    logger.debug('Completed store_error_summary for job %s' % job_guid)


@task(name='crossreference-error-lines', max_retries=10)
def crossreference_error_lines(project, job_guid):
    """This task is a wrapper for the store_error_summary command."""
    logger.info("Running crossreference-error-lines for %s" % job_guid)
    try:
        call_command('crossreference_error_lines', project, job_guid)
    except Exception, e:
        print e
        crossreference_error_lines.retry(exc=e, countdown=(1 + crossreference_error_lines.request.retries) * 60)
    logger.info("Completed crossreference-error-lines for %s" % job_guid)


@task(name='log_processing_complete', max_retries=10)
def log_processing_complete(project, job_guid):
    logger.info("Log processing complete for %s (TODO: store this)" % job_guid)
