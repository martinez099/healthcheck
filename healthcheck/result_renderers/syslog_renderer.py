import datetime
import os
import re
import socket


def render_result(_result, _func, *_args, **_kwargs):
    """
    Render result, tries to comply with https://tools.ietf.org/html/rfc5424.

    :param _result: The result.
    :param _func: The check function executed.
    """
    pri = 8 * 1  # facility = 1, i.e. user-level messages
    ver = '1'
    ts = datetime.datetime.now().isoformat()
    host = socket.gethostname()
    app = 'healthcheck'
    proc_id = os.getpid()
    msg_id = '-'
    sd = '-'

    remedy = None
    if _result[0] == '':
        status = 'SKIPPED'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is True:
        status = 'SUCCEEDED'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is False:
        status = 'FAILED'
        pri += 3  # severity = 3, i.e. Error
        doc = (_result[2] if len(_result) == 3 else _func.__doc__)
        remedy = re.findall(r'Remedy: (.*)', doc, re.MULTILINE)
    elif _result[0] is None:
        status = 'NO RESULT'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is Exception:
        status = 'ERROR'
        pri += 3  # severity = 3, i.e. Error
    else:
        raise NotImplementedError()

    msg = (_result[2] if len(_result) == 3 else _func.__doc__).split('\n')[0] + f' [{status}] ' + str(_result[1])
    if remedy:
        msg += f' Remedy: {remedy[0]}'
    print('<{}>{} {} {} {} {} {} {} {}'.format(pri, ver, ts, host, app, proc_id, msg_id, sd, msg))


def render_stats(_stats):
    """
    Render collected statistics, tries to comply with https://tools.ietf.org/html/rfc5424.

    :param _stats: A stats collector.
    """
    pri = 8 * 1  # facility = 1, i.e. user-level messages
    pri += 6  # severity = 6, i.e. Informational
    ver = '1'
    ts = datetime.datetime.now().isoformat()
    host = socket.gethostname()
    app = 'healthcheck'
    proc_id = os.getpid()
    msg_id = '-'
    sd = '-'
    msg = {
        'total checks run': sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped]),
        'succeeded': _stats.succeeded,
        'no result': _stats.no_result,
        'failed': _stats.failed,
        'errors': _stats.errors,
        'skipped': _stats.skipped
    }

    print('<{}>{} {} {} {} {} {} {} {}'.format(pri, ver, ts, host, app, proc_id, msg_id, sd, msg))
