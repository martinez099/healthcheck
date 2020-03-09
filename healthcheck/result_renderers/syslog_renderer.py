import datetime
import os
import socket


def render_result(_result, _func):
    """
    Render result.

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
    sd = _result[1]

    if _result[0] == '':
        status = 'SKIPPED'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is True:
        status = 'SUCCEEDED'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is False:
        status = 'FAILED'
        pri += 3  # severity = 3, i.e. Error
    elif _result[0] is None:
        status = 'NO RESULT'
        pri += 6  # severity = 6, i.e. Informational
    elif _result[0] is Exception:
        status = 'ERROR'
        pri += 3  # severity = 3, i.e. Error
    else:
        raise NotImplementedError()

    sd['status'] = status
    msg = _result[2] if len(_result) == 3 else _func.__doc__

    print('<{}>{} {} {} {} {} {} {} {}'.format(pri, ver, ts, host, app, proc_id, msg_id, sd, msg))


def render_stats(_stats):
    """
    Render collected statistics.

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
    sd = {
        'total checks run': sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped]),
        'succeeded': _stats.succeeded,
        'no result': _stats.no_result,
        'failed': _stats.failed,
        'errors': _stats.errors,
        'skipped': _stats.skipped
    }
    msg = '-'

    print('<{}>{} {} {} {} {} {} {} {}'.format(pri, ver, ts, host, app, proc_id, msg_id, sd, msg))
