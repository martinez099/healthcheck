import json


def render_result(_result, _func):
    """
    Render result.

    :param _result: The result.
    :param _func: The check function executed.
    """
    to_print = {
        'desc': _result[2] if len(_result) == 3 else _func.__doc__
    }
    if _result[0] == '':
        to_print['status'] = 'SKIPPED'
    elif _result[0] is True:
        to_print['status'] = 'SUCCEEDED'
    elif _result[0] is False:
        to_print['status'] = 'FAILED'
    elif _result[0] is None:
        to_print['status'] = 'NO RESULT'
    elif _result[0] is Exception:
        to_print['status'] = 'ERROR'
    else:
        raise NotImplementedError()

    to_print['info'] = _result[1]
    print(json.dumps(to_print))


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats: A stats collector.
    """
    to_print = {
        'total checks run': sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped]),
        'succeeded': _stats.succeeded,
        'no result': _stats.no_result,
        'failed': _stats.failed,
        'errors': _stats.errors,
        'skipped': _stats.skipped
    }
    print(json.dumps(to_print))
