from healthcheck.common import green, red, yellow, magenta


def render_result(_result, _func):
    """
    Render result to stdout.

    :param _result:
    :param _func:
    :return:
    """
    if not _result[1]:
        print('[ ] SKIPPED [{}]'.format(_func.__doc__))
        return
    if _result[0] is True:
        to_print = [green('[+] SUCCESS  ')]
    elif _result[0] is False:
        to_print = [red('[-] FAILED   ')]
    elif _result[0] is None:
        to_print = [yellow('[~] NO RESULT')]
    elif _result[0] is Exception:
        to_print = [magenta('[*] ERROR    ')]
    else:
        raise NotImplementedError()

    to_print.append(_func.__doc__)
    to_print.append(', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()]))

    print('{} [{}] {}'.format(*to_print))


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats:
    :return:
    """

    print("\n[STATISTICS] " + str(_stats.get()))


def render_list(_list):
    """
    Render check list.

    :param _list:
    :return:
    """
    result = []
    for suite in _list:
        print(f'\n{suite.__doc__}')
        check_names = filter(lambda x: x.startswith('check_'), dir(suite))
        checks = []
        for check_name in check_names:
            check_func = getattr(suite, check_name)
            print(f'- {check_func.__doc__}')
        result.append(checks)
