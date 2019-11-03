from healthcheck.common_funcs import green, red, yellow, magenta, get_parameter_map_name


def render_result(_result, _func):
    """
    Render result to stdout.

    :param _result: The result.
    :param _func: The check function executed.
    :return:
    """
    doc = _result[2] if len(_result) == 3 else _func.__doc__
    if _result[0] == '':
        print('[ ] {} [SKIPPED]'.format(doc))
        return
    if _result[0] is True:
        print(green('[+]'), doc, green('[SUCCEEDED]'))
        return
    elif _result[0] is False:
        to_print = [red('[-]'), doc,  red('[FAILED]')]
    elif _result[0] is None:
        to_print = [yellow('[~]'), doc, yellow('[NO RESULT]')]
    elif _result[0] is Exception:
        to_print = [magenta('[*]'), doc, magenta('[ERROR]')]
    else:
        raise NotImplementedError()

    to_print.append(', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()]))

    print('{} {} {} {}'.format(*to_print))


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats: A stats collector.
    :return:
    """
    print("\nchecks TOTAL: {}".format(sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped])))
    print(f'- {green("succeeded")}: {_stats.succeeded}')
    print(f'- {yellow("no result")}: {_stats.no_result}')
    print(f'- {red("failed")}: {_stats.failed}')
    print(f'- {magenta("errors")}: {_stats.errors}')
    print(f'- skipped: {_stats.skipped}')


def render_list(_list):
    """
    Render check list.

    :param _list: The list of check suites.
    :return:
    """
    for suite in _list:
        print(f'\n{green("Suite")}: {suite.__doc__}')
        if suite.params:
            print('{}: {}'.format(red("Parameter maps"), list(map(get_parameter_map_name, suite.params.keys()))))
        for check_name in filter(lambda x: x.startswith('check_'), dir(suite)):
            check_func = getattr(suite, check_name)
            print(f'{yellow("-")} {check_func.__doc__}')


def print_msg(_msg):
    """
    Print neutral message.

    :param _msg: The neutral message.
    :return:
    """
    print(_msg)


def print_error(_msg, _ex=None):
    """
    Print an error message.

    :param _msg: The error message.
    :param _ex: An optional exception.
    :return:
    """
    msg = [_msg]
    if _ex:
        msg.append(': ')
        if hasattr(_ex, 'reason'):
            msg.append(_ex.reason.strerror)
        else:
            msg.append(_ex.strerror)
    print(red(''.join(msg)))


def print_success(_msg):
    """
    Print a success message.

    :param _msg: The success message.
    :return:
    """
    print(green(_msg))
