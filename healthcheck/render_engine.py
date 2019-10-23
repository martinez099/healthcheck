from healthcheck.common_funcs import green, red, yellow, magenta, get_parameter_map_name


def render_result(_result, _func):
    """
    Render result to stdout.

    :param _result:
    :param _func:
    :return:
    """
    if not _result[1]:
        print('[ ] {} [SKIPPED]'.format(_func.__doc__))
        return
    if _result[0] is True:
        to_print = [green('[+]'), _func.__doc__, green('[SUCCEEDED]')]
    elif _result[0] is False:
        to_print = [red('[-]'), _func.__doc__,  red('[FAILED]')]
    elif _result[0] is None:
        to_print = [yellow('[~]'), _func.__doc__, yellow('[NO RESULT]')]
    elif _result[0] is Exception:
        to_print = [magenta('[*]'), _func.__doc__, magenta('[ERROR]')]
    else:
        raise NotImplementedError()

    to_print.append(', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()]))

    print('{} {} {} {}'.format(*to_print))


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats:
    :return:
    """
    print("\nTOTAL: {}".format(sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped])))
    print(f'- {green("succeeded")}: {_stats.succeeded}')
    print(f'- {yellow("no result")}: {_stats.no_result}')
    print(f'- {red("failed")}: {_stats.failed}')
    print(f'- {magenta("errors")}: {_stats.errors}')
    print(f'- skipped: {_stats.skipped}')


def render_list(_list):
    """
    Render check list.

    :param _list:
    :return:
    """
    for suite in _list:
        print(f'\n{suite.__doc__}')
        if suite.params:
            print('Parameter maps: {}'.format(list(map(get_parameter_map_name, suite.params.keys()))))
        check_names = filter(lambda x: x.startswith('check_'), dir(suite))
        for check_name in check_names:
            check_func = getattr(suite, check_name)
            print(f'- {check_func.__doc__}')


def print_msg(_msg):
    """
    Print neutral message.

    :param _msg: The neutral message.
    :return:
    """
    print(_msg)


def print_error(_msg):
    """
    Print an error message.

    :param _msg: The error message.
    :return:
    """
    print(red(_msg))


def print_success(_msg):
    """
    Print a success message.

    :param _msg: The success message.
    :return:
    """
    print(green(_msg))
