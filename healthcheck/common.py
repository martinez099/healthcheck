import math
import inspect

GB = pow(1024, 3)


def to_gb(_value):
    """
    Convert a numeric value from bytes to gigabytes.

    :param _value: A numeric value in bytes.
    :return: The numeric value in gigabytes.
    """
    return '{} GB'.format(math.floor(_value / GB))


def format_result(_result, **_kwargs):
    """
    Format a check result.

    :param _result: The result of the check.
    :param _kwargs: A dict with argument name->value pairs to render.
    :return: A string with the rendered result.
    """
    check_name = inspect.stack()[1][3]
    if _result:
        result = '[+] '
    elif _result is False:
        result = '[-] '
    else:
        result = '[~] '
    result += f'[{check_name}] '
    return result + f', '.join([k + ': ' + str(v) for k, v in _kwargs.items()])


def format_error(_check_name, _exception):
    """
    Format an error.

    :param _check_name: The name of the check.
    :param _exception: The exception occurred.
    :return: A string with the rendered result.
    """
    return f'[*] [{_check_name}] FAILED: {_exception}'
