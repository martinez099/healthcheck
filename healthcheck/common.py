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


def format_result(_desc, _result, **_kwargs):
    """
    Format a check result.

    :param _desc: The check description.
    :param _result: The result of the check.
    :param _kwargs: A dict with argument name->value pairs to render.
    :return: A string with the rendered result.
    """
    if _result is True:
        result = '[+] '
    elif _result is False:
        result = '[-] '
    elif _result is Exception:
        result = '[*] '
    elif _result == '':
        return f'[ ] {_desc} skipped'
    else:
        result = '[~] '
    result += f'[{_desc}] '
    return result + f', '.join([k + ': ' + str(v) for k, v in _kwargs.items()])
