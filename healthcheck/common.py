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
