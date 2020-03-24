preface = False


def render_result(_result, _func):
    """
    Render result.

    :param _result: The result.
    :param _func: The check function executed.
    """
    global preface
    if not preface:
        print('''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<html><head><title>RE health check result</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<style>
table th { text-align: left }
table tr td { border-bottom: 1px solid #ddd }
</style>
</head><body style="font-family:monospace">
<table style="width:100%">
<tr><th>Code: Description</th><th>Result</th><th>Info</th></tr>''')
        preface = True

    doc = (_result[2] if len(_result) == 3 else _func.__doc__).split('\n')[0]
    print(f'<tr><td>{doc}</td>')
    if _result[0] == '':
        print('<td>SKIPPED</td>')
    elif _result[0] is True:
        print('<td style="background-color:green">SUCCEDED</td>')
    elif _result[0] is False:
        print('<td style="background-color:red">FAILED</td>')
    elif _result[0] is None:
        print('<td style="background-color:yellow">NO RESULT</td>')
    elif _result[0] is Exception:
        print('<td style="background-color:magenta">ERROR</td>')
    else:
        raise NotImplementedError()

    print('<td>')
    print(', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()]))
    print('</td></tr>')


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats: A stats collector.
    """
    print('</table>')
    print('<table style="width:170px"><tr style="height:20px"><th></th></tr>')
    print('<tr><td>')
    print("total checks run: {}".format(
        sum([_stats.succeeded, _stats.no_result, _stats.failed, _stats.errors, _stats.skipped])))
    print('</td></tr><tr><td style="background-color:green;text-align:right">')
    print(f'succeeded:</td><td>{_stats.succeeded}')
    print('</td></tr><tr><td style="background-color:yellow;text-align:right">')
    print(f'no result:</td><td>{_stats.no_result}')
    print('</td></tr><tr><td style="background-color:red;text-align:right">')
    print(f'failed:</td><td>{_stats.failed}')
    print('</td></tr><tr><td style="background-color:magenta;text-align:right">')
    print(f'errors:</td><td>{_stats.errors}')
    print('</td></tr><tr><td style="text-align:right">')
    print(f'skipped:</td><td>{_stats.skipped}')
    print('</td></tr>')
    print('</table></body></html>')
