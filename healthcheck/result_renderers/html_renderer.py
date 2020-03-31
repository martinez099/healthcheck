import datetime
import re


preface = False


def render_result(_result, _func, *_args, **_kwargs):
    """
    Render result.

    :param _result: The result.
    :param _func: The check function executed.
    """
    global preface
    if not preface:
        print(f'''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<html><head><title>RE HealthCheck results for {_kwargs["_cluster_name"]}</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<style>
table {{ font-family: monospace }}
table th {{ text-align: left }}
table tr td {{ border-bottom: 1px solid #ddd }}
img#logo {{ float: right; margin: 10px; width: 200px }}
</style>
</head>
<body>
<img src="https://redislabs.com/wp-content/uploads/2018/09/br-Redis-labs-logo@2x.png" alt="Redis Labs logo" id="logo">
<h1>RE HealthCheck results for {_kwargs["_cluster_name"]}</h1>
<p>{datetime.datetime.now().isoformat().replace('T', ' ').split('.')[0]}</p>
<table style="width:100%">
<tr><th>Code: Description</th><th>Result</th><th>Info</th></tr>''')
        preface = True

    doc = (_result[2] if len(_result) == 3 else _func.__doc__).split('\n')[0]
    remedy = None
    print(f'<tr><td>{doc}</td>')
    if _result[0] == '':
        print('<td>SKIPPED</td>')
    elif _result[0] is True:
        print('<td style="background-color:green">SUCCEDED</td>')
    elif _result[0] is False:
        print('<td style="background-color:red">FAILED</td>')
        doc = (_result[2] if len(_result) == 3 else _func.__doc__)
        remedy = re.findall(r'Remedy: (.*)', doc, re.MULTILINE)[0]
    elif _result[0] is None:
        print('<td style="background-color:yellow">NO RESULT</td>')
    elif _result[0] is Exception:
        print('<td style="background-color:magenta">ERROR</td>')
    else:
        raise NotImplementedError()

    print('<td>')
    print(', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()]))
    if remedy:
        print(f'&nbsp;<i><b>Remedy:</b> {remedy}</i>')
    print('</td></tr>')


def render_stats(_stats):
    """
    Render collected statistics.

    :param _stats: A stats collector.
    """
    print('</table>')
    print('<table style="width:200px"><tr style="height:20px"><th></th></tr>')
    print('<tr><td>')
    print("Total checks run: {}".format(
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
