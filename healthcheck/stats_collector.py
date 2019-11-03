class StatsCollector(object):

    def __init__(self):
        self.succeeded = 0
        self.no_result = 0
        self.failed = 0
        self.errors = 0
        self.skipped = 0

    def collect(self, _result):
        if _result[0] == '':
            self.skipped += 1
        elif _result[0] is True:
            self.succeeded += 1
        elif _result[0] is False:
            self.failed += 1
        elif _result[0] is None:
            self.no_result += 1
        elif _result[0] is Exception:
            self.errors += 1
        else:
            raise NotImplementedError()
