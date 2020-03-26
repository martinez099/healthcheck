class StatsCollector(object):
    """
    Statistics Collector class.
    """

    def __init__(self):
        self.succeeded = 0
        self.no_result = 0
        self.failed = 0
        self.errors = 0
        self.skipped = 0

    def collect(self, _result):
        """
        Collect statistic for a result.

        :param _result: The result of a check.
        """
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

    def return_code(self):
        """
        Calculate return code.

        :return: The return code.
        """
        return 3 if self.errors else 2 if self.failed else 0
