
class StatsCollector(object):

    def __init__(self):
        self.succeeded = 0
        self.no_result = 0
        self.failed = 0
        self.errors = 0
        self.skipped = 0

    def incr_succeeded(self):
        self.succeeded += 1

    def incr_no_result(self):
        self.no_result += 1

    def incr_failed(self):
        self.failed += 1

    def incr_errors(self):
        self.errors += 1

    def incr_skipped(self):
        self.skipped += 1

    def get_stats(self):
        return {
            'succeeded': self.succeeded,
            'no result': self.no_result,
            'failed': self.failed,
            'errors': self.errors,
            'skipped': self.skipped,
            'total': sum([self.succeeded, self.no_result, self.failed, self.errors, self.skipped])
        }
