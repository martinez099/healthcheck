
class StatsCollector(object):

    def __init__(self):
        self.success = 0
        self.no_result = 0
        self.failed = 0
        self.error = 0
        self.skipped = 0

    def incr_success(self):
        self.success += 1

    def incr_no_result(self):
        self.no_result += 1

    def incr_failed(self):
        self.failed += 1

    def incr_error(self):
        self.error += 1

    def incr_skipped(self):
        self.skipped += 1

    def get_stats(self):
        return {
            'success': self.success,
            'no result': self.no_result,
            'failed': self.failed,
            'error': self.error,
            'skipped': self.skipped,
            'total': sum([self.success, self.no_result, self.failed, self.error, self.skipped])
        }
