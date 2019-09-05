import concurrent.futures
import functools

from healthcheck.common import format_error


class CheckExecutor(object):
    """
    Check Executor class.
    """

    def __init__(self, _result_cb, _max_workers=10):
        """
        :param _result_cb: A callback executed when the results are available.
        :param _max_workers: Amount of worker threads for the pool.
        """
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=_max_workers)
        self.futures = []
        self.result_cb = _result_cb

    def execute(self, _func, _args=None, _kwargs=None, _done_cb=None):
        """
        Execute a function.

        :param _func: The function to execute.
        :param _args: Unamed arguments, optional.
        :param _kwargs: Named arguments, optional.
        :param _done_cb: A callback executed when the execution is done, optional.
        """
        def wrapper(_check, *_args, **_kwargs):
            try:
                return _check(_args, _kwargs)
            except Exception as e:
                return format_error(_func.__name__, e)

        future = self.executor.submit(functools.partial(wrapper, _func), _args, _kwargs)
        if _done_cb:
            future.add_done_callback(_done_cb)
        self.futures.append(future)

    def execute_suite(self, check_suite):
        """
        Execute a check suite.

        :param check_suite: The check suite.
        """
        for check in filter(lambda x: x.startswith('check_'), dir(check_suite)):
            method = getattr(check_suite, check)
            self.execute(method)

    def wait(self):
        """
        Wait for completition of all futures.
        """
        for future in concurrent.futures.as_completed(self.futures):
            self.result_cb(future.result())

    def shutdown(self):
        """
        Shutdown the thread pool executor.
        """
        return self.executor.shutdown()
