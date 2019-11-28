import concurrent.futures
import functools


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

    def execute(self, _func, _args=[], _kwargs={}, _done_cb=None):
        """
        Execute a function.

        :param _func: The function to execute.
        :param _args: A list of arguments, optional.
        :param _kwargs: A dict of keyword arguments, optional.
        :param _done_cb: A callback executed when the execution is done, optional.
        """
        def wrapper(_check, _args, _kwargs):
            try:
                return _check(*_args, **_kwargs)
            except Exception as e:
                return Exception, {e.__class__.__name__: str(e)}

        future = self.executor.submit(functools.partial(wrapper, _func), _args, _kwargs)
        future.func = _func
        future.args = _args
        future.kwargs = _kwargs
        if _done_cb:
            future.add_done_callback(_done_cb)
        self.futures.append(future)

    def execute_suite(self, _check_suite, _args=[], _kwargs={}, _done_cb=None):
        """
        Execute a check suite.

        :param _check_suite: The check suite.
        :param _args: A list of arguments, optional.
        :param _kwargs: A dict of keyword arguments, optional.
        :param _done_cb: A callback executed when the execution is done, optional.
        """
        for check_name in filter(lambda x: x.startswith('check_'), dir(_check_suite)):
            check_func = getattr(_check_suite, check_name)
            self.execute(check_func, _args=_args, _kwargs=_kwargs, _done_cb=_done_cb)

    def wait(self):
        """
        Wait for completition of all futures.
        """
        for future in concurrent.futures.as_completed(self.futures):
            self.result_cb(future.result(), future.func, future.args, future.kwargs)

        self.futures = []

    def shutdown(self):
        """
        Shutdown the thread pool executor.
        """
        return self.executor.shutdown()
