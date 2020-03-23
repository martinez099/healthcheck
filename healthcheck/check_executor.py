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

    def execute(self, _func, _params=None, _done_cb=None):
        """
        Execute a function.

        :param _func: The function to execute.
        :param _params: An optional dict of keyword arguments.
        :param _done_cb: An optional callback executed when the execution is done.
        """
        def error_handler(_check, _params):
            try:
                return _check(_params)
            except Exception as e:
                return Exception, {e.__class__.__name__: str(e)}

        future = self.executor.submit(functools.partial(error_handler, _func), _params)
        future.func = _func
        future.params = _params
        if _done_cb:
            future.add_done_callback(_done_cb)
        self.futures.append(future)

    def wait(self):
        """
        Wait for completition of all futures.
        """
        for future in concurrent.futures.as_completed(self.futures):
            self.result_cb(future.result(), future.func)
        self.futures = []

    def shutdown(self):
        """
        Shutdown the thread pool executor.
        """
        return self.executor.shutdown()
