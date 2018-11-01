from abc import ABC, abstractmethod


class AbstractStatsMonitor(ABC):

    @abstractmethod
    def report_stats(self, report_type, metric, *args):
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class AbstractErrorMonitor(ABC):

    @abstractmethod
    def capture_exception(self, *args):
        raise NotImplementedError

    @abstractmethod
    def capture_message(self, msg):
        raise NotImplementedError

    @abstractmethod
    def set_context(self, context):
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


'''
A collection of dummy monitoring daemon classes with no-op stubs.
Useful to avoid excessive repetition of "if app['stats_monitor']:
app['stats_monitor'].statsd.increment(...)" snippets all scattered around the
codebase.
'''


class DummyStatsMonitor(AbstractStatsMonitor):
    def report_stats(self, *args, **kwargs): pass        # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa


class DummyErrorMonitor(AbstractErrorMonitor):
    def capture_exception(self, *args, **kwargs): pass   # noqa
    def capture_message(self, *args, **kwargs): pass     # noqa
    def set_context(self, context): pass                 # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa
