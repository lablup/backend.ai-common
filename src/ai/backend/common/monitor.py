'''
A collection of dummy monitoring daemon classes with no-op stubs.
Useful to avoid excessive repetition of "if app['stats_monitor']:
app['stats_monitor'].statsd.increment(...)" snippets all scattered around the
codebase.
'''


class DummyStatsMonitor:
    def report_stats(self, *args, **kwargs): pass        # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa


class DummyErrorMonitor:
    def capture_exception(self, *args, **kwargs): pass   # noqa
    def capture_message(self, *args, **kwargs): pass     # noqa
    def set_context(self, context): pass                 # noqa
    def __enter__(self): return self                     # noqa
    def __exit__(self, exc_type, exc_val, exc_tb): pass  # noqa
